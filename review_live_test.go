//go:build live

package langfuse_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/fgn/go-langfuse"
)

// Fixed names keep reruns from piling up configs and queues, which the
// Langfuse API cannot delete.
const (
	liveReviewConfig  = "go-live-review-route"
	liveArchiveConfig = "go-live-review-archive"
	liveReviewQueue   = "go-langfuse-live-review"
	liveReviewDataset = "go-langfuse-live-review"
)

var liveReviewCategories = []langfuse.ScoreCategory{{Label: "fix", Value: 0}, {Label: "human", Value: 1}, {Label: "ignore", Value: 2}}

// TestLiveAnnotationReview runs a guess-and-correct review loop against a
// live server: a categorical score config and a queue, a dataset item and an
// ANNOTATION score prefilled for a new trace, the queued trace, a reviewer's
// correction and comment, completion, the v3 score read back, and the
// reviewed label written into the item's expected output.
// Run with: go test -count=1 -tags=live -run TestLiveAnnotationReview -v .
func TestLiveAnnotationReview(t *testing.T) {
	if os.Getenv("LANGFUSE_PUBLIC_KEY") == "" || os.Getenv("LANGFUSE_SECRET_KEY") == "" {
		t.Fatal("live Langfuse credentials are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	client, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := client.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	}()
	marker := fmt.Sprintf("go-langfuse-live-review-%d", time.Now().UnixNano())

	config := liveReviewScoreConfig(ctx, t, client)
	liveArchiveRoundTrip(ctx, t, client)
	queue := liveReviewQueueFor(ctx, t, client, config.ID)

	traceCtx, root := client.StartObservation(ctx, marker, langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "synthetic alert", Output: "fix"})
	root.End()
	traceID := oteltrace.SpanFromContext(traceCtx).SpanContext().TraceID().String()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}

	// The guess becomes a dataset item, and is prefilled for review before
	// the trace is queued.
	if _, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{Name: liveReviewDataset}); err != nil {
		t.Fatalf("UpsertDataset(): %v", err)
	}
	if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
		DatasetName: liveReviewDataset, ID: marker, Input: "synthetic alert", SourceTraceID: traceID,
		ExpectedOutput: map[string]any{"route": "fix"}, Metadata: map[string]any{"review": "pending"},
	}); err != nil {
		t.Fatalf("UpsertDatasetItem(guess): %v", err)
	}
	defer func() {
		if err := client.DeleteDatasetItem(context.Background(), marker); err != nil {
			t.Errorf("DeleteDatasetItem(): %v", err)
		}
	}()
	guess := langfuse.Score{
		ID: traceID + "-route", TraceID: traceID, Name: config.Name, StringValue: new("fix"),
		ConfigID: config.ID, QueueID: queue.ID, Source: langfuse.ScoreSourceAnnotation,
		Metadata: map[string]any{"prefilled_by": "go-langfuse-live"},
	}
	if id, err := client.CreateScore(ctx, guess); err != nil || id != guess.ID {
		t.Fatalf("CreateScore(guess) = %q, %v", id, err)
	}
	item, err := client.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
		QueueID: queue.ID, ObjectID: traceID, ObjectType: langfuse.AnnotationObjectTrace,
	})
	if err != nil || item.Status != langfuse.AnnotationPending || item.ObjectID != traceID || !item.CompletedAt.IsZero() {
		t.Fatalf("CreateAnnotationQueueItem() = %+v, %v", item, err)
	}
	defer func() {
		if err := client.DeleteAnnotationQueueItem(context.Background(), queue.ID, item.ID); err != nil {
			t.Errorf("DeleteAnnotationQueueItem(): %v", err)
		}
		if _, err := client.GetAnnotationQueueItem(context.Background(), queue.ID, item.ID); !errors.Is(err,
			langfuse.ErrAnnotationQueueItemNotFound) {
			t.Errorf("GetAnnotationQueueItem() after delete error = %v, want ErrAnnotationQueueItemNotFound", err)
		}
	}()
	if got, err := client.GetAnnotationQueueItem(ctx, queue.ID, item.ID); err != nil || got.ID != item.ID {
		t.Fatalf("GetAnnotationQueueItem() = %+v, %v", got, err)
	}
	if !liveQueueHas(ctx, t, client, queue.ID, langfuse.AnnotationPending, item.ID) {
		t.Fatal("pending queue items do not include the new item")
	}

	// Scores are processed asynchronously: wait until the prefilled guess is
	// stored before the reviewer changes it.
	prefill := liveAnnotationScore(ctx, t, client, traceID, guess.ID, "fix")
	if prefill.TraceID != traceID || prefill.ConfigID != config.ID || prefill.QueueID != queue.ID ||
		prefill.Source != langfuse.ScoreSourceAnnotation || prefill.DataType != langfuse.ScoreTypeCategorical ||
		prefill.Environment == "" || prefill.Metadata["prefilled_by"] != "go-langfuse-live" || prefill.Comment != "" {
		t.Fatalf("stored prefill = %+v", prefill)
	}

	// The reviewer corrects the guess, explains why, and completes the item.
	correction := guess
	correction.StringValue, correction.Comment = new("human"), "needs a person"
	if _, err := client.CreateScore(ctx, correction); err != nil {
		t.Fatalf("CreateScore(correction): %v", err)
	}
	completed, err := client.UpdateAnnotationQueueItem(ctx, queue.ID, item.ID, langfuse.AnnotationCompleted)
	if err != nil || completed.Status != langfuse.AnnotationCompleted || completed.CompletedAt.IsZero() {
		t.Fatalf("UpdateAnnotationQueueItem() = %+v, %v", completed, err)
	}
	if !liveQueueHas(ctx, t, client, queue.ID, langfuse.AnnotationCompleted, item.ID) {
		t.Fatal("completed queue items do not include the item")
	}

	// The correction replaces the stored guess in place.
	stored := liveAnnotationScore(ctx, t, client, traceID, guess.ID, "human")
	if stored.TraceID != traceID || stored.ConfigID != config.ID || stored.QueueID != queue.ID ||
		stored.Comment != "needs a person" || !stored.Timestamp.Equal(prefill.Timestamp) ||
		stored.Metadata["prefilled_by"] != "go-langfuse-live" || !stored.CreatedAt.Equal(prefill.CreatedAt) {
		t.Fatalf("stored correction = %+v, prefill %+v", stored, prefill)
	}

	// Reopening keeps the completion time; completing again stamps a new one.
	reopened, err := client.UpdateAnnotationQueueItem(ctx, queue.ID, item.ID, langfuse.AnnotationPending)
	if err != nil || reopened.Status != langfuse.AnnotationPending || !reopened.CompletedAt.Equal(completed.CompletedAt) {
		t.Fatalf("UpdateAnnotationQueueItem(PENDING) = %+v, %v; want CompletedAt %v kept", reopened, err, completed.CompletedAt)
	}
	completed, err = client.UpdateAnnotationQueueItem(ctx, queue.ID, item.ID, langfuse.AnnotationCompleted)
	if err != nil || completed.CompletedAt.Before(reopened.CompletedAt) {
		t.Fatalf("UpdateAnnotationQueueItem(COMPLETED again) = %+v, %v", completed, err)
	}

	// Comments need the trace to be readable, which can take a moment.
	var commentID string
	liveEventually(ctx, t, "a comment on the exported trace", func() bool {
		id, err := client.CreateComment(ctx, langfuse.CommentSpec{
			ObjectType: langfuse.CommentObjectTrace, ObjectID: traceID, Content: "route: needs a person",
		})
		if errors.Is(err, langfuse.ErrCommentObjectNotFound) {
			return false
		}
		if err != nil {
			t.Fatalf("CreateComment(): %v", err)
		}
		commentID = id
		return true
	})
	comments, err := collect(client.Comments(ctx, langfuse.CommentQuery{
		ObjectType: langfuse.CommentObjectTrace, ObjectID: traceID,
	}))
	if err != nil || len(comments) != 1 || comments[0].ID != commentID || comments[0].Content != "route: needs a person" {
		t.Fatalf("Comments() = %+v, %v", comments, err)
	}
	if comment, err := client.GetComment(ctx, commentID); err != nil || comment.ObjectID != traceID {
		t.Fatalf("GetComment() = %+v, %v", comment, err)
	}

	// Copy the completed review into the dataset item for later experiments.
	reviewed, err := collect(client.DatasetItems(ctx, langfuse.DatasetItemQuery{
		DatasetName: liveReviewDataset, SourceTraceID: traceID,
	}))
	if err != nil || len(reviewed) != 1 || reviewed[0].ID != marker {
		t.Fatalf("DatasetItems(source trace) = %+v, %v", reviewed, err)
	}
	if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
		DatasetName: liveReviewDataset, ID: marker, ExpectedOutput: map[string]any{"route": *stored.StringValue},
		Metadata: map[string]any{
			"review": "corrected", "guess": map[string]any{"route": "fix"},
			"reviewed_at": completed.CompletedAt.Format(time.RFC3339), "review_comments": []string{comments[0].Content},
		},
	}); err != nil {
		t.Fatalf("UpsertDatasetItem(review): %v", err)
	}
	written, err := client.GetDatasetItem(ctx, marker)
	if err != nil || !sameJSON(t, string(written.ExpectedOutput), `{"route":"human"}`) ||
		!strings.Contains(string(written.Metadata), `"review":"corrected"`) || written.SourceTraceID != traceID {
		t.Fatalf("GetDatasetItem() = %+v, %v", written, err)
	}

	for name, err := range map[string]error{
		"GetScoreConfig":     errOf(client.GetScoreConfig(ctx, marker)),
		"GetAnnotationQueue": errOf(client.GetAnnotationQueue(ctx, marker)),
		"GetComment":         errOf(client.GetComment(ctx, marker)),
	} {
		if !errors.Is(err, map[string]error{
			"GetScoreConfig": langfuse.ErrScoreConfigNotFound, "GetAnnotationQueue": langfuse.ErrAnnotationQueueNotFound,
			"GetComment": langfuse.ErrCommentNotFound,
		}[name]) {
			t.Errorf("%s(missing) error = %v", name, err)
		}
	}
	if err := client.UnassignAnnotationQueue(ctx, queue.ID, marker); err != nil {
		t.Errorf("UnassignAnnotationQueue(absent user): %v", err)
	}
	if err := client.AssignAnnotationQueue(ctx, queue.ID, marker); err == nil {
		t.Error("AssignAnnotationQueue(non-member) succeeded")
	}
}

// liveReviewScoreConfig finds the active review config or creates it.
func liveReviewScoreConfig(ctx context.Context, t *testing.T, client *langfuse.Client) langfuse.ScoreConfig {
	t.Helper()
	for config, err := range client.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{PageSize: 100}) {
		if err != nil {
			t.Fatalf("ScoreConfigs(): %v", err)
		}
		if config.Name == liveReviewConfig && !config.Archived && config.DataType == langfuse.ScoreTypeCategorical &&
			slices.Equal(config.Categories, liveReviewCategories) {
			return config
		}
	}
	config, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
		Name: liveReviewConfig, DataType: langfuse.ScoreTypeCategorical, Categories: liveReviewCategories,
		Description: "go-langfuse live test",
	})
	if err != nil || !slices.Equal(config.Categories, liveReviewCategories) || config.Archived {
		t.Fatalf("CreateScoreConfig() = %+v, %v", config, err)
	}
	if got, err := client.GetScoreConfig(ctx, config.ID); err != nil || got.ID != config.ID {
		t.Fatalf("GetScoreConfig() = %+v, %v", got, err)
	}
	return config
}

// liveArchiveRoundTrip archives and restores one NUMERIC config, creating it
// on the first run.
func liveArchiveRoundTrip(ctx context.Context, t *testing.T, client *langfuse.Client) {
	t.Helper()
	var config langfuse.ScoreConfig
	for found, err := range client.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{}) {
		if err != nil {
			t.Fatalf("ScoreConfigs(): %v", err)
		}
		if found.Name == liveArchiveConfig && found.DataType == langfuse.ScoreTypeNumeric {
			config = found
			break
		}
	}
	if config.ID == "" {
		created, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
			Name: liveArchiveConfig, DataType: langfuse.ScoreTypeNumeric, MinValue: new(0.0), MaxValue: new(1.0),
		})
		if err != nil || *created.MinValue != 0 || *created.MaxValue != 1 {
			t.Fatalf("CreateScoreConfig(numeric) = %+v, %v", created, err)
		}
		config = created
	}
	for _, archived := range []bool{!config.Archived, config.Archived} {
		updated, err := client.UpdateScoreConfig(ctx, config.ID, langfuse.ScoreConfigUpdate{Archived: &archived})
		if err != nil || updated.Archived != archived || updated.ID != config.ID {
			t.Fatalf("UpdateScoreConfig(archived %t) = %+v, %v", archived, updated, err)
		}
	}
}

// liveReviewQueueFor finds the review queue or creates it with configID.
func liveReviewQueueFor(ctx context.Context, t *testing.T, client *langfuse.Client, configID string) langfuse.AnnotationQueue {
	t.Helper()
	for queue, err := range client.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{}) {
		if err != nil {
			t.Fatalf("AnnotationQueues(): %v", err)
		}
		if queue.Name == liveReviewQueue {
			if !slices.Contains(queue.ScoreConfigIDs, configID) {
				t.Fatalf("queue %s does not use config %s; its configs are %v", queue.Name, configID, queue.ScoreConfigIDs)
			}
			return queue
		}
	}
	queue, err := client.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{
		Name: liveReviewQueue, Description: "go-langfuse live test", ScoreConfigIDs: []string{configID},
	})
	if err != nil || !slices.Equal(queue.ScoreConfigIDs, []string{configID}) {
		t.Fatalf("CreateAnnotationQueue() = %+v, %v", queue, err)
	}
	if got, err := client.GetAnnotationQueue(ctx, queue.ID); err != nil || got.Name != liveReviewQueue {
		t.Fatalf("GetAnnotationQueue() = %+v, %v", got, err)
	}
	return queue
}

// liveAnnotationScore polls for the ANNOTATION score id on traceID until it
// holds value.
func liveAnnotationScore(
	ctx context.Context, t *testing.T, client *langfuse.Client, traceID, id, value string,
) langfuse.StoredScore {
	t.Helper()
	var stored langfuse.StoredScore
	liveEventually(ctx, t, "annotation score "+value, func() bool {
		scores, err := collect(client.Scores(ctx, langfuse.ScoreQuery{
			TraceIDs: []string{traceID}, Sources: []langfuse.ScoreSource{langfuse.ScoreSourceAnnotation},
		}))
		if err != nil {
			t.Fatalf("Scores(): %v", err)
		}
		for _, score := range scores {
			if score.ID == id && score.StringValue != nil && *score.StringValue == value {
				stored = score
				return true
			}
		}
		return false
	})
	return stored
}

func liveQueueHas(
	ctx context.Context, t *testing.T, client *langfuse.Client, queueID string, status langfuse.AnnotationStatus, itemID string,
) bool {
	t.Helper()
	for item, err := range client.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{QueueID: queueID, Status: status}) {
		if err != nil {
			t.Fatalf("AnnotationQueueItems(%s): %v", status, err)
		}
		if item.ID == itemID {
			return item.Status == status
		}
	}
	return false
}

func liveEventually(ctx context.Context, t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear within 90 seconds", what)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}
