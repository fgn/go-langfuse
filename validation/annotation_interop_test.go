//go:build datasetinterop

package validation

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// Fixed names keep reruns from piling up configs and queues, which the
// Langfuse API cannot delete.
const (
	interopReviewConfig = "go-interop-review-route"
	interopReviewQueue  = "go-langfuse-interop-review"
)

var interopReviewCategories = []langfuse.ScoreCategory{{Label: "fix", Value: 0}, {Label: "human", Value: 1}, {Label: "ignore", Value: 2}}

type reviewFixture struct {
	config langfuse.ScoreConfig
	queue  langfuse.AnnotationQueue
}

// reviewSetup finds or creates the shared review config and queue with Go.
func (h *interopHarness) reviewSetup(t *testing.T) reviewFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var review reviewFixture
	for config, err := range h.lf.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{PageSize: 100}) {
		if err != nil {
			t.Fatalf("ScoreConfigs(): %v", err)
		}
		if config.Name == interopReviewConfig && !config.Archived && slices.Equal(config.Categories, interopReviewCategories) {
			review.config = config
			break
		}
	}
	if review.config.ID == "" {
		config, err := h.lf.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
			Name: interopReviewConfig, DataType: langfuse.ScoreTypeCategorical, Categories: interopReviewCategories,
		})
		if err != nil {
			t.Fatalf("CreateScoreConfig(): %v", err)
		}
		review.config = config
	}
	for queue, err := range h.lf.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{PageSize: 100}) {
		if err != nil {
			t.Fatalf("AnnotationQueues(): %v", err)
		}
		if queue.Name == interopReviewQueue {
			if !slices.Contains(queue.ScoreConfigIDs, review.config.ID) {
				t.Fatalf("queue %s does not use config %s", queue.Name, review.config.ID)
			}
			review.queue = queue
			return review
		}
	}
	queue, err := h.lf.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{
		Name: interopReviewQueue, ScoreConfigIDs: []string{review.config.ID},
	})
	if err != nil {
		t.Fatalf("CreateAnnotationQueue(): %v", err)
	}
	review.queue = queue
	return review
}

// annotationRoundTrip has writer prefill a review score, queue the trace as
// completed, and comment on it; reader then reads all three back.
func (h *interopHarness) annotationRoundTrip(t *testing.T, review reviewFixture, writer, reader string, evidence *[]string) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	_, span := h.lf.StartObservation(ctx, h.marker+"-e8-"+writer+"-"+reader, langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "synthetic alert", Output: "fix"})
	span.End()
	traceID := span.TraceID()
	if err := h.lf.Flush(ctx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	scoreID, comment := traceID+"-route", writer+": route human"
	var itemID, commentID string
	t.Cleanup(func() {
		if itemID == "" {
			return
		}
		if err := h.lf.DeleteAnnotationQueueItem(context.Background(), review.queue.ID, itemID); err != nil {
			t.Errorf("DeleteAnnotationQueueItem(): %v", err)
		}
	})
	if writer == "go" {
		human := "human"
		if _, err := h.lf.CreateScore(ctx, langfuse.Score{
			ID: scoreID, TraceID: traceID, Name: review.config.Name, StringValue: &human, ConfigID: review.config.ID,
			QueueID: review.queue.ID, Source: langfuse.ScoreSourceAnnotation, Comment: comment,
		}); err != nil {
			t.Fatalf("CreateScore(): %v", err)
		}
		item, err := h.lf.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
			QueueID: review.queue.ID, ObjectID: traceID, ObjectType: langfuse.AnnotationObjectTrace,
			Status: langfuse.AnnotationCompleted,
		})
		if err != nil {
			t.Fatalf("CreateAnnotationQueueItem(): %v", err)
		}
		itemID = item.ID
		commentID = eventuallyValue(ctx, t, "a Go comment on the exported trace", func() (string, bool) {
			id, err := h.lf.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: traceID, Content: comment,
			})
			if errors.Is(err, langfuse.ErrCommentObjectNotFound) {
				return "", false
			}
			if err != nil {
				t.Fatalf("CreateComment(): %v", err)
			}
			return id, true
		})
	} else {
		written := h.sdk(t, writer, map[string]any{
			"op": "review_write", "score_id": scoreID, "trace_id": traceID, "config_name": review.config.Name,
			"config_id": review.config.ID, "queue_id": review.queue.ID, "value": "human", "comment": comment,
		})
		itemID, commentID = text(field(written, "item_id")), text(field(written, "comment_id"))
	}

	if reader == "go" {
		stored := eventuallyValue(ctx, t, "the "+writer+" score", func() (langfuse.StoredScore, bool) {
			for score, err := range h.lf.Scores(ctx, langfuse.ScoreQuery{
				TraceIDs: []string{traceID}, Sources: []langfuse.ScoreSource{langfuse.ScoreSourceAnnotation},
			}) {
				if err != nil {
					t.Fatalf("Scores(): %v", err)
				}
				if score.ID == scoreID {
					return score, true
				}
			}
			return langfuse.StoredScore{}, false
		})
		if stored.StringValue == nil || *stored.StringValue != "human" || stored.TraceID != traceID ||
			stored.ConfigID != review.config.ID || stored.QueueID != review.queue.ID || stored.Comment != comment ||
			stored.DataType != langfuse.ScoreTypeCategorical || stored.Name != review.config.Name ||
			stored.Source != langfuse.ScoreSourceAnnotation {
			t.Errorf("Go read of the %s score = %+v", writer, stored)
		}
		found := false
		for item, err := range h.lf.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{
			QueueID: review.queue.ID, Status: langfuse.AnnotationCompleted,
		}) {
			if err != nil {
				t.Fatalf("AnnotationQueueItems(): %v", err)
			}
			if item.ID == itemID {
				found = item.ObjectID == traceID && item.ObjectType == langfuse.AnnotationObjectTrace && !item.CompletedAt.IsZero()
			}
		}
		if !found {
			t.Errorf("completed queue items lack the %s item %s for trace %s", writer, itemID, traceID)
		}
		comments, err := collectAll(h.lf.Comments(ctx, langfuse.CommentQuery{ObjectType: langfuse.CommentObjectTrace, ObjectID: traceID}))
		if err != nil || len(comments) != 1 || comments[0].ID != commentID || comments[0].Content != comment {
			t.Errorf("Go read of the %s comments = %+v, %v", writer, comments, err)
		}
		*evidence = append(*evidence, "Go read the "+writer+" ANNOTATION score (label, config, queue, comment), "+
			"its completed queue item, and its trace comment")
		return
	}
	read := h.sdk(t, reader, map[string]any{
		"op": "review_read", "score_id": scoreID, "trace_id": traceID, "config_id": review.config.ID,
		"queue_id": review.queue.ID, "value": "human",
	})
	scores, _ := field(read, "scores").([]any)
	if len(scores) != 1 {
		t.Fatalf("%s read %d scores with ID %s, want 1", reader, len(scores), scoreID)
	}
	score := scores[0]
	for key, want := range map[string]string{
		"value": "human", "source": "ANNOTATION", "dataType": "CATEGORICAL", "configId": review.config.ID,
		"queueId": review.queue.ID, "comment": comment, "name": review.config.Name,
	} {
		if got := text(field(score, key)); got != want {
			t.Errorf("%s score %s = %q, want %q", reader, key, got, want)
		}
	}
	if text(field(score, "subject", "kind")) != "trace" || text(field(score, "subject", "id")) != traceID {
		t.Errorf("%s score subject = %v", reader, field(score, "subject"))
	}
	items, _ := field(read, "items").([]any)
	if len(items) != 1 || text(field(items[0], "id")) != itemID || text(field(items[0], "status")) != "COMPLETED" {
		t.Errorf("%s queue items for the trace = %v, want the completed item %s", reader, items, itemID)
	}
	comments, _ := field(read, "comments").([]any)
	if len(comments) != 1 || text(field(comments[0], "id")) != commentID || text(field(comments[0], "content")) != comment {
		t.Errorf("%s comments = %v", reader, comments)
	}
	if text(field(read, "config", "id")) != review.config.ID || text(field(read, "queue", "id")) != review.queue.ID {
		t.Errorf("%s config or queue read = %v / %v", reader, field(read, "config", "id"), field(read, "queue", "id"))
	}
	*evidence = append(*evidence, reader+" read the "+writer+" ANNOTATION score (label, source, config, queue, "+
		"comment, trace subject), its completed queue item, its comment, the config, and the queue")
}

func eventuallyValue[T any](ctx context.Context, t *testing.T, what string, poll func() (T, bool)) T {
	t.Helper()
	for {
		if value, ok := poll(); ok {
			return value
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s did not appear: %v", what, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func collectAll[T any](sequence func(func(T, error) bool)) ([]T, error) {
	var values []T
	for value, err := range sequence {
		if err != nil {
			return values, err
		}
		values = append(values, value)
	}
	return values, nil
}
