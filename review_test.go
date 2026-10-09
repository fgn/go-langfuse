package langfuse_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

func scoreConfigJSON(id, name string) string {
	return fmt.Sprintf(`{"id":%q,"projectId":"p-1","name":%q,"description":null,"dataType":"CATEGORICAL",`+
		`"isArchived":false,"minValue":null,"maxValue":null,`+
		`"categories":[{"label":"fix","value":0},{"label":"human","value":1}],%s}`, id, name, datasetTimes)
}

func queueJSON(id, name string) string {
	return fmt.Sprintf(`{"id":%q,"name":%q,"description":"review","scoreConfigIds":["cfg-1"],%s}`, id, name, datasetTimes)
}

func queueItemJSON(id, queueID, objectID, status string) string {
	completedAt := "null"
	if status == "COMPLETED" {
		completedAt = `"2026-09-30T12:00:00.000Z"`
	}
	return fmt.Sprintf(`{"id":%q,"queueId":%q,"objectId":%q,"objectType":"TRACE","status":%q,"completedAt":%s,%s}`,
		id, queueID, objectID, status, completedAt, datasetTimes)
}

func commentJSON(id, objectID string) string {
	return fmt.Sprintf(`{"id":%q,"projectId":"p-1","objectType":"TRACE","objectId":%q,"content":"looks right",`+
		`"authorUserId":null,%s}`, id, objectID, datasetTimes)
}

func numberedPage(totalPages int, entries ...string) string {
	return fmt.Sprintf(`{"data":[%s],"meta":{"page":1,"limit":50,"totalItems":%d,"totalPages":%d}}`,
		strings.Join(entries, ","), len(entries), totalPages)
}

const v3ScoreCore = `"projectId":"p-1","source":"ANNOTATION","timestamp":"2026-09-30T10:00:00.000Z",` +
	`"environment":"default","createdAt":"2026-09-30T10:00:00.000Z","updatedAt":"2026-09-30T10:30:00.000Z"`

func v3ScoreJSON(id, traceID string) string {
	return fmt.Sprintf(`{"id":%q,"name":"triage_route","dataType":"CATEGORICAL","value":"fix",%s,"comment":"why",`+
		`"configId":"cfg-1","metadata":{},"authorUserId":"u-1","queueId":"q-1","subject":{"kind":"trace","id":%q}}`,
		id, v3ScoreCore, traceID)
}

func numericConfigJSON(id string) string {
	return fmt.Sprintf(`{"id":%q,"projectId":"p-1","name":"latency","description":"seconds","dataType":"NUMERIC",`+
		`"isArchived":false,"minValue":null,"maxValue":10,"categories":null,%s}`, id, datasetTimes)
}

// echoJSON overlays defaults and then the request body on a stored object,
// as Langfuse answers a create or patch.
func echoJSON(stored string, body []byte, defaults map[string]any) string {
	var object, request map[string]any
	_ = json.Unmarshal([]byte(stored), &object)
	_ = json.Unmarshal(body, &request)
	maps.Copy(object, defaults)
	maps.Copy(object, request)
	data, err := json.Marshal(object)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// serveReview answers every review endpoint with a valid response that
// echoes the request, as Langfuse does.
func serveReview(w http.ResponseWriter, r *http.Request, body []byte) {
	path := strings.TrimPrefix(r.URL.Path, "/api/public")
	write := func(body string) { _, _ = io.WriteString(w, body) }
	segments := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	unescape := func(index int) string {
		value, _ := url.PathUnescape(segments[index])
		return value
	}
	switch {
	case path == "/score-configs" && r.Method == http.MethodGet:
		write(numberedPage(1, scoreConfigJSON("cfg-1", "triage_route")))
	case path == "/score-configs":
		var spec map[string]any
		_ = json.Unmarshal(body, &spec)
		extra := map[string]any{}
		switch spec["dataType"] {
		case "NUMERIC":
			write(echoJSON(numericConfigJSON("cfg-1"), body, map[string]any{"description": nil, "maxValue": nil}))
			return
		case "BOOLEAN":
			extra["categories"] = []any{map[string]any{"label": "True", "value": 1}, map[string]any{"label": "False", "value": 0}}
		case "TEXT":
			extra["categories"] = nil
		}
		write(echoJSON(scoreConfigJSON("cfg-1", "x"), body, extra))
	case path == "/score-configs/cfg-num":
		write(echoJSON(numericConfigJSON("cfg-num"), body, nil))
	case strings.HasPrefix(path, "/score-configs/"):
		write(echoJSON(scoreConfigJSON(unescape(3), "triage_route"), body, nil))
	case path == "/annotation-queues" && r.Method == http.MethodGet:
		write(numberedPage(1, queueJSON("q-1", "alerts")))
	case path == "/annotation-queues":
		write(echoJSON(queueJSON("q-1", "x"), body, map[string]any{"description": nil}))
	case strings.HasSuffix(path, "/assignments") && r.Method == http.MethodPost:
		write(echoJSON(fmt.Sprintf(`{"projectId":"p-1","queueId":%q}`, unescape(3)), body, nil))
	case strings.HasSuffix(path, "/assignments"):
		write(`{"success":true}`)
	case strings.HasSuffix(path, "/items") && r.Method == http.MethodGet:
		write(numberedPage(1, queueItemJSON("i-1", unescape(3), "t1", cmp.Or(r.URL.Query().Get("status"), "PENDING"))))
	case strings.HasSuffix(path, "/items"):
		status := "PENDING"
		if strings.Contains(string(body), "COMPLETED") {
			status = "COMPLETED"
		}
		write(queueItemJSON("i-1", unescape(3), "t1", status))
	case len(segments) == 6 && r.Method == http.MethodDelete:
		write(`{"success":true,"message":"Annotation queue item deleted successfully"}`)
	case len(segments) == 6 && r.Method == http.MethodPatch:
		write(queueItemJSON(unescape(5), unescape(3), "t1", "COMPLETED"))
	case len(segments) == 6:
		write(queueItemJSON(unescape(5), unescape(3), "t1", "PENDING"))
	case strings.HasPrefix(path, "/annotation-queues/"):
		write(queueJSON(unescape(3), "alerts"))
	case path == "/scores":
		write(`{"id":"t1-triage_route"}`)
	case path == "/v3/scores":
		write(`{"data":[` + v3ScoreJSON("s-1", "t1") + `],"meta":{"limit":50}}`)
	case path == "/comments" && r.Method == http.MethodGet:
		write(numberedPage(1, commentJSON("c-1", "t1")))
	case path == "/comments":
		write(`{"id":"c-1"}`)
	case strings.HasPrefix(path, "/comments/"):
		write(commentJSON(unescape(3), "t1"))
	default:
		w.WriteHeader(http.StatusTeapot)
	}
}

func collect[T any](sequence func(func(T, error) bool)) ([]T, error) {
	var values []T
	var final error
	for value, err := range sequence {
		if final != nil {
			return values, errors.New("listing yielded after its terminal error")
		}
		if err != nil {
			final = err
			continue
		}
		values = append(values, value)
	}
	return values, final
}

var reviewCalls = map[string]func(context.Context, *langfuse.Client) error{
	"CreateScoreConfig": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
			Name: "triage_route", DataType: langfuse.ScoreTypeCategorical,
			Categories: []langfuse.ScoreCategory{{Label: "fix", Value: 0}, {Label: "human", Value: 1}},
		}))
	},
	"GetScoreConfig": func(ctx context.Context, c *langfuse.Client) error { return errOf(c.GetScoreConfig(ctx, "cfg-1")) },
	"UpdateScoreConfig": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.UpdateScoreConfig(ctx, "cfg-1", langfuse.ScoreConfigUpdate{Archived: new(true)}))
	},
	"ScoreConfigs": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collect(c.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{})))
	},
	"CreateAnnotationQueue": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{Name: "alerts", ScoreConfigIDs: []string{"cfg-1"}}))
	},
	"GetAnnotationQueue": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.GetAnnotationQueue(ctx, "q-1"))
	},
	"AnnotationQueues": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collect(c.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{})))
	},
	"CreateAnnotationQueueItem": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
			QueueID: "q-1", ObjectID: "t1", ObjectType: langfuse.AnnotationObjectTrace,
		}))
	},
	"GetAnnotationQueueItem": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.GetAnnotationQueueItem(ctx, "q-1", "i-1"))
	},
	"UpdateAnnotationQueueItem": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.UpdateAnnotationQueueItem(ctx, "q-1", "i-1", langfuse.AnnotationCompleted))
	},
	"DeleteAnnotationQueueItem": func(ctx context.Context, c *langfuse.Client) error {
		return c.DeleteAnnotationQueueItem(ctx, "q-1", "i-1")
	},
	"AnnotationQueueItems": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collect(c.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{QueueID: "q-1"})))
	},
	"AssignAnnotationQueue": func(ctx context.Context, c *langfuse.Client) error {
		return c.AssignAnnotationQueue(ctx, "q-1", "u-1")
	},
	"UnassignAnnotationQueue": func(ctx context.Context, c *langfuse.Client) error {
		return c.UnassignAnnotationQueue(ctx, "q-1", "u-1")
	},
	"CreateScore": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.CreateScore(ctx, langfuse.Score{
			ID: "t1-triage_route", TraceID: "t1", Name: "triage_route", StringValue: new("fix"),
			ConfigID: "cfg-1", QueueID: "q-1", Source: langfuse.ScoreSourceAnnotation,
			Metadata: map[string]any{"prefilled_by": "test"},
		}))
	},
	"Scores": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collect(c.Scores(ctx, langfuse.ScoreQuery{TraceIDs: []string{"t1"}})))
	},
	"CreateComment": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.CreateComment(ctx, langfuse.CommentSpec{
			ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: "looks right",
		}))
	},
	"GetComment": func(ctx context.Context, c *langfuse.Client) error { return errOf(c.GetComment(ctx, "c-1")) },
	"Comments": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collect(c.Comments(ctx, langfuse.CommentQuery{ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1"})))
	},
}

var reviewWrites = map[string]bool{
	"CreateScoreConfig": true, "UpdateScoreConfig": true, "CreateAnnotationQueue": true,
	"CreateAnnotationQueueItem": true, "UpdateAnnotationQueueItem": true, "DeleteAnnotationQueueItem": true,
	"AssignAnnotationQueue": true, "UnassignAnnotationQueue": true, "CreateScore": true, "CreateComment": true,
}

func TestReviewRequestShape(t *testing.T) {
	t.Parallel()
	type seen struct{ method, uri, body string }
	var mu sync.Mutex
	var requests []seen
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		requests = append(requests, seen{r.Method, r.RequestURI, string(body)})
		mu.Unlock()
		serveReview(w, r, body)
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	check := func(call func() error, method, uri, body string) {
		t.Helper()
		mu.Lock()
		requests = requests[:0]
		mu.Unlock()
		if err := call(); err != nil {
			t.Fatalf("%s %s error = %v", method, uri, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(requests) != 1 {
			t.Fatalf("%s %s sent %d requests", method, uri, len(requests))
		}
		got := requests[0]
		if got.method != method || got.uri != uri {
			t.Fatalf("request = %s %s, want %s %s", got.method, got.uri, method, uri)
		}
		if (body == "") != (got.body == "") || body != "" && !sameJSON(t, got.body, body) {
			t.Fatalf("%s body = %s, want %s", uri, got.body, body)
		}
	}
	for _, test := range []struct {
		call              func() error
		method, uri, body string
	}{
		{
			func() error {
				config, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
					Name: "triage_route", DataType: langfuse.ScoreTypeCategorical, Description: "route",
					Categories: []langfuse.ScoreCategory{{Label: "fix", Value: 0}, {Label: "human", Value: 1}},
				})
				if err == nil && (config.ID != "cfg-1" || config.Archived || config.Description != "route" ||
					len(config.Categories) != 2 || config.Categories[1] != (langfuse.ScoreCategory{Label: "human", Value: 1}) ||
					config.MinValue != nil || !config.CreatedAt.Equal(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))) {
					err = fmt.Errorf("config = %+v", config)
				}
				return err
			}, http.MethodPost, "/api/public/score-configs",
			`{"name":"triage_route","dataType":"CATEGORICAL","description":"route",` +
				`"categories":[{"label":"fix","value":0},{"label":"human","value":1}]}`,
		},
		{
			func() error {
				return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
					Name: "triage_route", DataType: langfuse.ScoreTypeNumeric, MinValue: new(0.0), MaxValue: new(1.5),
				}))
			}, http.MethodPost, "/api/public/score-configs",
			`{"name":"triage_route","dataType":"NUMERIC","minValue":0,"maxValue":1.5}`,
		},
		{
			func() error { return errOf(client.GetScoreConfig(ctx, "a/b c")) },
			http.MethodGet, "/api/public/score-configs/a%2Fb%20c", "",
		},
		{
			func() error {
				config, err := client.UpdateScoreConfig(ctx, "cfg-1", langfuse.ScoreConfigUpdate{
					Archived: new(true), Name: new("triage route"), Description: new(""),
					Categories: []langfuse.ScoreCategory{{Label: "fix", Value: 0}},
				})
				if err == nil && (!config.Archived || config.Name != "triage route" || len(config.Categories) != 1) {
					err = fmt.Errorf("config = %+v", config)
				}
				return err
			}, http.MethodPatch, "/api/public/score-configs/cfg-1",
			`{"isArchived":true,"name":"triage route","description":"","categories":[{"label":"fix","value":0}]}`,
		},
		{
			func() error {
				config, err := client.UpdateScoreConfig(ctx, "cfg-num", langfuse.ScoreConfigUpdate{
					Archived: new(false), MinValue: new(0.0),
				})
				if err == nil && (config.MinValue == nil || *config.MinValue != 0 || *config.MaxValue != 10) {
					err = fmt.Errorf("config = %+v", config)
				}
				return err
			}, http.MethodPatch, "/api/public/score-configs/cfg-num", `{"isArchived":false,"minValue":0}`,
		},
		{
			func() error { return errOf(collect(client.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{}))) },
			http.MethodGet, "/api/public/score-configs?limit=50&page=1", "",
		},
		{
			func() error {
				queue, err := client.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{
					Name: "alerts", Description: "review", ScoreConfigIDs: []string{"cfg-1", "cfg-2"},
				})
				if err == nil && (queue.ID != "q-1" || queue.Name != "alerts" || queue.Description != "review" ||
					!slices.Equal(queue.ScoreConfigIDs, []string{"cfg-1", "cfg-2"})) {
					err = fmt.Errorf("queue = %+v", queue)
				}
				return err
			}, http.MethodPost, "/api/public/annotation-queues",
			`{"name":"alerts","description":"review","scoreConfigIds":["cfg-1","cfg-2"]}`,
		},
		{
			func() error { return errOf(client.GetAnnotationQueue(ctx, "q/1")) },
			http.MethodGet, "/api/public/annotation-queues/q%2F1", "",
		},
		{
			func() error {
				return errOf(collect(client.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{PageSize: 7})))
			},
			http.MethodGet, "/api/public/annotation-queues?limit=7&page=1", "",
		},
		{func() error {
			item, err := client.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
				QueueID: "q 1", ObjectID: "t1", ObjectType: langfuse.AnnotationObjectTrace,
			})
			if err == nil && (item.ID != "i-1" || item.QueueID != "q 1" || item.Status != langfuse.AnnotationPending ||
				!item.CompletedAt.IsZero()) {
				err = fmt.Errorf("item = %+v", item)
			}
			return err
		}, http.MethodPost, "/api/public/annotation-queues/q%201/items", `{"objectId":"t1","objectType":"TRACE"}`},
		{
			func() error { return errOf(client.GetAnnotationQueueItem(ctx, "q-1", "i/1")) },
			http.MethodGet, "/api/public/annotation-queues/q-1/items/i%2F1", "",
		},
		{func() error {
			item, err := client.UpdateAnnotationQueueItem(ctx, "q-1", "i-1", langfuse.AnnotationCompleted)
			if err == nil && !item.CompletedAt.Equal(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)) {
				err = fmt.Errorf("item = %+v", item)
			}
			return err
		}, http.MethodPatch, "/api/public/annotation-queues/q-1/items/i-1", `{"status":"COMPLETED"}`},
		{
			func() error { return client.DeleteAnnotationQueueItem(ctx, "q-1", "i-1") },
			http.MethodDelete, "/api/public/annotation-queues/q-1/items/i-1", "",
		},
		{func() error {
			return errOf(collect(client.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{
				QueueID: "q-1", Status: langfuse.AnnotationCompleted, PageSize: 100,
			})))
		}, http.MethodGet, "/api/public/annotation-queues/q-1/items?limit=100&page=1&status=COMPLETED", ""},
		{
			func() error { return client.AssignAnnotationQueue(ctx, "q-1", "u-1") },
			http.MethodPost, "/api/public/annotation-queues/q-1/assignments", `{"userId":"u-1"}`,
		},
		{
			func() error { return client.UnassignAnnotationQueue(ctx, "q-1", "u-1") },
			http.MethodDelete, "/api/public/annotation-queues/q-1/assignments", `{"userId":"u-1"}`,
		},
		{
			func() error {
				id, err := client.CreateScore(ctx, langfuse.Score{
					ID: "t1-triage_route", TraceID: "t1", Name: "triage_route", StringValue: new("fix"),
					DataType: langfuse.ScoreTypeCategorical, ConfigID: "cfg-1", QueueID: "q-1",
					Source: langfuse.ScoreSourceAnnotation, Comment: "guess", Metadata: map[string]any{"by": "bot"},
				})
				if err == nil && id != "t1-triage_route" {
					err = fmt.Errorf("id = %q", id)
				}
				return err
			}, http.MethodPost, "/api/public/scores",
			`{"id":"t1-triage_route","traceId":"t1","name":"triage_route","value":"fix","dataType":"CATEGORICAL",` +
				`"configId":"cfg-1","queueId":"q-1","source":"ANNOTATION","comment":"guess","metadata":{"by":"bot"},` +
				`"environment":"default"}`,
		},
		{func() error {
			return errOf(collect(client.Scores(ctx, langfuse.ScoreQuery{
				TraceIDs: []string{"t1", "t2"}, ObservationIDs: []string{"o1"}, Names: []string{"a", "b"},
				Sources: []langfuse.ScoreSource{langfuse.ScoreSourceAnnotation}, QueueIDs: []string{"q-1"},
				ConfigIDs: []string{"cfg-1"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeBoolean},
				Values: []string{"true"}, Environments: []string{"default"}, AuthorUserIDs: []string{"u-1"},
				IDs: []string{"s-1"}, From: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
				To: time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("CEST", 2*3600)), PageSize: 100,
			})))
		}, http.MethodGet, "/api/public/v3/scores?authorUserId=u-1&configId=cfg-1&dataType=BOOLEAN&environment=default" +
			"&fields=details%2Csubject%2Cannotation&fromTimestamp=2026-09-30T00%3A00%3A00.000Z&id=s-1&limit=100" +
			"&name=a%2Cb&observationId=o1&queueId=q-1&source=ANNOTATION&toTimestamp=2026-09-30T22%3A00%3A00.000Z" +
			"&traceId=t1%2Ct2&value=true", ""},
		{func() error {
			return errOf(collect(client.Scores(ctx, langfuse.ScoreQuery{
				DatasetRunIDs: []string{"run-1"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric},
				MinValue: new(0.25), MaxValue: new(1e21),
			})))
		}, http.MethodGet, "/api/public/v3/scores?dataType=NUMERIC&experimentId=run-1" +
			"&fields=details%2Csubject%2Cannotation&limit=50&valueMax=1e%2B21&valueMin=0.25", ""},
		{
			func() error {
				id, err := client.CreateComment(ctx, langfuse.CommentSpec{
					ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: " looks right ", AuthorUserID: "u-1",
				})
				if err == nil && id != "c-1" {
					err = fmt.Errorf("id = %q", id)
				}
				return err
			}, http.MethodPost, "/api/public/comments",
			`{"projectId":"","objectType":"TRACE","objectId":"t1","content":" looks right ","authorUserId":"u-1"}`,
		},
		{func() error {
			comment, err := client.GetComment(ctx, "c/1")
			if err == nil && (comment.ID != "c/1" || comment.ObjectType != langfuse.CommentObjectTrace ||
				comment.Content != "looks right" || comment.AuthorUserID != "") {
				err = fmt.Errorf("comment = %+v", comment)
			}
			return err
		}, http.MethodGet, "/api/public/comments/c%2F1", ""},
		{func() error {
			return errOf(collect(client.Comments(ctx, langfuse.CommentQuery{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", AuthorUserID: "u-1",
			})))
		}, http.MethodGet, "/api/public/comments?authorUserId=u-1&limit=50&objectId=t1&objectType=TRACE&page=1", ""},
	} {
		check(test.call, test.method, test.uri, test.body)
	}
}

func TestReviewResponseOutcomes(t *testing.T) {
	t.Parallel()
	notFound := map[string]error{
		"GetScoreConfig": langfuse.ErrScoreConfigNotFound, "UpdateScoreConfig": langfuse.ErrScoreConfigNotFound,
		"GetAnnotationQueue": langfuse.ErrAnnotationQueueNotFound, "AnnotationQueueItems": langfuse.ErrAnnotationQueueNotFound,
		"CreateAnnotationQueueItem": langfuse.ErrAnnotationQueueNotFound,
		"GetAnnotationQueueItem":    langfuse.ErrAnnotationQueueItemNotFound,
		"UpdateAnnotationQueueItem": langfuse.ErrAnnotationQueueItemNotFound,
		"DeleteAnnotationQueueItem": langfuse.ErrAnnotationQueueItemNotFound,
		"GetComment":                langfuse.ErrCommentNotFound, "CreateComment": langfuse.ErrCommentObjectNotFound,
	}
	sentinels := []error{
		langfuse.ErrScoreConfigNotFound, langfuse.ErrAnnotationQueueNotFound, langfuse.ErrAnnotationQueueItemNotFound,
		langfuse.ErrCommentNotFound, langfuse.ErrCommentObjectNotFound, langfuse.ErrWriteOutcomeUnknown,
	}
	for call := range reviewCalls {
		for _, status := range []int{http.StatusNotFound, http.StatusBadRequest, http.StatusServiceUnavailable} {
			t.Run(fmt.Sprintf("%s %d", call, status), func(t *testing.T) {
				t.Parallel()
				var answered atomic.Bool
				server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
					if answered.Swap(true) {
						serveReview(w, r, body)
						return
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"message":"secret"}`)
				})
				client := newDatasetClient(t, server.URL, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := reviewCalls[call](ctx, client)
				retried := status == http.StatusServiceUnavailable && !reviewWrites[call]
				var want error
				switch {
				case status == http.StatusNotFound:
					want = notFound[call]
				case status == http.StatusServiceUnavailable && reviewWrites[call]:
					want = langfuse.ErrWriteOutcomeUnknown
				}
				if (err == nil) != retried {
					t.Fatalf("error = %v, want success only after a retried read", err)
				}
				for _, sentinel := range sentinels {
					if errors.Is(err, sentinel) != (sentinel == want) {
						t.Fatalf("error = %v, want sentinel %v", err, want)
					}
				}
				if err != nil && strings.Contains(err.Error(), "secret") {
					t.Fatalf("error %q leaks the response", err)
				}
				requests := int32(1)
				if retried {
					requests = 2
				}
				if got := server.requests.Load(); got != requests {
					t.Fatalf("requests = %d, want %d", got, requests)
				}
			})
		}
	}
}

func TestReviewRejectsMismatchedResponses(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		call, body string
	}{
		"config name":          {"CreateScoreConfig", scoreConfigJSON("cfg-1", "other")},
		"config archive flag":  {"UpdateScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"), `"isArchived":false,`, "", 1)},
		"config ID":            {"UpdateScoreConfig", scoreConfigJSON("cfg-2", "x")},
		"config data type":     {"UpdateScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"), "CATEGORICAL", "CORRECTION", 1)},
		"config category":      {"UpdateScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"), `"label":"fix"`, `"label":""`, 1)},
		"queue name":           {"CreateAnnotationQueue", queueJSON("q-1", "other")},
		"queue config IDs":     {"CreateAnnotationQueue", strings.Replace(queueJSON("q-1", "alerts"), `"scoreConfigIds":["cfg-1"],`, "", 1)},
		"item object":          {"CreateAnnotationQueueItem", queueItemJSON("i-1", "q-1", "t2", "PENDING")},
		"item queue":           {"CreateAnnotationQueueItem", queueItemJSON("i-1", "q-2", "t1", "PENDING")},
		"item object type":     {"CreateAnnotationQueueItem", strings.Replace(queueItemJSON("i-1", "q-1", "t1", "PENDING"), "TRACE", "PROMPT", 1)},
		"item status":          {"UpdateAnnotationQueueItem", queueItemJSON("i-1", "q-1", "t1", "PENDING")},
		"item ID":              {"UpdateAnnotationQueueItem", queueItemJSON("i-2", "q-1", "t1", "COMPLETED")},
		"delete failure":       {"DeleteAnnotationQueueItem", `{"success":false,"message":"no"}`},
		"assignment queue":     {"AssignAnnotationQueue", `{"userId":"u-1","projectId":"p-1","queueId":"q-2"}`},
		"unassignment failure": {"UnassignAnnotationQueue", `{"success":false}`},
		"assignment user":      {"AssignAnnotationQueue", `{"userId":"u-2","projectId":"p-1","queueId":"q-1"}`},
		"assignment no user":   {"AssignAnnotationQueue", `{"projectId":"p-1","queueId":"q-1"}`},
		"config created type": {"CreateScoreConfig", strings.Replace(numericConfigJSON("cfg-1"),
			`"name":"latency"`, `"name":"triage_route"`, 1)},
		"config categories": {"CreateScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "triage_route"),
			`{"label":"fix","value":0},{"label":"human","value":1}`, `{"label":"human","value":1},{"label":"fix","value":0}`, 1)},
		"config archive ignored": {"UpdateScoreConfig", scoreConfigJSON("cfg-1", "x")},
		"queue extra config ID": {"CreateAnnotationQueue", strings.Replace(queueJSON("q-1", "alerts"),
			`"description":"review","scoreConfigIds":["cfg-1"]`, `"description":null,"scoreConfigIds":["cfg-1","cfg-2"]`, 1)},
		"score ID":        {"CreateScore", `{"id":"other"}`},
		"comment ID":      {"CreateComment", `{"id":""}`},
		"comment ID type": {"CreateComment", `{"id":7}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				_, _ = io.WriteString(w, test.body)
			})
			client := newDatasetClient(t, server.URL, nil)
			if err := reviewCalls[test.call](context.Background(), client); !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrWriteOutcomeUnknown", err)
			}
			if got := server.requests.Load(); got != 1 {
				t.Fatalf("requests = %d; a write must not be repeated", got)
			}
		})
	}
	for name, test := range map[string]struct {
		call, body string
	}{
		"config ID":          {"GetScoreConfig", scoreConfigJSON("cfg-2", "x")},
		"config dates":       {"GetScoreConfig", `{"id":"cfg-1","name":"x","dataType":"TEXT","isArchived":false}`},
		"config page meta":   {"ScoreConfigs", `{"data":[]}`},
		"config page count":  {"ScoreConfigs", `{"data":[],"meta":{"page":1,"limit":50}}`},
		"item page count":    {"AnnotationQueueItems", `{"data":[],"meta":{"totalPages":-1}}`},
		"queue ID":           {"GetAnnotationQueue", queueJSON("q-2", "alerts")},
		"queue entry":        {"AnnotationQueues", numberedPage(1, `{"id":"q-1"}`)},
		"item queue":         {"GetAnnotationQueueItem", queueItemJSON("i-1", "q-2", "t1", "PENDING")},
		"item status":        {"AnnotationQueueItems", numberedPage(1, queueItemJSON("i-1", "q-1", "t1", "DONE"))},
		"score page data":    {"Scores", `{"meta":{"limit":50}}`},
		"score subject":      {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"), `,"subject":{"kind":"trace","id":"t1"}`, "", 1) + `],"meta":{}}`},
		"score source":       {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"), "ANNOTATION", "UI", 1) + `],"meta":{}}`},
		"score value type":   {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"), `"value":"fix"`, `"value":1`, 1) + `],"meta":{}}`},
		"score subject kind": {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"), `"kind":"trace"`, `"kind":"prompt"`, 1) + `],"meta":{}}`},
		"score environment":  {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"), `"environment":"default",`, "", 1) + `],"meta":{}}`},
		"comment ID":         {"GetComment", commentJSON("c-2", "t1")},
		"comment type":       {"Comments", numberedPage(1, strings.Replace(commentJSON("c-1", "t1"), "TRACE", "trace", 1))},
		"comment content":    {"GetComment", strings.Replace(commentJSON("c-1", "t1"), `"content":"looks right",`, "", 1)},
		"category value": {"GetScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"),
			`{"label":"fix","value":0}`, `{"label":"fix"}`, 1)},
		"boolean category order": {"GetScoreConfig", strings.Replace(strings.Replace(scoreConfigJSON("cfg-1", "x"),
			`[{"label":"fix","value":0},{"label":"human","value":1}]`, `[{"label":"False","value":0},{"label":"True","value":1}]`, 1),
			"CATEGORICAL", "BOOLEAN", 1)},
		"numeric categories": {"GetScoreConfig", strings.Replace(numericConfigJSON("cfg-1"), `"categories":null`, `"categories":[]`, 1)},
		"categorical bounds": {"GetScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"), `"minValue":null`, `"minValue":0`, 1)},
		"config name":        {"GetScoreConfig", strings.Replace(scoreConfigJSON("cfg-1", "x"), `"name":"x",`, "", 1)},
		"queue name":         {"GetAnnotationQueue", strings.Replace(queueJSON("q-1", "x"), `"name":"x",`, "", 1)},
		"item parent queue":  {"AnnotationQueueItems", numberedPage(1, queueItemJSON("i-1", "q-2", "t1", "PENDING"))},
		"score details": {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"),
			`"comment":"why","configId":"cfg-1","metadata":{},`, "", 1) + `],"meta":{}}`},
		"score annotation": {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"),
			`"authorUserId":"u-1","queueId":"q-1",`, "", 1) + `],"meta":{}}`},
		"score metadata": {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"),
			`"metadata":{}`, `"metadata":["k"]`, 1) + `],"meta":{}}`},
		"score comment type": {"Scores", `{"data":[` + strings.Replace(v3ScoreJSON("s-1", "t1"),
			`"comment":"why"`, `"comment":7`, 1) + `],"meta":{}}`},
	} {
		t.Run("read "+name, func(t *testing.T) {
			t.Parallel()
			server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				_, _ = io.WriteString(w, test.body)
			})
			client := newDatasetClient(t, server.URL, nil)
			err := reviewCalls[test.call](context.Background(), client)
			if err == nil || errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
				t.Fatalf("error = %v, want a plain failure", err)
			}
		})
	}
}

func TestReviewInvalidInputSendsNothing(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, serveReview)
	var masked atomic.Bool
	client := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.Mask = func(_ langfuse.MaskField, value any) any { masked.Store(true); return value }
	})
	ctx := context.Background()
	categorical := []langfuse.ScoreCategory{{Label: "a", Value: 0}}
	tooLong := strings.Repeat("x", 36)
	invalid := map[string]func() error{
		"config without name": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{DataType: langfuse.ScoreTypeText}))
		},
		"config name too long": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: tooLong, DataType: langfuse.ScoreTypeText}))
		},
		"config name of 36 UTF-16 units": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: strings.Repeat("a", 34) + "𠀀", DataType: langfuse.ScoreTypeText,
			}))
		},
		"config name characters": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: "a/b", DataType: langfuse.ScoreTypeText}))
		},
		"config correction type": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: "a", DataType: langfuse.ScoreTypeCorrection}))
		},
		"config categorical without categories": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: "a", DataType: langfuse.ScoreTypeCategorical}))
		},
		"config boolean categories": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: "a", DataType: langfuse.ScoreTypeBoolean, Categories: categorical,
			}))
		},
		"config duplicate labels": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: "a", DataType: langfuse.ScoreTypeCategorical,
				Categories: []langfuse.ScoreCategory{{Label: "a", Value: 0}, {Label: "a", Value: 1}},
			}))
		},
		"config duplicate values": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: "a", DataType: langfuse.ScoreTypeCategorical,
				Categories: []langfuse.ScoreCategory{{Label: "a", Value: 0}, {Label: "b", Value: 0}},
			}))
		},
		"config text bounds": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: "a", DataType: langfuse.ScoreTypeText, MinValue: new(0.0),
			}))
		},
		"config equal bounds": func() error {
			return errOf(client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
				Name: "a", DataType: langfuse.ScoreTypeNumeric, MinValue: new(1.0), MaxValue: new(1.0),
			}))
		},
		"empty config update": func() error {
			return errOf(client.UpdateScoreConfig(ctx, "cfg-1", langfuse.ScoreConfigUpdate{}))
		},
		"config update empty categories": func() error {
			return errOf(client.UpdateScoreConfig(ctx, "cfg-1", langfuse.ScoreConfigUpdate{Categories: []langfuse.ScoreCategory{}}))
		},
		"config update without ID": func() error {
			return errOf(client.UpdateScoreConfig(ctx, "", langfuse.ScoreConfigUpdate{Archived: new(true)}))
		},
		"queue without configs": func() error {
			return errOf(client.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{Name: "q"}))
		},
		"queue empty config ID": func() error {
			return errOf(client.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{Name: "q", ScoreConfigIDs: []string{""}}))
		},
		"item object type": func() error {
			return errOf(client.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
				QueueID: "q-1", ObjectID: "t1", ObjectType: "trace",
			}))
		},
		"item status": func() error {
			return errOf(client.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
				QueueID: "q-1", ObjectID: "t1", ObjectType: langfuse.AnnotationObjectTrace, Status: "DONE",
			}))
		},
		"item update status": func() error {
			return errOf(client.UpdateAnnotationQueueItem(ctx, "q-1", "i-1", ""))
		},
		"item without queue": func() error { return client.DeleteAnnotationQueueItem(ctx, "", "i-1") },
		"assignment user":    func() error { return client.AssignAnnotationQueue(ctx, "q-1", "") },
		"annotation score without config": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", StringValue: new("x"), Source: langfuse.ScoreSourceAnnotation,
			}))
		},
		"eval score": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", NumericValue: new(1.0), Source: langfuse.ScoreSourceEval,
			}))
		},
		"timestamped score": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", NumericValue: new(1.0), Timestamp: time.Now(),
			}))
		},
		"score without target": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{Name: "a", NumericValue: new(1.0), Metadata: map[string]any{"k": 1}}))
		},
		"recorded annotation score": func() error {
			return client.RecordScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", StringValue: new("x"), ConfigID: "c", Source: langfuse.ScoreSourceAnnotation,
			})
		},
		"comment without content": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: " \n ",
			}))
		},
		"comment too long": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: strings.Repeat("é", 5001),
			}))
		},
		"TEXT score of 502 UTF-16 units": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", DataType: langfuse.ScoreTypeText, StringValue: new(strings.Repeat("𠀀", 251)),
			}))
		},
		"recorded TEXT score of 502 UTF-16 units": func() error {
			return client.RecordScore(ctx, langfuse.Score{
				TraceID: "t1", Name: "a", DataType: langfuse.ScoreTypeText, StringValue: new(strings.Repeat("𠀀", 251)),
			})
		},
		"NaN score": func() error {
			return errOf(client.CreateScore(ctx, langfuse.Score{TraceID: "t1", Name: "a", NumericValue: new(math.NaN())}))
		},
		"comment of 5001 UTF-16 units": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: strings.Repeat("𠀀", 2500) + "a",
			}))
		},
		"comment of JavaScript whitespace": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectTrace, ObjectID: "t1", Content: "\uFEFF\u3000\u2028 ",
			}))
		},
		"comment object start time": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{
				ObjectType: langfuse.CommentObjectObservation, ObjectID: "o1", Content: "x",
				ObjectStartTime: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
			}))
		},
		"experiment query padded name": func() error {
			return errOf(collect(client.Experiments(ctx, langfuse.ExperimentQuery{From: time.Now(), Names: []string{"run "}})))
		},
		"comment object type": func() error {
			return errOf(client.CreateComment(ctx, langfuse.CommentSpec{ObjectType: "DATASET", ObjectID: "t1", Content: "x"}))
		},
		"comment query object without type": func() error {
			return errOf(collect(client.Comments(ctx, langfuse.CommentQuery{ObjectID: "t1"})))
		},
		"queue page size": func() error {
			return errOf(collect(client.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{PageSize: 101})))
		},
		"items without queue": func() error {
			return errOf(collect(client.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{})))
		},
	}
	for name, query := range map[string]langfuse.ScoreQuery{
		"trace and session":      {TraceIDs: []string{"t1"}, SessionIDs: []string{"s1"}},
		"session and run":        {SessionIDs: []string{"s1"}, DatasetRunIDs: []string{"r1"}},
		"observation alone":      {ObservationIDs: []string{"o1"}},
		"comma":                  {Names: []string{"a,b"}},
		"blank value":            {TraceIDs: []string{" "}},
		"padded value":           {ConfigIDs: []string{"\tcfg-1"}},
		"empty value":            {IDs: []string{""}},
		"source":                 {Sources: []langfuse.ScoreSource{"api"}},
		"data type":              {DataTypes: []langfuse.ScoreDataType{"numeric"}},
		"values without type":    {Values: []string{"1"}},
		"text values":            {Values: []string{"x"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeText}},
		"bounds without numeric": {MinValue: new(1.0), DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeBoolean}},
		"infinite bound":         {MaxValue: new(math.Inf(1)), DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"boolean 0":              {Values: []string{"0"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeBoolean}},
		"boolean TRUE":           {Values: []string{"TRUE"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeBoolean}},
		"numeric NaN":            {Values: []string{"NaN"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"numeric Infinity":       {Values: []string{"Infinity"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"numeric overflow":       {Values: []string{"1e400"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"numeric bare prefix":    {Values: []string{"0x"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"numeric signed hex":     {Values: []string{"-0x10"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"numeric underscore":     {Values: []string{"1_0"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		"time range":             {From: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		"page size":              {PageSize: -1},
	} {
		invalid["score query "+name] = func() error { return errOf(collect(client.Scores(ctx, query))) }
	}
	for name, call := range invalid {
		if err := call(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if got := server.requests.Load(); got != 0 || masked.Load() {
		t.Fatalf("invalid input sent %d requests, masked %t", got, masked.Load())
	}
}

func TestReviewUnavailableClientsSendNothing(t *testing.T) {
	t.Parallel()
	var masked atomic.Bool
	mask := func(langfuse.MaskField, any) any { masked.Store(true); return nil }
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true, Mask: mask})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server := newDatasetServer(t, serveReview)
	stopped := newDatasetClient(t, server.URL, func(config *langfuse.Config) { config.Mask = mask })
	if err := stopped.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	for state, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled, "stopped": stopped} {
		for name, call := range reviewCalls {
			if err := call(context.Background(), client); err == nil {
				t.Errorf("%s client: %s succeeded", state, name)
			}
		}
		for _, id := range []string{"", "fixed"} {
			if got, err := client.CreateScore(context.Background(), langfuse.Score{
				ID: id, TraceID: "t1", Name: "a", NumericValue: new(1.0),
			}); got != "" || err == nil {
				t.Errorf("%s client: CreateScore(ID %q) = %q, %v; want no ID and an error", state, id, got, err)
			}
		}
	}
	//nolint:staticcheck // a nil context is the case under test
	if got, err := stopped.CreateScore(nil, langfuse.Score{ID: "fixed", TraceID: "t1", Name: "a", NumericValue: new(1.0)}); got != "" || err == nil {
		t.Errorf("CreateScore(nil context) = %q, %v; want no ID and an error", got, err)
	}
	if masked.Load() || server.requests.Load() != 0 {
		t.Fatalf("unavailable clients masked %t and sent %d requests", masked.Load(), server.requests.Load())
	}
}

func TestScoresPagesByCursorAndDecodes(t *testing.T) {
	t.Parallel()
	pages := map[string]string{
		"": `{"data":[` + strings.Join([]string{
			`{"id":"n","name":"n","dataType":"NUMERIC","value":0.5,` + v3ScoreCore + `,"comment":null,"configId":null,` +
				`"metadata":{"k":"v","n":12345678901234567890},"authorUserId":null,"queueId":null,` +
				`"subject":{"kind":"observation","id":"o1","traceId":"t1"}}`,
			`{"id":"b","name":"b","dataType":"BOOLEAN","value":true,` + v3ScoreCore + `,"comment":null,"configId":null,` +
				`"metadata":{},"authorUserId":null,"queueId":null,"subject":{"kind":"session","id":"s1"}}`,
		}, ",") + `],"meta":{"limit":2,"cursor":"c1"}}`,
		"c1": `{"data":[` + strings.Join([]string{
			v3ScoreJSON("c", "t1"),
			`{"id":"x","name":"output","dataType":"CORRECTION","value":"fixed text",` + v3ScoreCore + `,"comment":null,` +
				`"configId":null,"metadata":{},"authorUserId":null,"queueId":null,"subject":{"kind":"experiment","id":"r1"}}`,
		}, ",") + `],"meta":{"limit":2}}`,
	}
	var mu sync.Mutex
	var queries []url.Values
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		mu.Lock()
		queries = append(queries, r.URL.Query())
		mu.Unlock()
		_, _ = io.WriteString(w, pages[r.URL.Query().Get("cursor")])
	})
	client := newDatasetClient(t, server.URL, nil)
	scores, err := collect(client.Scores(context.Background(), langfuse.ScoreQuery{Names: []string{"n"}, PageSize: 2}))
	if err != nil || len(scores) != 4 {
		t.Fatalf("Scores() = %d scores, %v", len(scores), err)
	}
	if len(queries) != 2 || queries[1].Get("cursor") != "c1" || queries[1].Get("name") != "n" || queries[1].Get("limit") != "2" {
		t.Fatalf("queries = %v, want the filters resent with the cursor", queries)
	}
	numeric, boolean, categorical, correction := scores[0], scores[1], scores[2], scores[3]
	if *numeric.NumericValue != 0.5 || numeric.TraceID != "t1" || numeric.ObservationID != "o1" ||
		numeric.Source != langfuse.ScoreSourceAnnotation || numeric.Environment != "default" ||
		numeric.Metadata["n"] != json.Number("12345678901234567890") || numeric.Comment != "" || numeric.ConfigID != "" ||
		!numeric.UpdatedAt.Equal(time.Date(2026, 9, 30, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("numeric = %+v", numeric)
	}
	if *boolean.NumericValue != 1 || boolean.StringValue != nil || boolean.SessionID != "s1" || boolean.Metadata != nil {
		t.Fatalf("boolean = %+v", boolean)
	}
	if *categorical.StringValue != "fix" || categorical.ConfigID != "cfg-1" || categorical.QueueID != "q-1" ||
		categorical.AuthorUserID != "u-1" || categorical.Comment != "why" || categorical.TraceID != "t1" {
		t.Fatalf("categorical = %+v", categorical)
	}
	if *correction.StringValue != "fixed text" || correction.DatasetRunID != "r1" || correction.TraceID != "" {
		t.Fatalf("correction = %+v", correction)
	}

	repeating := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, `{"data":[],"meta":{"limit":50,"cursor":"same"}}`)
	})
	client = newDatasetClient(t, repeating.URL, nil)
	if _, err := collect(client.Scores(context.Background(), langfuse.ScoreQuery{})); err == nil {
		t.Fatal("Scores() followed a repeating cursor without an error")
	}
	if got := repeating.requests.Load(); got != 2 {
		t.Fatalf("repeating cursor requests = %d, want 2", got)
	}
}

func TestReviewListingsPage(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		page := r.URL.Query().Get("page")
		switch r.URL.Path {
		case "/api/public/score-configs":
			_, _ = io.WriteString(w, numberedPage(2, scoreConfigJSON("cfg-"+page, "x")))
		case "/api/public/annotation-queues/q-1/items":
			_, _ = io.WriteString(w, numberedPage(3, queueItemJSON("i-"+page, "q-1", "t1", "PENDING")))
		case "/api/public/comments":
			_, _ = io.WriteString(w, numberedPage(2, commentJSON("c-"+page, "t1")))
		default:
			_, _ = io.WriteString(w, numberedPage(1))
		}
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	configs, err := collect(client.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{}))
	if err != nil || len(configs) != 2 || configs[1].ID != "cfg-2" {
		t.Fatalf("ScoreConfigs() = %+v, %v", configs, err)
	}
	items, err := collect(client.AnnotationQueueItems(ctx, langfuse.AnnotationQueueItemQuery{QueueID: "q-1"}))
	if err != nil || len(items) != 3 || items[2].ID != "i-3" {
		t.Fatalf("AnnotationQueueItems() = %+v, %v", items, err)
	}
	comments, err := collect(client.Comments(ctx, langfuse.CommentQuery{}))
	if err != nil || len(comments) != 2 || comments[1].ID != "c-2" {
		t.Fatalf("Comments() = %+v, %v", comments, err)
	}
	queues, err := collect(client.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{}))
	if err != nil || len(queues) != 0 {
		t.Fatalf("AnnotationQueues() = %+v, %v", queues, err)
	}
}

func TestCreateScoreGeneratesAndReturnsItsID(t *testing.T) {
	t.Parallel()
	var sent atomic.Value
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, body []byte) {
		var score struct {
			ID          string         `json:"id"`
			Environment string         `json:"environment"`
			Metadata    map[string]any `json:"metadata"`
			Source      *string        `json:"source"`
		}
		_ = json.Unmarshal(body, &score)
		sent.Store(score)
		_, _ = fmt.Fprintf(w, `{"id":%q}`, score.ID)
	})
	client := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.Environment = "staging"
		config.Mask = func(field langfuse.MaskField, value any) any {
			if field == langfuse.MaskScoreMetadata {
				return map[string]any{"masked": true}
			}
			return value
		}
	})
	id, err := client.CreateScore(context.Background(), langfuse.Score{
		SessionID: "s1", Name: "quality", NumericValue: new(0.75), Metadata: map[string]any{"raw": "secret"},
	})
	score := sent.Load().(struct {
		ID          string         `json:"id"`
		Environment string         `json:"environment"`
		Metadata    map[string]any `json:"metadata"`
		Source      *string        `json:"source"`
	})
	if err != nil || len(id) != 36 || id != score.ID {
		t.Fatalf("CreateScore() = %q, %v; sent ID %q", id, err, score.ID)
	}
	if score.Environment != "staging" || score.Metadata["masked"] != true || score.Source != nil {
		t.Fatalf("sent %+v, want the client environment, masked metadata, and no source", score)
	}

	failing := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(http.StatusBadGateway)
	})
	client = newDatasetClient(t, failing.URL, nil)
	id, err = client.CreateScore(context.Background(), langfuse.Score{TraceID: "t1", Name: "q", NumericValue: new(1.0)})
	if !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) || len(id) != 36 {
		t.Fatalf("CreateScore() = %q, %v; want the generated ID with ErrWriteOutcomeUnknown", id, err)
	}
}

func TestReviewAcceptsServerLimitsAndStoredShapes(t *testing.T) {
	t.Parallel()
	var bodies []string
	var mu sync.Mutex
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/public/score-configs/"):
			_, _ = io.WriteString(w, strings.Replace(scoreConfigJSON("cfg-1", "x"),
				`[{"label":"fix","value":0},{"label":"human","value":1}]`, `[]`, 1))
		case strings.HasPrefix(r.URL.Path, "/api/public/score-configs"):
			_, _ = io.WriteString(w, scoreConfigJSON("cfg-1", "other"))
		case strings.HasPrefix(r.URL.Path, "/api/public/annotation-queues"):
			_, _ = io.WriteString(w, queueJSON("q-1", ""))
		default:
			serveReview(w, r, body)
		}
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	name := strings.Repeat("a", 33) + "𠀀" // 35 UTF-16 code units
	// The fake answers with another name, so the sent write cannot be confirmed.
	_, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: name, DataType: langfuse.ScoreTypeText})
	if !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
		t.Fatalf("CreateScoreConfig(35 units) error = %v, want the fake's name mismatch", err)
	}
	if config, err := client.GetScoreConfig(ctx, "cfg-1"); err != nil || config.Categories == nil || len(config.Categories) != 0 {
		t.Fatalf("GetScoreConfig(stored empty categories) = %+v, %v", config, err)
	}
	if queue, err := client.GetAnnotationQueue(ctx, "q-1"); err != nil || queue.Name != "" {
		t.Fatalf("GetAnnotationQueue(stored empty name) = %+v, %v", queue, err)
	}
	if _, err := client.CreateScore(ctx, langfuse.Score{
		TraceID: "t1", Name: "a", DataType: langfuse.ScoreTypeText, StringValue: new(strings.Repeat("𠀀", 250)),
	}); !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
		t.Fatalf("CreateScore(500 units) error = %v, want only the fake's ID mismatch", err)
	}
	for _, query := range []langfuse.ScoreQuery{
		{Values: []string{"0x10", ".5", "-1e3", "5.", "0b11", "0O17"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeNumeric}},
		{Values: []string{"false", "true"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeBoolean}},
		{Values: []string{"0", "TRUE"}, DataTypes: []langfuse.ScoreDataType{langfuse.ScoreTypeCategorical}},
	} {
		if _, err := collect(client.Scores(ctx, query)); err != nil {
			t.Fatalf("Scores(%v) error = %v", query.Values, err)
		}
	}
	for _, content := range []string{"\uFEFF" + strings.Repeat("𠀀", 2500) + " ", "\u0085"} {
		if _, err := client.CreateComment(ctx, langfuse.CommentSpec{
			ObjectType: langfuse.CommentObjectObservation, ObjectID: "o1", Content: content,
			ObjectStartTime: time.Date(2026, 9, 30, 10, 0, 0, 123456789, time.FixedZone("CEST", 2*3600)),
		}); err != nil {
			t.Fatalf("CreateComment(%d bytes) error = %v", len(content), err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	last := bodies[len(bodies)-1]
	if !sameJSON(t, last, `{"projectId":"","objectType":"OBSERVATION","objectId":"o1","content":"\u0085",`+
		`"objectStartTime":"2026-09-30T08:00:00.123Z"}`) {
		t.Fatalf("comment body = %s", last)
	}
	if !strings.Contains(bodies[0], `"name":"`+name+`"`) {
		t.Fatalf("config body = %s, want the 35-unit name sent", bodies[0])
	}
}

func TestAnnotationQueueItemsKeepTheirCompletionTime(t *testing.T) {
	t.Parallel()
	reopened := strings.Replace(queueItemJSON("i-1", "q-1", "t1", "COMPLETED"), `"status":"COMPLETED"`, `"status":"PENDING"`, 1)
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/items/i-1") {
			_, _ = io.WriteString(w, reopened)
			return
		}
		serveReview(w, r, body)
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	completedAt := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	item, err := client.GetAnnotationQueueItem(ctx, "q-1", "i-1")
	if err != nil || item.Status != langfuse.AnnotationPending || !item.CompletedAt.Equal(completedAt) {
		t.Fatalf("GetAnnotationQueueItem(reopened) = %+v, %v; want PENDING with the last completion time", item, err)
	}
	item, err = client.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
		QueueID: "q-1", ObjectID: "t1", ObjectType: langfuse.AnnotationObjectTrace, Status: langfuse.AnnotationCompleted,
	})
	if err != nil || item.Status != langfuse.AnnotationCompleted || !item.CompletedAt.Equal(completedAt) {
		t.Fatalf("CreateAnnotationQueueItem(COMPLETED) = %+v, %v", item, err)
	}
}

func TestCreateScoreConfigTypes(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, serveReview)
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	boolean, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: "ok", DataType: langfuse.ScoreTypeBoolean})
	if err != nil || boolean.DataType != langfuse.ScoreTypeBoolean ||
		!slices.Equal(boolean.Categories, []langfuse.ScoreCategory{{Label: "True", Value: 1}, {Label: "False", Value: 0}}) {
		t.Fatalf("CreateScoreConfig(BOOLEAN) = %+v, %v", boolean, err)
	}
	text, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{Name: "why", DataType: langfuse.ScoreTypeText})
	if err != nil || text.DataType != langfuse.ScoreTypeText || text.Categories != nil {
		t.Fatalf("CreateScoreConfig(TEXT) = %+v, %v", text, err)
	}
	numeric, err := client.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
		Name: "latency", DataType: langfuse.ScoreTypeNumeric, MinValue: new(0.0),
	})
	if err != nil || numeric.MinValue == nil || *numeric.MinValue != 0 || numeric.MaxValue != nil {
		t.Fatalf("CreateScoreConfig(NUMERIC, one bound) = %+v, %v", numeric, err)
	}
}
