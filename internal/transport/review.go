package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
)

// ScoreConfig is one decoded score config.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type ScoreConfig struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	DataType    string          `json:"dataType"`
	IsArchived  *bool           `json:"isArchived"`
	MinValue    *float64        `json:"minValue"`
	MaxValue    *float64        `json:"maxValue"`
	Categories  []ScoreCategory `json:"categories"`
	Description string          `json:"description"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
}

// ScoreCategory is one category of a categorical or boolean score config.
type ScoreCategory struct {
	Label string   `json:"label"`
	Value *float64 `json:"value"`
}

// valid applies the server's config union: NUMERIC and TEXT have no
// categories, CATEGORICAL and TEXT no bounds, BOOLEAN exactly "True" (1)
// then "False" (0), and categories unique labels and values.
func (config *ScoreConfig) valid() bool {
	labels, values := map[string]bool{}, map[float64]bool{}
	for _, category := range config.Categories {
		if category.Label == "" || category.Value == nil || labels[category.Label] || values[*category.Value] {
			return false
		}
		labels[category.Label], values[*category.Value] = true, true
	}
	bounded := config.MinValue != nil || config.MaxValue != nil
	switch config.DataType {
	case "NUMERIC":
		if config.Categories != nil ||
			config.MinValue != nil && config.MaxValue != nil && *config.MaxValue <= *config.MinValue {
			return false
		}
	case "BOOLEAN":
		if len(config.Categories) != 2 || config.Categories[0].Label != "True" || *config.Categories[0].Value != 1 ||
			config.Categories[1].Label != "False" || *config.Categories[1].Value != 0 {
			return false
		}
	case "CATEGORICAL":
		if config.Categories == nil || bounded {
			return false
		}
	case "TEXT":
		if config.Categories != nil || bounded {
			return false
		}
	default:
		return false
	}
	return config.ID != "" && config.Name != "" && config.IsArchived != nil &&
		!config.CreatedAt.IsZero() && !config.UpdatedAt.IsZero()
}

// ScoreConfigChange lists what a config write set; the response must report
// it. Empty and nil fields were not set.
type ScoreConfigChange struct {
	Name        string
	DataType    string
	Archived    *bool
	Description *string
	Categories  []ScoreCategory
	MinValue    *float64
	MaxValue    *float64
}

func (config *ScoreConfig) reports(change ScoreConfigChange) bool {
	same := func(want, got *float64) bool { return want == nil || got != nil && *got == *want }
	return (change.Name == "" || config.Name == change.Name) &&
		(change.DataType == "" || config.DataType == change.DataType) &&
		(change.Archived == nil || *config.IsArchived == *change.Archived) &&
		(change.Description == nil || config.Description == *change.Description) &&
		(change.Categories == nil || slices.EqualFunc(config.Categories, change.Categories,
			func(got, want ScoreCategory) bool { return got.Label == want.Label && same(want.Value, got.Value) })) &&
		same(change.MinValue, config.MinValue) && same(change.MaxValue, config.MaxValue)
}

// CreateScoreConfig posts one serialized score config body.
func (d *RESTClient) CreateScoreConfig(ctx context.Context, body []byte, change ScoreConfigChange) (ScoreConfig, error) {
	call := restCall{
		op: "score config create", method: http.MethodPost, url: d.base + "/score-configs",
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(config *ScoreConfig) bool { return config.valid() && config.reports(change) })
}

// GetScoreConfig reads one score config by ID.
func (d *RESTClient) GetScoreConfig(ctx context.Context, id string) (ScoreConfig, error) {
	call := restCall{
		op: "score config read", method: http.MethodGet,
		url: d.base + "/score-configs/" + url.PathEscape(id), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(config *ScoreConfig) bool { return config.valid() && config.ID == id })
}

// UpdateScoreConfig patches one score config by ID.
func (d *RESTClient) UpdateScoreConfig(
	ctx context.Context, id string, body []byte, change ScoreConfigChange,
) (ScoreConfig, error) {
	call := restCall{
		op: "score config update", method: http.MethodPatch,
		url: d.base + "/score-configs/" + url.PathEscape(id), body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(config *ScoreConfig) bool {
		return config.valid() && config.ID == id && config.reports(change)
	})
}

// ListScoreConfigs reads one page of score configs.
func (d *RESTClient) ListScoreConfigs(ctx context.Context, page, limit int) (Page[ScoreConfig, PageMeta], error) {
	return listPage[ScoreConfig, PageMeta](ctx, d, "score config list", "/score-configs", pageQuery(page, limit),
		(*ScoreConfig).valid)
}

// AnnotationQueue is one decoded annotation queue. Its name may be empty
// but not absent.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type AnnotationQueue struct {
	ID             string    `json:"id"`
	Name           *string   `json:"name"`
	Description    string    `json:"description"`
	ScoreConfigIDs []string  `json:"scoreConfigIds"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

func (queue *AnnotationQueue) valid() bool {
	return queue.ID != "" && queue.Name != nil && queue.ScoreConfigIDs != nil &&
		!queue.CreatedAt.IsZero() && !queue.UpdatedAt.IsZero()
}

// CreateAnnotationQueue posts one serialized annotation queue body. The
// response must report the requested name, description, and configs.
func (d *RESTClient) CreateAnnotationQueue(
	ctx context.Context, body []byte, name, description string, scoreConfigIDs []string,
) (AnnotationQueue, error) {
	call := restCall{
		op: "annotation queue create", method: http.MethodPost, url: d.base + "/annotation-queues",
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(queue *AnnotationQueue) bool {
		return queue.valid() && *queue.Name == name && queue.Description == description &&
			slices.Equal(queue.ScoreConfigIDs, scoreConfigIDs)
	})
}

// GetAnnotationQueue reads one annotation queue by ID.
func (d *RESTClient) GetAnnotationQueue(ctx context.Context, id string) (AnnotationQueue, error) {
	call := restCall{
		op: "annotation queue read", method: http.MethodGet,
		url: d.base + "/annotation-queues/" + url.PathEscape(id), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(queue *AnnotationQueue) bool { return queue.valid() && queue.ID == id })
}

// ListAnnotationQueues reads one page of annotation queues.
func (d *RESTClient) ListAnnotationQueues(ctx context.Context, page, limit int) (Page[AnnotationQueue, PageMeta], error) {
	return listPage[AnnotationQueue, PageMeta](ctx, d, "annotation queue list", "/annotation-queues",
		pageQuery(page, limit), (*AnnotationQueue).valid)
}

// AnnotationQueueItem is one decoded annotation queue item.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type AnnotationQueueItem struct {
	ID          string     `json:"id"`
	QueueID     string     `json:"queueId"`
	ObjectID    string     `json:"objectId"`
	ObjectType  string     `json:"objectType"`
	Status      string     `json:"status"`
	CompletedAt *time.Time `json:"completedAt"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
}

// valid accepts an item of queueID. completedAt is nullable whatever the
// status: a pending item may keep the time it was last completed.
func (item *AnnotationQueueItem) valid(queueID string) bool {
	switch item.ObjectType {
	case "TRACE", "OBSERVATION", "SESSION":
	default:
		return false
	}
	return item.ID != "" && item.QueueID == queueID && item.ObjectID != "" &&
		(item.Status == "PENDING" || item.Status == "COMPLETED") &&
		!item.CreatedAt.IsZero() && !item.UpdatedAt.IsZero()
}

func queueItemsPath(queueID string) string {
	return "/annotation-queues/" + url.PathEscape(queueID) + "/items"
}

// CreateAnnotationQueueItem posts one serialized queue item body. The
// response must echo the queue, the object, and a requested status.
func (d *RESTClient) CreateAnnotationQueueItem(
	ctx context.Context, queueID string, body []byte, want AnnotationQueueItem,
) (AnnotationQueueItem, error) {
	call := restCall{
		op: "annotation queue item create", method: http.MethodPost, url: d.base + queueItemsPath(queueID),
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *AnnotationQueueItem) bool {
		return item.valid(queueID) && item.ObjectID == want.ObjectID && item.ObjectType == want.ObjectType &&
			(want.Status == "" || item.Status == want.Status)
	})
}

// GetAnnotationQueueItem reads one queue item.
func (d *RESTClient) GetAnnotationQueueItem(ctx context.Context, queueID, itemID string) (AnnotationQueueItem, error) {
	call := restCall{
		op: "annotation queue item read", method: http.MethodGet,
		url: d.base + queueItemsPath(queueID) + "/" + url.PathEscape(itemID), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *AnnotationQueueItem) bool { return item.valid(queueID) && item.ID == itemID })
}

// UpdateAnnotationQueueItem patches one queue item's status.
func (d *RESTClient) UpdateAnnotationQueueItem(
	ctx context.Context, queueID, itemID string, body []byte, status string,
) (AnnotationQueueItem, error) {
	call := restCall{
		op: "annotation queue item update", method: http.MethodPatch,
		url:  d.base + queueItemsPath(queueID) + "/" + url.PathEscape(itemID),
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *AnnotationQueueItem) bool {
		return item.valid(queueID) && item.ID == itemID && item.Status == status
	})
}

// DeleteAnnotationQueueItem removes one queue item.
func (d *RESTClient) DeleteAnnotationQueueItem(ctx context.Context, queueID, itemID string) error {
	call := restCall{
		op: "annotation queue item delete", method: http.MethodDelete,
		url:   d.base + queueItemsPath(queueID) + "/" + url.PathEscape(itemID),
		write: true, limit: restResponseLimit,
	}
	_, err := send(ctx, d, call, succeeded)
	return err
}

// ListAnnotationQueueItems reads one page of the items of queueID; status
// may be empty.
func (d *RESTClient) ListAnnotationQueueItems(
	ctx context.Context, queueID, status string, page, limit int,
) (Page[AnnotationQueueItem, PageMeta], error) {
	query := pageQuery(page, limit)
	if status != "" {
		query.Set("status", status)
	}
	return listPage[AnnotationQueueItem, PageMeta](ctx, d, "annotation queue item list", queueItemsPath(queueID),
		query, func(item *AnnotationQueueItem) bool {
			return item.valid(queueID) && (status == "" || item.Status == status)
		})
}

// AssignAnnotationQueue assigns userID to a queue when assign is true and
// removes the assignment otherwise; body holds the user ID.
func (d *RESTClient) AssignAnnotationQueue(
	ctx context.Context, queueID, userID string, body []byte, assign bool,
) error {
	call := restCall{
		op: "annotation queue assignment", method: http.MethodPost,
		url:  d.base + "/annotation-queues/" + url.PathEscape(queueID) + "/assignments",
		body: body, write: true, limit: restResponseLimit,
	}
	if !assign {
		call.op, call.method = "annotation queue unassignment", http.MethodDelete
		_, err := send(ctx, d, call, succeeded)
		return err
	}
	_, err := send(ctx, d, call, func(assignment *queueAssignment) bool {
		return assignment.QueueID == queueID && assignment.UserID == userID
	})
	return err
}

//nolint:tagliatelle // Langfuse wire keys are camelCase.
type queueAssignment struct {
	QueueID string `json:"queueId"`
	UserID  string `json:"userId"`
}

// succeeded accepts the {"success": true} body of a delete.
func succeeded(response *struct {
	Success bool `json:"success"`
},
) bool {
	return response.Success
}

// CreateScore posts one serialized score body. The response must echo id.
func (d *RESTClient) CreateScore(ctx context.Context, body []byte, id string) error {
	call := restCall{
		op: "score create", method: http.MethodPost, url: d.base + "/scores",
		body: body, write: true, limit: restResponseLimit,
	}
	_, err := send(ctx, d, call, func(created *createdObject) bool { return created.ID == id })
	return err
}

type createdObject struct {
	ID string `json:"id"`
}

// ScoreRecord is one decoded score in the v3 shape, as score and experiment
// reads return it. The core fields and the subject are always present. The
// details group (comment, configId, metadata) and the annotation group
// (authorUserId, queueId) are present in score reads and absent from
// experiment reads; absent members are nil and null ones "null".
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type ScoreRecord struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	DataType     string          `json:"dataType"`
	Value        json.RawMessage `json:"value"`
	Source       string          `json:"source"`
	Timestamp    time.Time       `json:"timestamp"`
	Environment  string          `json:"environment"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
	Comment      json.RawMessage `json:"comment"`
	ConfigID     json.RawMessage `json:"configId"`
	Metadata     json.RawMessage `json:"metadata"`
	AuthorUserID json.RawMessage `json:"authorUserId"`
	QueueID      json.RawMessage `json:"queueId"`
	Subject      *struct {
		Kind    string `json:"kind"`
		ID      string `json:"id"`
		TraceID string `json:"traceId"`
	} `json:"subject"`
}

// valid checks the core fields and subject, and the type of any optional
// member that is present: the value's JSON type must match the data type.
func (score *ScoreRecord) valid() bool {
	value := bytes.TrimSpace(score.Value)
	if score.ID == "" || score.Name == "" || len(value) == 0 || string(value) == "null" {
		return false
	}
	switch score.DataType {
	case "NUMERIC":
		var number float64
		if value[0] == '"' || json.Unmarshal(value, &number) != nil {
			return false
		}
	case "BOOLEAN":
		if string(value) != "true" && string(value) != "false" {
			return false
		}
	case "CATEGORICAL", "TEXT", "CORRECTION":
		if _, ok := NullableString(value); !ok {
			return false
		}
	default:
		return false
	}
	switch score.Source {
	case "API", "ANNOTATION", "EVAL":
	default:
		return false
	}
	if score.Environment == "" || score.Timestamp.IsZero() || score.CreatedAt.IsZero() || score.UpdatedAt.IsZero() {
		return false
	}
	subject := score.Subject
	if subject == nil || subject.ID == "" || subject.Kind != "observation" && subject.TraceID != "" {
		return false
	}
	switch subject.Kind {
	case "trace", "observation", "session", "experiment":
	default:
		return false
	}
	for _, member := range []json.RawMessage{score.Comment, score.ConfigID, score.AuthorUserID, score.QueueID} {
		if _, ok := NullableString(member); !ok {
			return false
		}
	}
	metadata := bytes.TrimSpace(score.Metadata)
	return len(metadata) == 0 || string(metadata) == "null" || metadata[0] == '{'
}

// validFull also requires the details and annotation groups that score reads
// request.
func (score *ScoreRecord) validFull() bool {
	return score.valid() && score.Comment != nil && score.ConfigID != nil && score.Metadata != nil &&
		score.AuthorUserID != nil && score.QueueID != nil
}

// NullableString decodes an optional JSON string member: absent and null
// are "".
func NullableString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", true
	}
	var text string
	return text, json.Unmarshal(raw, &text) == nil
}

// ListScores reads one page of v3 scores.
func (d *RESTClient) ListScores(ctx context.Context, query url.Values) (Page[ScoreRecord, CursorMeta], error) {
	return listPage[ScoreRecord, CursorMeta](ctx, d, "score list", "/v3/scores", query, (*ScoreRecord).validFull)
}

// Comment is one decoded comment.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type Comment struct {
	ID           string    `json:"id"`
	ObjectType   string    `json:"objectType"`
	ObjectID     string    `json:"objectId"`
	Content      string    `json:"content"`
	AuthorUserID string    `json:"authorUserId"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func (comment *Comment) valid() bool {
	switch comment.ObjectType {
	case "TRACE", "OBSERVATION", "SESSION", "PROMPT":
	default:
		return false
	}
	return comment.ID != "" && comment.ObjectID != "" && comment.Content != "" &&
		!comment.CreatedAt.IsZero() && !comment.UpdatedAt.IsZero()
}

// CreateComment posts one serialized comment body and returns the new ID.
func (d *RESTClient) CreateComment(ctx context.Context, body []byte) (string, error) {
	call := restCall{
		op: "comment create", method: http.MethodPost, url: d.base + "/comments",
		body: body, write: true, limit: restResponseLimit,
	}
	created, err := send(ctx, d, call, func(created *createdObject) bool { return validIdentifier(created.ID) })
	return created.ID, err
}

// GetComment reads one comment by ID.
func (d *RESTClient) GetComment(ctx context.Context, id string) (Comment, error) {
	call := restCall{
		op: "comment read", method: http.MethodGet,
		url: d.base + "/comments/" + url.PathEscape(id), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(comment *Comment) bool { return comment.valid() && comment.ID == id })
}

// ListComments reads one page of comments.
func (d *RESTClient) ListComments(ctx context.Context, query url.Values) (Page[Comment, PageMeta], error) {
	return listPage[Comment, PageMeta](ctx, d, "comment list", "/comments", query, (*Comment).valid)
}

func pageQuery(page, limit int) url.Values {
	values := url.Values{}
	values.Set("page", strconv.Itoa(page))
	values.Set("limit", strconv.Itoa(limit))
	return values
}
