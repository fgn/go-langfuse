package langfuse

import (
	"context"
	"errors"
	"iter"
	"time"
	"unicode/utf8"

	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrAnnotationQueueNotFound reports that the annotation queue does not
// exist.
var ErrAnnotationQueueNotFound = errors.New("langfuse: annotation queue not found")

// ErrAnnotationQueueItemNotFound reports that the annotation queue item, or
// its queue, does not exist.
var ErrAnnotationQueueItemNotFound = errors.New("langfuse: annotation queue item not found")

// AnnotationObjectType is the kind of object an annotation queue item
// reviews.
type AnnotationObjectType string

const (
	AnnotationObjectTrace       AnnotationObjectType = "TRACE"
	AnnotationObjectObservation AnnotationObjectType = "OBSERVATION"
	AnnotationObjectSession     AnnotationObjectType = "SESSION"
)

// AnnotationStatus is the review state of an annotation queue item.
type AnnotationStatus string

const (
	AnnotationPending   AnnotationStatus = "PENDING"
	AnnotationCompleted AnnotationStatus = "COMPLETED"
)

// AnnotationQueueSpec creates an annotation queue.
type AnnotationQueueSpec struct {
	// Name is required.
	Name string
	// Description is shown to reviewers. It is not masked.
	Description string
	// ScoreConfigIDs are the score configs reviewers fill in, in display
	// order.
	ScoreConfigIDs []string
}

// AnnotationQueue is an annotation queue as stored by Langfuse.
type AnnotationQueue struct {
	ID             string
	Name           string
	Description    string
	ScoreConfigIDs []string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// AnnotationQueueQuery configures [Client.AnnotationQueues].
type AnnotationQueueQuery struct {
	// PageSize is the number of queues per request; 0 selects 50 and
	// Langfuse accepts at most 100.
	PageSize int
}

// AnnotationQueueItemSpec adds an object to an annotation queue.
type AnnotationQueueItemSpec struct {
	// QueueID, ObjectID, and ObjectType are required.
	QueueID    string
	ObjectID   string
	ObjectType AnnotationObjectType
	// Status is empty (PENDING) or an AnnotationStatus.
	Status AnnotationStatus
}

// AnnotationQueueItem is one object in an annotation queue.
type AnnotationQueueItem struct {
	ID         string
	QueueID    string
	ObjectID   string
	ObjectType AnnotationObjectType
	Status     AnnotationStatus
	// CompletedAt is when the item was last completed, zero when Langfuse
	// has no time; returning an item to PENDING keeps it, so Status tells
	// whether the item is complete.
	CompletedAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// AnnotationQueueItemQuery selects the items of one queue for
// [Client.AnnotationQueueItems].
type AnnotationQueueItemQuery struct {
	// QueueID is required.
	QueueID string
	// Status, when set, selects only items in that state.
	Status AnnotationStatus
	// PageSize is the number of items per request; 0 selects 50 and Langfuse
	// accepts at most 100.
	PageSize int
}

// CreateAnnotationQueue creates an annotation queue. A name already in use
// or an unknown score config fails with status 400. The write is sent once;
// a failure after sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) CreateAnnotationQueue(ctx context.Context, spec AnnotationQueueSpec) (AnnotationQueue, error) {
	invalid := requireField("annotation queue name", spec.Name)
	if invalid == nil && !utf8.ValidString(spec.Description) {
		invalid = errors.New("langfuse: annotation queue description is not valid UTF-8")
	}
	if invalid == nil && len(spec.ScoreConfigIDs) == 0 {
		invalid = errors.New("langfuse: annotation queue requires a score config ID")
	}
	if invalid == nil {
		invalid = requireIDs("annotation queue score config ID", spec.ScoreConfigIDs)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return AnnotationQueue{}, err
	}
	body := map[string]any{"name": spec.Name, "scoreConfigIds": spec.ScoreConfigIDs}
	if spec.Description != "" {
		body["description"] = spec.Description
	}
	payload, err := marshalBody(body, maxRESTBodyBytes, "annotation queue")
	if err != nil {
		return AnnotationQueue{}, err
	}
	result, err := runREST(ctx, c, nil, func(ctx context.Context) (transport.AnnotationQueue, error) {
		return c.restTransport.CreateAnnotationQueue(ctx, payload, spec.Name, spec.Description, spec.ScoreConfigIDs)
	})
	return annotationQueueFromWire(result), err
}

// GetAnnotationQueue reads one annotation queue by ID. A missing queue wraps
// [ErrAnnotationQueueNotFound].
func (c *Client) GetAnnotationQueue(ctx context.Context, id string) (AnnotationQueue, error) {
	if err := c.restReady(ctx, requireField("annotation queue ID", id)); err != nil {
		return AnnotationQueue{}, err
	}
	result, err := runREST(ctx, c, ErrAnnotationQueueNotFound, func(ctx context.Context) (transport.AnnotationQueue, error) {
		return c.restTransport.GetAnnotationQueue(ctx, id)
	})
	return annotationQueueFromWire(result), err
}

// AnnotationQueues lazily iterates over the project's annotation queues,
// newest first, with the paging and failure rules of [Client.DatasetItems].
func (c *Client) AnnotationQueues(ctx context.Context, query AnnotationQueueQuery) iter.Seq2[AnnotationQueue, error] {
	pageSize, invalid := restPageSize(query.PageSize)
	return numberedPages(ctx, c, invalid, nil, func(ctx context.Context, page int) ([]AnnotationQueue, int, error) {
		wire, err := c.restTransport.ListAnnotationQueues(ctx, page, pageSize)
		queues := make([]AnnotationQueue, len(wire.Data))
		for index, queue := range wire.Data {
			queues[index] = annotationQueueFromWire(queue)
		}
		return queues, wire.Meta.Pages(), err
	})
}

// CreateAnnotationQueueItem adds an object to a queue and returns the stored
// item. Langfuse neither checks that the object exists nor deduplicates
// items, so adding an object twice queues it twice; list the queue first
// when that matters. A missing queue wraps [ErrAnnotationQueueNotFound]. The
// write is sent once; a failure after sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) CreateAnnotationQueueItem(
	ctx context.Context, spec AnnotationQueueItemSpec,
) (AnnotationQueueItem, error) {
	invalid := requireField("annotation queue ID", spec.QueueID)
	if invalid == nil {
		invalid = requireField("annotation queue item object ID", spec.ObjectID)
	}
	if invalid == nil {
		invalid = validateAnnotationObjectType(spec.ObjectType)
	}
	if invalid == nil && spec.Status != "" {
		invalid = validateAnnotationStatus(spec.Status)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return AnnotationQueueItem{}, err
	}
	body := map[string]any{"objectId": spec.ObjectID, "objectType": string(spec.ObjectType)}
	if spec.Status != "" {
		body["status"] = string(spec.Status)
	}
	payload, err := marshalBody(body, maxRESTBodyBytes, "annotation queue item")
	if err != nil {
		return AnnotationQueueItem{}, err
	}
	want := transport.AnnotationQueueItem{
		ObjectID: spec.ObjectID, ObjectType: string(spec.ObjectType), Status: string(spec.Status),
	}
	result, err := runREST(ctx, c, ErrAnnotationQueueNotFound, func(ctx context.Context) (transport.AnnotationQueueItem, error) {
		return c.restTransport.CreateAnnotationQueueItem(ctx, spec.QueueID, payload, want)
	})
	return annotationQueueItemFromWire(result), err
}

// GetAnnotationQueueItem reads one queue item. A missing item or queue wraps
// [ErrAnnotationQueueItemNotFound].
func (c *Client) GetAnnotationQueueItem(ctx context.Context, queueID, itemID string) (AnnotationQueueItem, error) {
	if err := c.restReady(ctx, requireQueueItem(queueID, itemID)); err != nil {
		return AnnotationQueueItem{}, err
	}
	result, err := runREST(ctx, c, ErrAnnotationQueueItemNotFound,
		func(ctx context.Context) (transport.AnnotationQueueItem, error) {
			return c.restTransport.GetAnnotationQueueItem(ctx, queueID, itemID)
		})
	return annotationQueueItemFromWire(result), err
}

// UpdateAnnotationQueueItem sets the status of one queue item and returns
// the stored item; completing it stamps CompletedAt. A missing item or queue
// wraps
// [ErrAnnotationQueueItemNotFound], and a failure after sending wraps
// [ErrWriteOutcomeUnknown].
func (c *Client) UpdateAnnotationQueueItem(
	ctx context.Context, queueID, itemID string, status AnnotationStatus,
) (AnnotationQueueItem, error) {
	invalid := requireQueueItem(queueID, itemID)
	if invalid == nil {
		invalid = validateAnnotationStatus(status)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return AnnotationQueueItem{}, err
	}
	payload, err := marshalBody(map[string]any{"status": string(status)}, maxRESTBodyBytes, "annotation queue item")
	if err != nil {
		return AnnotationQueueItem{}, err
	}
	result, err := runREST(ctx, c, ErrAnnotationQueueItemNotFound,
		func(ctx context.Context) (transport.AnnotationQueueItem, error) {
			return c.restTransport.UpdateAnnotationQueueItem(ctx, queueID, itemID, payload, string(status))
		})
	return annotationQueueItemFromWire(result), err
}

// DeleteAnnotationQueueItem removes one item from its queue; scores already
// recorded for the object remain. A missing item or queue wraps
// [ErrAnnotationQueueItemNotFound], and a failure after sending wraps
// [ErrWriteOutcomeUnknown].
func (c *Client) DeleteAnnotationQueueItem(ctx context.Context, queueID, itemID string) error {
	if err := c.restReady(ctx, requireQueueItem(queueID, itemID)); err != nil {
		return err
	}
	_, err := runREST(ctx, c, ErrAnnotationQueueItemNotFound, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.restTransport.DeleteAnnotationQueueItem(ctx, queueID, itemID)
	})
	return err
}

// AnnotationQueueItems lazily iterates over the items of one queue, newest
// first, with the paging and failure rules of [Client.DatasetItems]. A
// missing queue wraps [ErrAnnotationQueueNotFound].
func (c *Client) AnnotationQueueItems(
	ctx context.Context, query AnnotationQueueItemQuery,
) iter.Seq2[AnnotationQueueItem, error] {
	pageSize, invalid := restPageSize(query.PageSize)
	if invalid == nil {
		invalid = requireField("annotation queue ID", query.QueueID)
	}
	if invalid == nil && query.Status != "" {
		invalid = validateAnnotationStatus(query.Status)
	}
	return numberedPages(ctx, c, invalid, ErrAnnotationQueueNotFound,
		func(ctx context.Context, page int) ([]AnnotationQueueItem, int, error) {
			wire, err := c.restTransport.ListAnnotationQueueItems(ctx, query.QueueID, string(query.Status), page, pageSize)
			items := make([]AnnotationQueueItem, len(wire.Data))
			for index, item := range wire.Data {
				items[index] = annotationQueueItemFromWire(item)
			}
			return items, wire.Meta.Pages(), err
		})
}

// AssignAnnotationQueue assigns a queue to a Langfuse user, who must be a
// member of the project; assigning twice is harmless. A missing queue or a
// user outside the project fails with status 404, and a failure after
// sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) AssignAnnotationQueue(ctx context.Context, queueID, userID string) error {
	return c.annotationQueueAssignment(ctx, queueID, userID, true)
}

// UnassignAnnotationQueue removes a user's queue assignment; removing a
// missing assignment is harmless. Errors follow
// [Client.AssignAnnotationQueue].
func (c *Client) UnassignAnnotationQueue(ctx context.Context, queueID, userID string) error {
	return c.annotationQueueAssignment(ctx, queueID, userID, false)
}

func (c *Client) annotationQueueAssignment(ctx context.Context, queueID, userID string, assign bool) error {
	invalid := requireField("annotation queue ID", queueID)
	if invalid == nil {
		invalid = requireField("user ID", userID)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return err
	}
	payload, err := marshalBody(map[string]any{"userId": userID}, maxRESTBodyBytes, "annotation queue assignment")
	if err != nil {
		return err
	}
	_, err = runREST(ctx, c, nil, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.restTransport.AssignAnnotationQueue(ctx, queueID, userID, payload, assign)
	})
	return err
}

func requireQueueItem(queueID, itemID string) error {
	if err := requireField("annotation queue ID", queueID); err != nil {
		return err
	}
	return requireField("annotation queue item ID", itemID)
}

func requireIDs(field string, ids []string) error {
	for _, id := range ids {
		if err := requireField(field, id); err != nil {
			return err
		}
		if !utf8.ValidString(id) {
			return errors.New("langfuse: " + field + " is not valid UTF-8")
		}
	}
	return nil
}

func validateAnnotationObjectType(objectType AnnotationObjectType) error {
	switch objectType {
	case AnnotationObjectTrace, AnnotationObjectObservation, AnnotationObjectSession:
		return nil
	default:
		return errors.New("langfuse: annotation object type must be TRACE, OBSERVATION, or SESSION")
	}
}

func validateAnnotationStatus(status AnnotationStatus) error {
	switch status {
	case AnnotationPending, AnnotationCompleted:
		return nil
	default:
		return errors.New("langfuse: annotation status must be PENDING or COMPLETED")
	}
}

func annotationQueueFromWire(wire transport.AnnotationQueue) AnnotationQueue {
	queue := AnnotationQueue{
		ID: wire.ID, Description: wire.Description, ScoreConfigIDs: wire.ScoreConfigIDs,
		CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt,
	}
	if wire.Name != nil {
		queue.Name = *wire.Name
	}
	return queue
}

func annotationQueueItemFromWire(wire transport.AnnotationQueueItem) AnnotationQueueItem {
	item := AnnotationQueueItem{
		ID: wire.ID, QueueID: wire.QueueID, ObjectID: wire.ObjectID,
		ObjectType: AnnotationObjectType(wire.ObjectType), Status: AnnotationStatus(wire.Status),
		CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt,
	}
	if wire.CompletedAt != nil {
		item.CompletedAt = *wire.CompletedAt
	}
	return item
}
