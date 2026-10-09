package langfuse

import (
	"context"
	"errors"
	"iter"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrCommentNotFound reports that the comment does not exist.
var ErrCommentNotFound = errors.New("langfuse: comment not found")

// ErrCommentObjectNotFound reports that the object a new comment refers to
// does not exist, or is not yet visible after ingestion.
var ErrCommentObjectNotFound = errors.New("langfuse: comment object not found")

const maxCommentCharacters = 5000

// CommentObjectType is the kind of object a comment is attached to.
type CommentObjectType string

const (
	CommentObjectTrace       CommentObjectType = "TRACE"
	CommentObjectObservation CommentObjectType = "OBSERVATION"
	CommentObjectSession     CommentObjectType = "SESSION"
	CommentObjectPrompt      CommentObjectType = "PROMPT"
)

// CommentSpec creates a comment.
type CommentSpec struct {
	// ObjectType and ObjectID are required and must name an existing object;
	// a prompt is named by its version's ID.
	ObjectType CommentObjectType
	ObjectID   string
	// Content is required Markdown of at most 5000 UTF-16 code units after
	// Langfuse trims surrounding whitespace. It is explicit caller content
	// and is not masked.
	Content string
	// AuthorUserID, when set, must name a member of the project's
	// organization.
	AuthorUserID string
	// ObjectStartTime, when set, is the start time of the commented
	// observation; it only speeds up Langfuse's lookup of the object.
	ObjectStartTime time.Time
}

// Comment is a comment as stored by Langfuse.
type Comment struct {
	ID           string
	ObjectType   CommentObjectType
	ObjectID     string
	Content      string
	AuthorUserID string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CommentQuery selects comments for [Client.Comments].
type CommentQuery struct {
	// ObjectType, when set, selects comments on that kind of object; ObjectID
	// requires it.
	ObjectType   CommentObjectType
	ObjectID     string
	AuthorUserID string
	// PageSize is the number of comments per request; 0 selects 50 and
	// Langfuse accepts at most 100.
	PageSize int
}

// CreateComment adds a comment and returns its ID. A missing object wraps
// [ErrCommentObjectNotFound]; Langfuse looks traces, observations, and
// sessions up in its analytics store, so a trace exported moments ago can be
// missing. The write is sent once; a failure after sending wraps
// [ErrWriteOutcomeUnknown], and repeating it can add a second comment.
func (c *Client) CreateComment(ctx context.Context, spec CommentSpec) (string, error) {
	invalid := validateCommentObjectType(spec.ObjectType)
	if invalid == nil {
		invalid = requireField("comment object ID", spec.ObjectID)
	}
	if invalid == nil && !spec.ObjectStartTime.IsZero() && !validDatasetInstant(spec.ObjectStartTime) {
		invalid = errors.New("langfuse: comment object start time is outside the RFC 3339 year range")
	}
	if content := trimJS(spec.Content); invalid == nil &&
		(content == "" || !utf8.ValidString(content) || lengthJS(content) > maxCommentCharacters) {
		invalid = errors.New("langfuse: comment content must be 1 to 5000 characters of UTF-8")
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return "", err
	}
	// Langfuse requires projectId but uses the API key's project.
	body := map[string]any{
		"projectId": "", "objectType": string(spec.ObjectType), "objectId": spec.ObjectID, "content": spec.Content,
	}
	if spec.AuthorUserID != "" {
		body["authorUserId"] = spec.AuthorUserID
	}
	if !spec.ObjectStartTime.IsZero() {
		body["objectStartTime"] = formatDatasetInstant(spec.ObjectStartTime)
	}
	payload, err := marshalBody(body, maxRESTBodyBytes, "comment")
	if err != nil {
		return "", err
	}
	return runREST(ctx, c, ErrCommentObjectNotFound, func(ctx context.Context) (string, error) {
		return c.restTransport.CreateComment(ctx, payload)
	})
}

// GetComment reads one comment by ID. A missing comment wraps
// [ErrCommentNotFound].
func (c *Client) GetComment(ctx context.Context, id string) (Comment, error) {
	if err := c.restReady(ctx, requireField("comment ID", id)); err != nil {
		return Comment{}, err
	}
	result, err := runREST(ctx, c, ErrCommentNotFound, func(ctx context.Context) (transport.Comment, error) {
		return c.restTransport.GetComment(ctx, id)
	})
	return commentFromWire(result), err
}

// Comments lazily iterates over the project's comments with the paging and
// failure rules of [Client.DatasetItems]. Langfuse returns comments in no
// defined order, so sort them, for example by CreatedAt, after collecting
// them; pages can repeat or skip comments added during the loop.
func (c *Client) Comments(ctx context.Context, query CommentQuery) iter.Seq2[Comment, error] {
	pageSize, invalid := restPageSize(query.PageSize)
	if invalid == nil && query.ObjectType != "" {
		invalid = validateCommentObjectType(query.ObjectType)
	}
	if invalid == nil && query.ObjectID != "" && query.ObjectType == "" {
		invalid = errors.New("langfuse: comment query object ID requires an object type")
	}
	values := url.Values{}
	for key, value := range map[string]string{
		"objectType": string(query.ObjectType), "objectId": query.ObjectID, "authorUserId": query.AuthorUserID,
	} {
		if value != "" {
			values.Set(key, value)
		}
	}
	values.Set("limit", strconv.Itoa(pageSize))
	return numberedPages(ctx, c, invalid, nil, func(ctx context.Context, page int) ([]Comment, int, error) {
		request := cloneValues(values)
		request.Set("page", strconv.Itoa(page))
		wire, err := c.restTransport.ListComments(ctx, request)
		comments := make([]Comment, len(wire.Data))
		for index, comment := range wire.Data {
			comments[index] = commentFromWire(comment)
		}
		return comments, wire.Meta.Pages(), err
	})
}

func validateCommentObjectType(objectType CommentObjectType) error {
	switch objectType {
	case CommentObjectTrace, CommentObjectObservation, CommentObjectSession, CommentObjectPrompt:
		return nil
	default:
		return errors.New("langfuse: comment object type must be TRACE, OBSERVATION, SESSION, or PROMPT")
	}
}

func commentFromWire(wire transport.Comment) Comment {
	return Comment{
		ID: wire.ID, ObjectType: CommentObjectType(wire.ObjectType), ObjectID: wire.ObjectID,
		Content: wire.Content, AuthorUserID: wire.AuthorUserID, CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt,
	}
}
