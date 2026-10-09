package langfuse

import (
	"context"
	"errors"
	"iter"
	"math"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrScoreConfigNotFound reports that the score config does not exist.
var ErrScoreConfigNotFound = errors.New("langfuse: score config not found")

const (
	maxScoreConfigNameCharacters = 35
	maxRESTBodyBytes             = 1 << 20
	defaultRESTPageSize          = 50
	maxRESTPageSize              = 100
)

var scoreConfigName = regexp.MustCompile(`^[\p{L}\p{N}_ .()-]+$`)

// ScoreCategory is one category of a categorical or boolean score config.
// Scores use the label as their value.
type ScoreCategory struct {
	Label string
	Value float64
}

// ScoreConfigSpec creates a score config. Langfuse does not key configs by
// name, so creating the same spec twice stores two configs.
type ScoreConfigSpec struct {
	// Name is required: at most 35 UTF-16 code units of letters, digits,
	// underscores, spaces, periods, parentheses, and hyphens. Scores that use
	// the config are stored under its name.
	Name string
	// DataType is ScoreTypeNumeric, ScoreTypeBoolean, ScoreTypeCategorical,
	// or ScoreTypeText.
	DataType ScoreDataType
	// Categories are required for a CATEGORICAL config, which needs at least
	// one, and not allowed for any other; labels and values must be unique.
	// BOOLEAN configs get "True" (1) and "False" (0).
	Categories []ScoreCategory
	// MinValue and MaxValue bound a NUMERIC config inclusively; nil is
	// unbounded, and MaxValue must exceed MinValue.
	MinValue *float64
	MaxValue *float64
	// Description is shown in the Langfuse UI. It is not masked.
	Description string
}

// ScoreConfig is a score config as stored by Langfuse.
type ScoreConfig struct {
	ID          string
	Name        string
	DataType    ScoreDataType
	Archived    bool
	Categories  []ScoreCategory
	MinValue    *float64
	MaxValue    *float64
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ScoreConfigUpdate changes a stored score config; nil fields keep their
// stored values, and at least one must be set. Langfuse cannot change the
// data type or remove MinValue or MaxValue, and validates the result as a
// whole.
type ScoreConfigUpdate struct {
	// Archived hides a config from new annotations; Langfuse then drops new
	// scores that reference it.
	Archived *bool
	Name     *string
	// Description, set to "", empties the description.
	Description *string
	// Categories replace the stored ones; they fit a CATEGORICAL config, or
	// a BOOLEAN one as exactly "True" (1) then "False" (0). The SDK rejects
	// an empty non-nil slice.
	Categories []ScoreCategory
	MinValue   *float64
	MaxValue   *float64
}

// ScoreConfigQuery configures [Client.ScoreConfigs].
type ScoreConfigQuery struct {
	// PageSize is the number of configs per request; 0 selects 50 and
	// Langfuse accepts at most 100.
	PageSize int
}

// CreateScoreConfig creates a score config. The write is sent once; a
// failure after sending wraps [ErrWriteOutcomeUnknown], and repeating it can
// create a second config with the same name.
func (c *Client) CreateScoreConfig(ctx context.Context, spec ScoreConfigSpec) (ScoreConfig, error) {
	if err := c.restReady(ctx, validateScoreConfigSpec(spec)); err != nil {
		return ScoreConfig{}, err
	}
	body := map[string]any{"name": spec.Name, "dataType": string(spec.DataType)}
	if spec.Categories != nil {
		body["categories"] = wireCategories(spec.Categories)
	}
	if spec.MinValue != nil {
		body["minValue"] = *spec.MinValue
	}
	if spec.MaxValue != nil {
		body["maxValue"] = *spec.MaxValue
	}
	if spec.Description != "" {
		body["description"] = spec.Description
	}
	payload, err := marshalBody(body, maxRESTBodyBytes, "score config")
	if err != nil {
		return ScoreConfig{}, err
	}
	change := transport.ScoreConfigChange{
		Name: spec.Name, DataType: string(spec.DataType), Archived: new(bool), Categories: wireCategories(spec.Categories),
		MinValue: spec.MinValue, MaxValue: spec.MaxValue,
	}
	if spec.Description != "" {
		change.Description = &spec.Description
	}
	result, err := runREST(ctx, c, nil, func(ctx context.Context) (transport.ScoreConfig, error) {
		return c.restTransport.CreateScoreConfig(ctx, payload, change)
	})
	return scoreConfigFromWire(result), err
}

// GetScoreConfig reads one score config by ID. A missing config wraps
// [ErrScoreConfigNotFound].
func (c *Client) GetScoreConfig(ctx context.Context, id string) (ScoreConfig, error) {
	if err := c.restReady(ctx, requireField("score config ID", id)); err != nil {
		return ScoreConfig{}, err
	}
	result, err := runREST(ctx, c, ErrScoreConfigNotFound, func(ctx context.Context) (transport.ScoreConfig, error) {
		return c.restTransport.GetScoreConfig(ctx, id)
	})
	return scoreConfigFromWire(result), err
}

// UpdateScoreConfig changes one score config and returns the stored config.
// A missing config wraps [ErrScoreConfigNotFound], and a failure after
// sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) UpdateScoreConfig(ctx context.Context, id string, update ScoreConfigUpdate) (ScoreConfig, error) {
	invalid := requireField("score config ID", id)
	if invalid == nil {
		invalid = validateScoreConfigUpdate(update)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return ScoreConfig{}, err
	}
	body := map[string]any{}
	if update.Archived != nil {
		body["isArchived"] = *update.Archived
	}
	if update.Name != nil {
		body["name"] = *update.Name
	}
	if update.Description != nil {
		body["description"] = *update.Description
	}
	if update.Categories != nil {
		body["categories"] = wireCategories(update.Categories)
	}
	if update.MinValue != nil {
		body["minValue"] = *update.MinValue
	}
	if update.MaxValue != nil {
		body["maxValue"] = *update.MaxValue
	}
	payload, err := marshalBody(body, maxRESTBodyBytes, "score config")
	if err != nil {
		return ScoreConfig{}, err
	}
	change := transport.ScoreConfigChange{
		Archived: update.Archived, Description: update.Description, Categories: wireCategories(update.Categories),
		MinValue: update.MinValue, MaxValue: update.MaxValue,
	}
	if update.Name != nil {
		change.Name = *update.Name
	}
	result, err := runREST(ctx, c, ErrScoreConfigNotFound, func(ctx context.Context) (transport.ScoreConfig, error) {
		return c.restTransport.UpdateScoreConfig(ctx, id, payload, change)
	})
	return scoreConfigFromWire(result), err
}

// ScoreConfigs lazily iterates over the project's score configs, newest
// first and including archived ones, with the paging and failure rules of
// [Client.DatasetItems].
func (c *Client) ScoreConfigs(ctx context.Context, query ScoreConfigQuery) iter.Seq2[ScoreConfig, error] {
	pageSize, invalid := restPageSize(query.PageSize)
	return numberedPages(ctx, c, invalid, nil, func(ctx context.Context, page int) ([]ScoreConfig, int, error) {
		wire, err := c.restTransport.ListScoreConfigs(ctx, page, pageSize)
		configs := make([]ScoreConfig, len(wire.Data))
		for index, config := range wire.Data {
			configs[index] = scoreConfigFromWire(config)
		}
		return configs, wire.Meta.Pages(), err
	})
}

func validateScoreConfigSpec(spec ScoreConfigSpec) error {
	if err := validateScoreConfigName(spec.Name); err != nil {
		return err
	}
	switch spec.DataType {
	case ScoreTypeCategorical:
		if len(spec.Categories) == 0 {
			return errors.New("langfuse: a CATEGORICAL score config requires categories")
		}
	case ScoreTypeNumeric, ScoreTypeBoolean, ScoreTypeText:
		if spec.Categories != nil {
			return errors.New("langfuse: only a CATEGORICAL score config takes categories")
		}
	default:
		return errors.New("langfuse: score config data type must be NUMERIC, BOOLEAN, CATEGORICAL, or TEXT")
	}
	if spec.DataType != ScoreTypeNumeric && (spec.MinValue != nil || spec.MaxValue != nil) {
		return errors.New("langfuse: only a NUMERIC score config takes a minimum or maximum value")
	}
	return validateScoreConfigFields(spec.Categories, spec.MinValue, spec.MaxValue, spec.Description)
}

func validateScoreConfigUpdate(update ScoreConfigUpdate) error {
	if update.Archived == nil && update.Name == nil && update.Description == nil && update.Categories == nil &&
		update.MinValue == nil && update.MaxValue == nil {
		return errors.New("langfuse: score config update changes nothing")
	}
	if update.Name != nil {
		if err := validateScoreConfigName(*update.Name); err != nil {
			return err
		}
	}
	if update.Categories != nil && len(update.Categories) == 0 {
		return errors.New("langfuse: score config categories must not be empty")
	}
	description := ""
	if update.Description != nil {
		description = *update.Description
	}
	return validateScoreConfigFields(update.Categories, update.MinValue, update.MaxValue, description)
}

func validateScoreConfigName(name string) error {
	if name == "" {
		return errors.New("langfuse: score config name is required")
	}
	if !scoreConfigName.MatchString(name) || lengthJS(name) > maxScoreConfigNameCharacters {
		return errors.New("langfuse: score config name must be at most 35 letters, digits, underscores, " +
			"spaces, periods, parentheses, or hyphens")
	}
	return nil
}

func validateScoreConfigFields(categories []ScoreCategory, minValue, maxValue *float64, description string) error {
	labels := map[string]bool{}
	values := map[float64]bool{}
	for _, category := range categories {
		if category.Label == "" || !utf8.ValidString(category.Label) {
			return errors.New("langfuse: score config category labels must be non-empty UTF-8")
		}
		if math.IsNaN(category.Value) || math.IsInf(category.Value, 0) {
			return errors.New("langfuse: score config category values must be finite")
		}
		if labels[category.Label] || values[category.Value] {
			return errors.New("langfuse: score config category labels and values must be unique")
		}
		labels[category.Label], values[category.Value] = true, true
	}
	for _, bound := range []*float64{minValue, maxValue} {
		if bound != nil && (math.IsNaN(*bound) || math.IsInf(*bound, 0)) {
			return errors.New("langfuse: score config minimum and maximum values must be finite")
		}
	}
	if minValue != nil && maxValue != nil && *minValue >= *maxValue {
		return errors.New("langfuse: score config maximum value must exceed its minimum value")
	}
	if !utf8.ValidString(description) {
		return errors.New("langfuse: score config description is not valid UTF-8")
	}
	return nil
}

// wireCategories keeps nil apart from empty: nil categories are not sent.
func wireCategories(categories []ScoreCategory) []transport.ScoreCategory {
	if categories == nil {
		return nil
	}
	wire := make([]transport.ScoreCategory, len(categories))
	for index, category := range categories {
		wire[index] = transport.ScoreCategory{Label: category.Label, Value: &category.Value}
	}
	return wire
}

func scoreConfigFromWire(wire transport.ScoreConfig) ScoreConfig {
	config := ScoreConfig{
		ID: wire.ID, Name: wire.Name, DataType: ScoreDataType(wire.DataType),
		MinValue: wire.MinValue, MaxValue: wire.MaxValue, Description: wire.Description,
		CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt,
	}
	if wire.IsArchived != nil {
		config.Archived = *wire.IsArchived
	}
	if wire.Categories != nil {
		config.Categories = make([]ScoreCategory, len(wire.Categories))
		for index, category := range wire.Categories {
			config.Categories[index] = ScoreCategory{Label: category.Label, Value: *category.Value}
		}
	}
	return config
}

// restPageSize applies the default and bounds of a REST listing.
func restPageSize(pageSize int) (int, error) {
	if pageSize < 0 || pageSize > maxRESTPageSize {
		return 0, errors.New("langfuse: page size must be 0 to 100")
	}
	if pageSize == 0 {
		return defaultRESTPageSize, nil
	}
	return pageSize, nil
}
