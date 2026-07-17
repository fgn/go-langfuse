package langfuse_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// This golden covers every exported declaration, signature, field type, and
// constant value in the root package. The compile-time checks in
// api_surface_test.go provide friendlier errors for intended call shapes.
func TestCompletePublicAPIGolden(t *testing.T) {
	t.Parallel()

	got := renderPublicAPI(t)
	if got != wantPublicAPI {
		t.Fatalf("public API changed; review the v0.1 contract and update the golden intentionally:\n%s", got)
	}
}

func renderPublicAPI(t *testing.T) string {
	t.Helper()

	set := token.NewFileSet()
	packages, err := parser.ParseDir(set, ".", func(info os.FileInfo) bool {
		return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse root package: %v", err)
	}
	pkg := packages["langfuse"]
	if pkg == nil {
		t.Fatal("parsed root package does not contain package langfuse")
	}

	entries := make([]string, 0)
	for _, file := range pkg.Files {
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if !declaration.Name.IsExported() {
					continue
				}
				clone := *declaration
				clone.Doc = nil
				clone.Body = nil
				entries = append(entries, formatAPINode(t, set, &clone))
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					if entry := exportedSpec(t, set, declaration.Tok, spec); entry != "" {
						entries = append(entries, entry)
					}
				}
			}
		}
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n\n") + "\n"
}

func exportedSpec(t *testing.T, set *token.FileSet, kind token.Token, spec ast.Spec) string {
	t.Helper()

	switch spec := spec.(type) {
	case *ast.TypeSpec:
		if !spec.Name.IsExported() {
			return ""
		}
		if structure, ok := spec.Type.(*ast.StructType); ok {
			var output strings.Builder
			output.WriteString("type ")
			output.WriteString(spec.Name.Name)
			output.WriteString(" struct {\n")
			for _, field := range structure.Fields.List {
				if !exportedField(field) {
					continue
				}
				output.WriteByte('\t')
				exportedNames := 0
				for _, name := range field.Names {
					if !name.IsExported() {
						continue
					}
					if exportedNames != 0 {
						output.WriteString(", ")
					}
					output.WriteString(name.Name)
					exportedNames++
				}
				if exportedNames != 0 {
					output.WriteByte(' ')
				}
				output.WriteString(formatAPINode(t, set, field.Type))
				if field.Tag != nil {
					output.WriteByte(' ')
					output.WriteString(field.Tag.Value)
				}
				output.WriteByte('\n')
			}
			output.WriteByte('}')
			return output.String()
		}
		clone := *spec
		clone.Doc = nil
		clone.Comment = nil
		return formatAPINode(t, set, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&clone}})
	case *ast.ValueSpec:
		entries := make([]string, 0, len(spec.Names))
		for index, name := range spec.Names {
			if !name.IsExported() {
				continue
			}
			clone := *spec
			clone.Doc = nil
			clone.Comment = nil
			clone.Names = []*ast.Ident{name}
			if len(spec.Values) == len(spec.Names) {
				clone.Values = []ast.Expr{spec.Values[index]}
			}
			entries = append(entries, formatAPINode(t, set, &ast.GenDecl{Tok: kind, Specs: []ast.Spec{&clone}}))
		}
		return strings.Join(entries, "\n\n")
	default:
		return ""
	}
}

func exportedField(field *ast.Field) bool {
	for _, name := range field.Names {
		if name.IsExported() {
			return true
		}
	}
	if len(field.Names) != 0 {
		return false
	}
	switch fieldType := field.Type.(type) {
	case *ast.Ident:
		return fieldType.IsExported()
	case *ast.StarExpr:
		return exportedEmbeddedType(fieldType.X)
	default:
		return exportedEmbeddedType(fieldType)
	}
}

func exportedEmbeddedType(expression ast.Expr) bool {
	switch expression := expression.(type) {
	case *ast.Ident:
		return expression.IsExported()
	case *ast.SelectorExpr:
		return expression.Sel.IsExported()
	case *ast.StarExpr:
		return exportedEmbeddedType(expression.X)
	case *ast.IndexExpr:
		return exportedEmbeddedType(expression.X)
	case *ast.IndexListExpr:
		return exportedEmbeddedType(expression.X)
	default:
		return false
	}
}

func formatAPINode(t *testing.T, set *token.FileSet, node ast.Node) string {
	t.Helper()

	var output bytes.Buffer
	if err := format.Node(&output, set, node); err != nil {
		t.Fatalf("format public API declaration: %v", err)
	}
	return output.String()
}

const wantPublicAPI = `const LevelDebug Level = "DEBUG"

const LevelDefault Level = "DEFAULT"

const LevelError Level = "ERROR"

const LevelWarning Level = "WARNING"

const TypeAgent ObservationType = "agent"

const TypeChain ObservationType = "chain"

const TypeEmbedding ObservationType = "embedding"

const TypeEvaluator ObservationType = "evaluator"

const TypeEvent ObservationType = "event"

const TypeGeneration ObservationType = "generation"

const TypeGuardrail ObservationType = "guardrail"

const TypeRetriever ObservationType = "retriever"

const TypeSpan ObservationType = "span"

const TypeTool ObservationType = "tool"

func (c *Client) Event(ctx context.Context, name string, values ObservationAttributes)

func (c *Client) Flush(ctx context.Context) error

func (c *Client) Shutdown(ctx context.Context) error

func (c *Client) StartObservation(
	ctx context.Context,
	name string,
	observationType ObservationType,
	values ObservationAttributes,
) (context.Context, *Observation)

func (c *Client) WithTraceAttributes(ctx context.Context, values TraceAttributes) context.Context

func (o *Observation) End()

func (o *Observation) ID() string

func (o *Observation) RecordError(err error)

func (o *Observation) TraceID() string

func (o *Observation) Update(values ObservationAttributes)

func ConfigFromEnv() Config

func New(ctx context.Context, cfg Config) (*Client, error)

type Client struct {
}

type Config struct {
	BaseURL string
	PublicKey string
	SecretKey string
	Environment string
	Release string
	ServiceName string
	TracerProvider *sdktrace.TracerProvider
	MaxQueueSize int
	BlockOnQueueFull bool
	Disabled bool
	DisableContentCapture bool
	Mask func(value any) any
}

type Level string

type Observation struct {
}

type ObservationAttributes struct {
	Input any
	Output any
	Metadata map[string]any
	Level Level
	StatusMessage string
	Version string
	Model string
	ModelParameters map[string]any
	Usage *Usage
	CostDetails map[string]float64
	Prompt *PromptRef
	CompletionStartTime time.Time
	StartTime time.Time
}

type ObservationType string

type PromptRef struct {
	Name string
	Version int
}

type TraceAttributes struct {
	Name string
	UserID string
	SessionID string
	Tags []string
	Metadata map[string]any
	Version string
}

type Usage struct {
	InputTokens int64
	OutputTokens int64
	CacheReadInputTokens int64
	CacheCreationInputTokens int64
	ReasoningOutputTokens int64
	Details map[string]int64
}
`
