package langfuse

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var _ func(context.Context, Config, ...sdktrace.BatchSpanProcessorOption) (sdktrace.SpanProcessor, error) = NewSpanProcessor

func TestTopLevelAPIStaysSmall(t *testing.T) {
	files, err := parser.ParseDir(token.NewFileSet(), ".", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	var exported []string
	for _, file := range files["langfuse"].Files {
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.IsExported() {
					exported = append(exported, decl.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							exported = append(exported, spec.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								exported = append(exported, name.Name)
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(exported)
	want := []string{"Config", "NewSpanProcessor"}
	if !equalStrings(exported, want) {
		t.Fatalf("top-level API = %v, want %v", exported, want)
	}
}

func TestConfigSurfaceStaysSmall(t *testing.T) {
	configType := reflect.TypeFor[Config]()
	want := []string{"BaseURL", "PublicKey", "SecretKey"}
	if configType.NumField() != len(want) {
		t.Fatalf("Config has %d fields, want %d", configType.NumField(), len(want))
	}
	for index, name := range want {
		field := configType.Field(index)
		if field.Name != name || field.Type != reflect.TypeFor[string]() || !field.IsExported() {
			t.Fatalf("Config field %d = %s %s, want %s string", index, field.Name, field.Type, name)
		}
	}
	if configType.NumMethod() != 0 || reflect.PointerTo(configType).NumMethod() != 0 {
		t.Fatal("Config unexpectedly has exported methods")
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
