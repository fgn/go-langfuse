package langfuse_test

import (
	"os"
	"strings"
	"testing"
)

func TestREADMEExamplesMatchCompiledExamples(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	tests := []struct {
		name string
		path string
	}{
		{name: "quickstart", path: "examples/existingotel/telemetry.go"},
		{name: "manualspan", path: "examples/manualspan/chat.go"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			example, err := os.ReadFile(test.path)
			if err != nil {
				t.Fatalf("read compiled example: %v", err)
			}

			start := "<!-- " + test.name + ":start -->\n```go\n"
			end := "\n```\n<!-- " + test.name + ":end -->"
			_, after, ok := strings.Cut(string(readme), start)
			if !ok {
				t.Fatalf("README is missing the %s start marker", test.name)
			}
			documented, _, ok := strings.Cut(after, end)
			if !ok {
				t.Fatalf("README is missing the %s end marker", test.name)
			}
			if strings.TrimSpace(documented) != strings.TrimSpace(string(example)) {
				t.Fatalf("README %s differs from compiled example", test.name)
			}
		})
	}
}
