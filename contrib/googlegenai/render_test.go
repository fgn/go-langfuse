package langfusegenai

import (
	"encoding/json"
	"testing"
)

func renderJSON(t *testing.T, value any) string {
	t.Helper()
	rendered, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered)
}
