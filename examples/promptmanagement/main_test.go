package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/api"
)

func TestPromptManagementWorkflow(t *testing.T) {
	var mu sync.Mutex
	prompts := map[string][]api.Prompt{}
	writes, exports := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/api/public/otel/") {
			exports++
			w.WriteHeader(http.StatusOK)
			return
		}
		pk, sk, ok := r.BasicAuth()
		if !ok || pk != "pk-fixture" || sk != "sk-fixture" {
			t.Error("missing fixture authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		respond := func(value api.Prompt) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Error(err)
			}
		}
		if r.Method == http.MethodPost && r.URL.Path == "/api/public/v2/prompts" {
			var input struct {
				Name   string            `json:"name"`
				Type   api.PromptType    `json:"type"`
				Prompt api.PromptContent `json:"prompt"`
				Labels []string          `json:"labels"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if slices.Contains(input.Labels, "production") {
				t.Error("example must never move production")
			}
			versions := prompts[input.Name]
			for i := range versions {
				versions[i].Labels = slices.DeleteFunc(versions[i].Labels, func(label string) bool { return label == "latest" })
			}
			value := api.Prompt{Name: input.Name, Version: len(versions) + 1, Type: input.Type, Prompt: input.Prompt, Config: json.RawMessage(`{}`), Labels: append(input.Labels, "latest"), Tags: []string{}}
			prompts[input.Name] = append(versions, value)
			writes++
			respond(value)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/api/public/v2/prompts/")
		if r.Method == http.MethodPatch {
			index := strings.LastIndex(name, "/versions/")
			if index < 0 {
				t.Error("invalid label route")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			version, err := strconv.Atoi(name[index+len("/versions/"):])
			name = name[:index]
			versions := prompts[name]
			if err != nil || version < 1 || version > len(versions) {
				t.Error("unknown version")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var input map[string][]string
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			if !slices.Equal(input["newLabels"], []string{"staging"}) {
				t.Error("unexpected deployment labels")
			}
			for i := range versions {
				versions[i].Labels = slices.DeleteFunc(versions[i].Labels, func(label string) bool { return label == "staging" })
			}
			versions[version-1].Labels = append(versions[version-1].Labels, "staging")
			writes++
			respond(versions[version-1])
			return
		}
		if r.Method == http.MethodGet {
			for _, value := range prompts[name] {
				if slices.Contains(value.Labels, r.URL.Query().Get("label")) {
					respond(value)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	if err := run(t.Context(), langfuse.Config{BaseURL: server.URL, PublicKey: "pk-fixture", SecretKey: "sk-fixture"}, "synthetic/folder prompt"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if writes != 5 || exports == 0 || len(prompts["synthetic/folder prompt"]) != 2 || !slices.Contains(prompts["synthetic/folder prompt"][0].Labels, "staging") {
		t.Fatalf("workflow did not complete: writes=%d exports=%d", writes, exports)
	}
}
