package langfuseopenai

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

// protocol recognizes the OpenAI wire routes: chat completions, legacy
// completions, embeddings, and (since v0.2) the Responses API, in both
// plain and Azure path shapes. Suffix matching tolerates reverse-proxy
// path prefixes. Response retrieval (/responses/{id}), input-items
// listing, and background polling pass through unobserved.
type protocol struct {
	captureCap      int
	toolDefinitions bool
}

func (p protocol) Recognize(u *url.URL) (wiretap.Route, bool) {
	path := u.EscapedPath()
	var route wiretap.Route
	switch {
	case strings.HasSuffix(path, "/responses"):
		route = wiretap.Route{Name: "openai.responses", Type: langfuse.TypeGeneration}
	case strings.HasSuffix(path, "/chat/completions"):
		route = wiretap.Route{Name: "openai.chat.completions", Type: langfuse.TypeGeneration}
	case strings.HasSuffix(path, "/embeddings"):
		route = wiretap.Route{Name: "openai.embeddings", Type: langfuse.TypeEmbedding}
	case strings.HasSuffix(path, "/completions"):
		route = wiretap.Route{Name: "openai.completions", Type: langfuse.TypeGeneration}
	default:
		return wiretap.Route{}, false
	}
	route.Provider = classifyProvider(u.Host)
	if deployment, ok := azureDeployment(path); ok {
		route.Provider = "azure-openai"
		route.Metadata = map[string]any{"azure.deployment": deployment}
	}
	if version := u.Query().Get("api-version"); version != "" {
		route.APIVersion = version
	}
	return route, true
}

func (p protocol) NewCall(route wiretap.Route) wiretap.Call {
	if route.Name == "openai.responses" {
		call := newResponsesCall(route, p.captureCap)
		call.toolDefinitions = p.toolDefinitions
		return call
	}
	return &call{route: route, captureCap: p.captureCap, toolDefinitions: p.toolDefinitions}
}

// classifyProvider labels the wire endpoint truthfully; unknown hosts
// are "openai-compatible" and WithProvider overrides for proxies.
func classifyProvider(host string) string {
	host = strings.ToLower(host)
	switch {
	case host == "api.openai.com":
		return "openai"
	case strings.HasSuffix(host, ".openai.azure.com"):
		return "azure-openai"
	case host == "generativelanguage.googleapis.com":
		return "google-openai-compat"
	default:
		return "openai-compatible"
	}
}

// azureDeployment extracts the deployment segment from classic Azure
// paths (/openai/deployments/{deployment}/...). Deployments are
// operator-chosen routing labels, never models; the transport records
// them as metadata only. The segment is stored percent-decoded.
func azureDeployment(escapedPath string) (string, bool) {
	const marker = "/openai/deployments/"
	_, rest, found := strings.Cut(escapedPath, marker)
	if !found {
		return "", false
	}
	end := strings.IndexByte(rest, '/')
	if end <= 0 {
		return "", false
	}
	segment, err := url.PathUnescape(rest[:end])
	if err != nil || segment == "" || len(segment) > 200 {
		return "", false
	}
	return segment, true
}

// validResponseID is a syntactic check for provider identifier shapes. It is
// not a confidentiality boundary: response_id is metadata, exported with
// content export disabled and governed by the observation-metadata masker.
func validResponseID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.', r == ':':
		default:
			return false
		}
	}
	return true
}

// decodeResponseID reads an optional id member on its own, so a malformed
// id is omitted without discarding the rest of the response.
func decodeResponseID(raw json.RawMessage) (string, bool) {
	var id string
	if len(raw) == 0 || json.Unmarshal(raw, &id) != nil || !validResponseID(id) {
		return "", false
	}
	return id, true
}
