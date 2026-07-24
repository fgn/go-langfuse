package langfuseopenai

import (
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
	captureCap int
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
		return newResponsesCall(route, p.captureCap)
	}
	return &call{route: route, captureCap: p.captureCap}
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
