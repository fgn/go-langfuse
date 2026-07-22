package langfusegenai

import (
	"net/url"
	"strings"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/googlegenai/internal/wiretap"
)

// protocol recognizes the Gemini API wire format on both backends the
// google.golang.org/genai SDK targets: the Developer API
// (generativelanguage.googleapis.com, models/... and tunedModels/...)
// and Vertex AI (projects/.../locations/.../publishers/{publisher}/
// models/...), across API versions v1, v1beta, and v1beta1. The model
// is extracted from the URL, where the SDK places it; countTokens is
// deliberately unobserved in v0.1.
type protocol struct {
	captureCap int
}

// methodRoutes maps the URL method suffix to route identity. Vertex
// embeddings additionally use :predict on embedding models.
var methodRoutes = map[string]struct {
	name      string
	obsType   langfuse.ObservationType
	streaming bool
}{
	"generateContent":       {"genai.generate_content", langfuse.TypeGeneration, false},
	"streamGenerateContent": {"genai.generate_content_stream", langfuse.TypeGeneration, true},
	"embedContent":          {"genai.embed_content", langfuse.TypeEmbedding, false},
	"batchEmbedContents":    {"genai.batch_embed_contents", langfuse.TypeEmbedding, false},
	"predict":               {"genai.predict", langfuse.TypeEmbedding, false},
}

func (p protocol) Recognize(u *url.URL) (wiretap.Route, bool) {
	path := u.EscapedPath()
	colon := strings.LastIndexByte(path, ':')
	if colon < 0 || colon == len(path)-1 {
		return wiretap.Route{}, false
	}
	method := path[colon+1:]
	spec, ok := methodRoutes[method]
	if !ok {
		return wiretap.Route{}, false
	}
	resource := path[:colon]
	model, version, qualified := parseModelResource(resource)
	route := wiretap.Route{
		Provider:   classifyProvider(u.Host),
		Name:       spec.name,
		APIVersion: version,
		Model:      model,
		Type:       spec.obsType,
		Streaming:  spec.streaming || u.Query().Get("alt") == "sse",
	}
	if model == "" && qualified != "" {
		// A fully qualified resource the grammar does not map to a
		// bare model (for example cached content or corpora) is still
		// observed, with the resource recorded as metadata only.
		route.Metadata = map[string]any{"resource": qualified}
	}
	return route, true
}

func (p protocol) NewCall(route wiretap.Route) wiretap.Call {
	return &call{route: route, captureCap: p.captureCap}
}

func classifyProvider(host string) string {
	host = strings.ToLower(host)
	switch {
	case host == "generativelanguage.googleapis.com":
		return "google-genai"
	case strings.HasSuffix(host, "aiplatform.googleapis.com"):
		return "google-vertex"
	default:
		return "google-genai-compatible"
	}
}

// parseModelResource extracts the model and API version from the
// resource path preceding the method. Supported shapes, tolerating
// reverse-proxy prefixes via segment scanning:
//
//	.../{version}/models/{model}
//	.../{version}/tunedModels/{model}
//	.../{version}/projects/{p}/locations/{l}/publishers/{pub}/models/{model}
//
// Any other resource returns model "" and the percent-decoded resource
// tail for metadata. Segments are decoded individually and bounded.
func parseModelResource(escapedPath string) (model, version, qualified string) {
	segments := strings.Split(strings.Trim(escapedPath, "/"), "/")
	for index, segment := range segments {
		switch segment {
		case "v1", "v1beta", "v1beta1":
			version = segment
		case "models", "tunedModels":
			if index == len(segments)-1 {
				continue
			}
			if decoded, ok := decodeSegment(segments[index+1]); ok {
				// The last models/... pair wins: Vertex paths contain
				// exactly one, Developer paths contain exactly one.
				model = decoded
			}
		}
	}
	if model != "" {
		return model, version, ""
	}
	tail := segments
	if len(tail) > 8 {
		tail = tail[len(tail)-8:]
	}
	if decoded, err := url.PathUnescape(strings.Join(tail, "/")); err == nil && len(decoded) <= 400 {
		qualified = decoded
	}
	return "", version, qualified
}

func decodeSegment(segment string) (string, bool) {
	decoded, err := url.PathUnescape(segment)
	if err != nil || decoded == "" || len(decoded) > 200 {
		return "", false
	}
	return decoded, true
}
