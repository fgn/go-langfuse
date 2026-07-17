package transport

import (
	"errors"
	"net/url"
	"strings"
)

const (
	defaultBaseURL = "https://cloud.langfuse.com"
	otelBasePath   = "/api/public/otel"
	tracesPath     = otelBasePath + "/v1/traces"
)

// NormalizeEndpoint converts a Langfuse host, OTLP base endpoint, or complete
// traces endpoint into the canonical Langfuse OTLP/HTTP traces endpoint.
func NormalizeEndpoint(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		value = defaultBaseURL
	}

	u, err := url.Parse(value)
	if err != nil {
		return "", errors.New("langfuse transport: invalid base URL")
	}

	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("langfuse transport: base URL must use http or https")
	}
	if u.Opaque != "" || u.Host == "" || u.Hostname() == "" {
		return "", errors.New("langfuse transport: base URL must include a host")
	}
	if u.User != nil {
		return "", errors.New("langfuse transport: base URL must not contain credentials")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", errors.New("langfuse transport: base URL must not contain a query")
	}
	if u.Fragment != "" || strings.Contains(value, "#") {
		return "", errors.New("langfuse transport: base URL must not contain a fragment")
	}
	if u.RawPath != "" {
		return "", errors.New("langfuse transport: base URL path must not be escaped")
	}

	path := strings.TrimSuffix(u.Path, "/")
	switch path {
	case "", otelBasePath, tracesPath:
		u.Path = tracesPath
	default:
		return "", errors.New("langfuse transport: base URL has an unsupported path")
	}

	u.RawPath = ""
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return u.String(), nil
}
