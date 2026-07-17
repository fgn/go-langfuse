package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxScoreErrorBodyBytes bounds how much of an error response body is copied
// into the returned error message.
const maxScoreErrorBodyBytes = 256

// ScoresClient submits scores to the Langfuse REST scores endpoint.
type ScoresClient struct {
	endpoint  string
	publicKey string
	secretKey string
	client    *http.Client
}

// NewScoresClient builds a scores client from an already validated transport
// configuration. It performs no network I/O.
func NewScoresClient(cfg Config) (*ScoresClient, error) {
	endpoint, err := NormalizeScoresEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &ScoresClient{
		endpoint:  endpoint,
		publicKey: cfg.PublicKey,
		secretKey: cfg.SecretKey,
		client:    &http.Client{Timeout: timeout},
	}, nil
}

// Send posts one JSON-encoded score payload. The response body is read only
// to build a bounded error message; credentials travel exclusively in the
// Authorization header and never appear in returned errors.
func (s *ScoresClient) Send(ctx context.Context, payload []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		return errors.New("langfuse transport: build score request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(s.publicKey, s.secretKey)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("langfuse transport: send score: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		excerpt, _ := io.ReadAll(io.LimitReader(response.Body, maxScoreErrorBodyBytes))
		return fmt.Errorf("langfuse transport: scores endpoint returned %s: %s",
			response.Status, string(excerpt))
	}
	return nil
}
