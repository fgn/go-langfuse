// This example shows the langfusegenai adapter on Vertex AI with the
// documented credentials composition: the caller's HTTP client policy
// survives, the OAuth token is attached by the inner auth transport
// (the adapter never sees it), and every generateContent call records
// a Langfuse generation automatically.
//
// It runs out of the box with a synthetic token provider and a
// synthetic Gemini-wire server, so only Langfuse credentials are
// needed:
//
//	export LANGFUSE_BASE_URL=http://localhost:3000
//	export LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-...
//	go run ./examples/vertexgenai
//
// For a real Vertex project, replace the synthetic credentials with
// credentials.DetectDefault (see the commented block) and drop the
// BaseURL override.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/httptransport"
	genai "google.golang.org/genai"

	"github.com/fgn/go-langfuse"
	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create Langfuse client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := lf.Shutdown(shutdownCtx); err != nil {
			log.Printf("shut down Langfuse client: %v", err)
		}
	}()

	// Synthetic stand-ins so the example runs anywhere. For a real
	// Vertex project use:
	//
	//	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
	//	    Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
	//	})
	creds := auth.NewCredentials(&auth.CredentialsOptions{TokenProvider: staticToken{}})
	provider := syntheticGemini()
	defer provider.Close()

	// The documented composition, resolving the genai TODO pattern
	// ("setting HTTPClient without losing OAuth credentials"): start
	// from the caller-owned policy client, build the authenticated
	// transport over its transport, and layer Langfuse outside, so the
	// adapter never sees Authorization headers.
	base := &http.Client{Timeout: 30 * time.Second}
	baseRT := base.Transport
	if baseRT == nil {
		baseRT = http.DefaultTransport
	}
	authed, err := httptransport.NewClient(&httptransport.Options{
		Credentials:      creds,
		BaseRoundTripper: baseRT,
	})
	if err != nil {
		return fmt.Errorf("build authenticated transport: %w", err)
	}
	client := *base
	client.Transport = langfusegenai.NewTransport(lf, authed.Transport)

	gemini, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:     genai.BackendVertexAI,
		Project:     "example-project",
		Location:    "eu",
		Credentials: creds,
		HTTPClient:  &client,
		HTTPOptions: genai.HTTPOptions{BaseURL: provider.URL}, // drop for real Vertex
	})
	if err != nil {
		return fmt.Errorf("create genai client: %w", err)
	}

	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: "summarize-document", UserID: "user-123",
	})
	response, err := gemini.Models.GenerateContent(ctx,
		"gemini-3.6-flash", genai.Text("Summarize what this adapter records."), nil)
	if err != nil {
		return fmt.Errorf("generate content: %w", err)
	}

	fmt.Println("model answered:", response.Text())
	fmt.Println("The Langfuse generation carries the URL-derived model (overridden")
	fmt.Println("by the response modelVersion), prompt and candidate token usage")
	fmt.Println("including thought tokens, the sanitized output parts, and the")
	fmt.Println("finish reason, with OAuth handled entirely below the adapter.")
	return nil
}

type staticToken struct{}

func (staticToken) Token(context.Context) (*auth.Token, error) {
	return &auth.Token{Value: "synthetic-oauth-token", Expiry: time.Now().Add(time.Hour)}, nil
}

// syntheticGemini speaks just enough Gemini wire format for the
// example to run without a Google project.
func syntheticGemini() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"candidates":[{"content":{"role":"model","parts":[
				{"text":"Model, usage, thoughts, output parts, and finish reason."}
			]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":11,"thoughtsTokenCount":3},
			"modelVersion":"gemini-3.6-flash-002"
		}`)
	}))
}
