package langfusegenai_test

import (
	"context"
	"log"
	"net/http"

	"github.com/fgn/go-langfuse"
	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
)

// Attach the transport where the HTTP client is constructed; genai
// call sites do not change. For the Vertex AI backend, compose the
// authenticated transport first, as shown in the module README.
func ExampleNewTransport() {
	lf, err := langfuse.New(context.Background(), langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	httpClient := &http.Client{Transport: langfusegenai.NewTransport(lf, nil)}

	// Pass httpClient to google.golang.org/genai, for example:
	//   genai.NewClient(ctx, &genai.ClientConfig{
	//       APIKey:     apiKey,
	//       HTTPClient: httpClient,
	//   })
	_ = httpClient
}
