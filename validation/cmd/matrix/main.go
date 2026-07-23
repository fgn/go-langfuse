//go:build validation

// Command matrix produces the tested provider-SDK transport
// compatibility table: for each manifest version (plus the newest
// stable release above the manifest maximum, as a candidate), it
// copies the core, adapter, and integrationtest modules into a temp
// tree, requires the SDK version, and runs the synthetic-wire suite.
// A checkmark is evidence that the canonical client program compiles
// against that SDK version and its calls flow through the adapters
// with the suite's assertions passing on the repository toolchain. It
// is not a blanket SDK or provider support claim.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type manifest struct {
	SDKs []struct {
		Module    string   `json:"module"`
		Supported []string `json:"supported"`
	} `json:"sdks"`
}

type cellResult struct {
	module, version, phase string
	candidate              bool
	pass                   bool
	detail                 string
}

func main() {
	out := flag.String("out", "../docs/support-matrix.md", "output markdown path")
	flag.Parse()
	if err := run(*out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(outPath string) error {
	raw, err := os.ReadFile("matrix.json")
	if err != nil {
		return err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}

	repoRoot, err := filepath.Abs("..")
	if err != nil {
		return err
	}
	tree, err := buildTree(repoRoot)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tree)

	var results []cellResult
	failed := false
	for _, sdk := range m.SDKs {
		versions := append([]string(nil), sdk.Supported...)
		if candidate := newestStableAbove(sdk.Module, maxVersion(sdk.Supported)); candidate != "" {
			versions = append(versions, candidate)
		}
		for _, version := range versions {
			candidate := !contains(sdk.Supported, version)
			result := runCell(tree, sdk.Module, version, candidate)
			results = append(results, result)
			status := "ok"
			if !result.pass {
				status = "FAIL (" + result.phase + ")"
				failed = true
			}
			fmt.Printf("%-40s %-16s candidate=%-5v %s\n", sdk.Module, version, candidate, status)
		}
	}

	if err := writeMarkdown(outPath, results); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("matrix has failing cells; see %s", outPath)
	}
	return nil
}

// buildTree copies the four modules into a temp directory preserving
// their relative layout, so the integrationtest replace directives
// stay valid, and adds a replacement pinning core to the checkout.
func buildTree(repoRoot string) (string, error) {
	tree, err := os.MkdirTemp("", "matrix-*")
	if err != nil {
		return "", err
	}
	copies := map[string][]string{
		".":                       {"go.mod", "go.sum", "internal"},
		"contrib/openai":          {"go.mod", "go.sum", "internal", "README.md"},
		"contrib/googlegenai":     {"go.mod", "go.sum", "internal", "README.md"},
		"contrib/integrationtest": {"go.mod", "go.sum"},
	}
	// Root and adapter Go sources live at the module roots.
	for _, module := range []string{".", "contrib/openai", "contrib/googlegenai", "contrib/integrationtest"} {
		entries, err := os.ReadDir(filepath.Join(repoRoot, module))
		if err != nil {
			return "", err
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".go") {
				copies[module] = append(copies[module], entry.Name())
			}
		}
	}
	for module, names := range copies {
		for _, name := range names {
			src := filepath.Join(repoRoot, module, name)
			dst := filepath.Join(tree, module, name)
			if err := copyPath(src, dst); err != nil {
				return "", fmt.Errorf("copy %s: %w", src, err)
			}
		}
	}
	// The temp integrationtest must use the checkout's core, not the
	// released one, so the matrix tests current adapters end to end.
	cell := filepath.Join(tree, "contrib/integrationtest")
	if out, err := cellCmd(cell, "go", "mod", "edit",
		"-replace", "github.com/fgn/go-langfuse=../.."); err != nil {
		return "", fmt.Errorf("pin core: %v: %s", err, out)
	}
	return tree, nil
}

func runCell(tree, module, version string, candidate bool) cellResult {
	result := cellResult{module: module, version: version, candidate: candidate}
	cell := filepath.Join(tree, "contrib/integrationtest")

	steps := [][]string{
		{"go", "mod", "edit", "-require", module + "@" + version},
		{"go", "mod", "tidy"},
	}
	for _, step := range steps {
		if out, err := cellCmd(cell, step[0], step[1:]...); err != nil {
			result.phase, result.detail = "resolve", firstLine(out)
			return result
		}
	}
	// The resolved graph must actually contain the requested version.
	out, err := cellCmd(cell, "go", "list", "-m", module)
	if err != nil || !strings.Contains(out, version) {
		result.phase, result.detail = "resolve", "requested version not in resolved graph: "+firstLine(out)
		return result
	}
	if out, err := cellCmd(cell, "go", "test", "-mod=readonly", "-count=1", "./..."); err != nil {
		result.phase, result.detail = "test", firstLine(out)
		return result
	}
	result.pass = true
	return result
}

// cellCmd runs one command with the allowlisted hermetic environment:
// workspace off, local toolchain, explicit proxy and sumdb, ambient
// GOFLAGS/GOENV neutralized; module and build caches are shared
// deliberately (checksum-verified and content-addressed).
func cellCmd(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(hermeticEnv(),
		"HOME="+os.Getenv("HOME"),
		"PATH="+os.Getenv("PATH"),
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func hermeticEnv() []string {
	return []string{
		"GOWORK=off",
		"GOTOOLCHAIN=local",
		"GOFLAGS=",
		"GOENV=off",
		"GOPROXY=" + envOr("GOPROXY", "https://proxy.golang.org,direct"),
		"GOSUMDB=" + envOr("GOSUMDB", "sum.golang.org"),
		"GOMODCACHE=" + envOr("GOMODCACHE", filepath.Join(os.Getenv("HOME"), "go", "pkg", "mod")),
		"GOPATH=" + envOr("GOPATH", filepath.Join(os.Getenv("HOME"), "go")),
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// newestStableAbove returns the newest stable (non-prerelease) version
// of the module above the floor, or "" when none exists.
func newestStableAbove(module, floor string) string {
	out, err := cellCmd(".", "go", "list", "-m", "-versions", module+"@latest")
	if err != nil {
		return ""
	}
	fields := strings.Fields(out)
	newest := ""
	for _, field := range fields[1:] {
		if strings.Contains(field, "-") {
			continue // prereleases excluded unless explicitly pinned
		}
		newest = field // `go list -m -versions` sorts ascending
	}
	if newest == "" || newest == floor || semverLE(newest, floor) {
		return ""
	}
	return newest
}

func maxVersion(versions []string) string {
	max := ""
	for _, version := range versions {
		if max == "" || semverLE(max, version) {
			max = version
		}
	}
	return max
}

// semverLE is a minimal comparison sufficient for release versions.
func semverLE(a, b string) bool {
	pa, pb := parts(a), parts(b)
	for i := range 3 {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return true
}

func parts(version string) [3]int {
	var p [3]int
	fmt.Sscanf(strings.TrimPrefix(version, "v"), "%d.%d.%d", &p[0], &p[1], &p[2])
	return p
}

func contains(list []string, item string) bool {
	for _, candidate := range list {
		if candidate == item {
			return true
		}
	}
	return false
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		s = s[:idx]
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

func copyPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := copyPath(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

func writeMarkdown(path string, results []cellResult) error {
	var b strings.Builder
	b.WriteString("# Tested provider-SDK transport compatibility\n\n")
	b.WriteString("Generated by `task matrix` on " + time.Now().UTC().Format("2006-01-02") + ". ")
	b.WriteString("A checkmark means the canonical integration program compiled against\n")
	b.WriteString("that SDK version and its calls flowed through the adapters with the\n")
	b.WriteString("synthetic-wire suite's assertions passing on the repository's Go\n")
	b.WriteString("toolchain. Exercised operations: chat completions (unary and\n")
	b.WriteString("streaming with usage), embeddings routes, generateContent (unary and\n")
	b.WriteString("streaming), Vertex auth composition, per-attempt retry recording.\n")
	b.WriteString("This is not a blanket SDK or provider support claim. Never edit by\n")
	b.WriteString("hand; regenerate with `task matrix`.\n\n")
	b.WriteString("| SDK module | Version | Status |\n| --- | --- | --- |\n")
	for _, result := range results {
		if result.candidate {
			continue
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n", result.module, result.version, mark(result)))
	}
	b.WriteString("\n## Candidates (newest stable, not yet promoted)\n\n")
	b.WriteString("| SDK module | Version | Status |\n| --- | --- | --- |\n")
	any := false
	for _, result := range results {
		if !result.candidate {
			continue
		}
		any = true
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n", result.module, result.version, mark(result)))
	}
	if !any {
		b.WriteString("| (none discovered) | | |\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func mark(result cellResult) string {
	if result.pass {
		return "✅"
	}
	return "❌ " + result.phase + ": " + result.detail
}
