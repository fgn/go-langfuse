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
	"context"
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

	goVersion, _ := cellCmd(".", "go", "version")

	var results []cellResult
	failed := false
	cellIndex := 0
	for _, sdk := range m.SDKs {
		versions := append([]string(nil), sdk.Supported...)
		candidate, err := newestStableAbove(sdk.Module, maxVersion(sdk.Supported))
		if err != nil {
			// A discovery failure must never masquerade as "no new
			// versions": that would green a run that probed nothing.
			return fmt.Errorf("version discovery for %s failed: %w", sdk.Module, err)
		}
		if candidate != "" {
			versions = append(versions, candidate)
		}
		for _, version := range versions {
			cellIndex++
			candidate := !contains(sdk.Supported, version)
			result := runCell(tree, cellIndex, sdk.Module, version, candidate)
			results = append(results, result)
			status := "ok"
			if !result.pass {
				status = "FAIL (" + result.phase + ")"
				failed = true
			}
			fmt.Printf("%-40s %-16s candidate=%-5v %s\n", sdk.Module, version, candidate, status)
		}
	}

	if err := writeMarkdown(outPath, results, strings.TrimSpace(goVersion)); err != nil {
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

// runCell clones the pristine integrationtest module into a fresh
// per-cell directory before any edit: cells never share mutated
// go.mod/go.sum state, so one SDK choice cannot leak into the next.
func runCell(tree string, index int, module, version string, candidate bool) cellResult {
	result := cellResult{module: module, version: version, candidate: candidate}
	pristine := filepath.Join(tree, "contrib/integrationtest")
	cell := filepath.Join(tree, "contrib", fmt.Sprintf("integrationtest-cell%d", index))
	if err := copyPath(pristine, cell); err != nil {
		result.phase, result.detail = "setup", err.Error()
		return result
	}

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
		if graph, graphErr := cellCmd(cell, "go", "list", "-m", "all"); graphErr == nil {
			// Attribution: the resolved graph accompanies every failure.
			fmt.Printf("resolved graph for failing cell %s@%s:\n%s\n", module, version, graph)
		}
		return result
	}
	result.pass = true
	return result
}

// cellCmd runs one command with the allowlisted hermetic environment
// and a hard per-command deadline: workspace off, local toolchain,
// FIXED proxy and sumdb (never ambient overrides), GOFLAGS/GOENV
// neutralized; module and build caches are shared deliberately
// (checksum-verified and content-addressed).
func cellCmd(dir, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(hermeticEnv(),
		"HOME="+os.Getenv("HOME"),
		"PATH="+os.Getenv("PATH"),
	)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(out), fmt.Errorf("timed out after 10m: %w", ctx.Err())
	}
	return string(out), err
}

func hermeticEnv() []string {
	home := os.Getenv("HOME")
	return []string{
		"GOWORK=off",
		"GOTOOLCHAIN=local",
		"GOFLAGS=",
		"GOENV=off",
		"GOPROXY=https://proxy.golang.org,direct",
		"GOSUMDB=sum.golang.org",
		"GOMODCACHE=" + filepath.Join(home, "go", "pkg", "mod"),
		"GOPATH=" + filepath.Join(home, "go"),
	}
}

// newestStableAbove returns the newest stable (non-prerelease) version
// of the module above the floor, "" when none exists, and an error
// when discovery itself failed (which must fail the run).
func newestStableAbove(module, floor string) (string, error) {
	out, err := cellCmd(".", "go", "list", "-m", "-versions", module+"@latest")
	if err != nil {
		return "", fmt.Errorf("go list -m -versions: %s", firstLine(out))
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
		return "", nil
	}
	return newest, nil
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

func writeMarkdown(path string, results []cellResult, goVersion string) error {
	var b strings.Builder
	b.WriteString("# Tested provider-SDK transport compatibility\n\n")
	b.WriteString("Generated by `task matrix` on " + time.Now().UTC().Format("2006-01-02") +
		" with `" + goVersion + "`. ")
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
