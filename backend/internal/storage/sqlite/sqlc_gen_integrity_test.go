package sqlite

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// sqlc 1.31 slices each query out of the source by byte offset. A multi-byte
// character in a comment (an em dash, say) shifts every later offset, so the
// generated const silently starts mid-statement ("0;\n\nSELECT ...") or ends
// short ("ORDER BY n"). The package still compiles and the query only fails
// at run time. This test re-derives each query from queries/*.sql and demands
// the generated const match it.

var (
	genQueryRe = regexp.MustCompile(`(?s)const (\w+) = ` + "`" + `-- name: (\w+) :\w+
(.*?)` + "`" + `
`)
	srcNameRe   = regexp.MustCompile(`(?m)^-- name: (\w+) :\w+\s*$`)
	paramRe     = regexp.MustCompile(`sqlc\.n?arg\([^)]*\)|@\w+|\?\d*`)
	lineCommRe  = regexp.MustCompile(`(?m)--[^\n]*`)
	whitespaceR = regexp.MustCompile(`\s+`)
)

func normalizeSQL(s string) string {
	s = lineCommRe.ReplaceAllString(s, " ")
	s = paramRe.ReplaceAllString(s, "?")
	s = whitespaceR.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	return strings.TrimSpace(strings.TrimSuffix(s, ";"))
}

func TestGeneratedQueriesMatchSource(t *testing.T) {
	source := map[string]string{}
	srcFiles, err := filepath.Glob("queries/*.sql")
	if err != nil || len(srcFiles) == 0 {
		t.Fatalf("no query sources found: %v", err)
	}
	for _, f := range srcFiles {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ReplaceAll(string(raw), "\r\n", "\n")
		idx := srcNameRe.FindAllStringSubmatchIndex(text, -1)
		for i, m := range idx {
			end := len(text)
			if i+1 < len(idx) {
				end = idx[i+1][0]
			}
			source[text[m[2]:m[3]]] = normalizeSQL(text[m[1]:end])
		}
	}

	genFiles, err := filepath.Glob("gen/*.sql.go")
	if err != nil || len(genFiles) == 0 {
		t.Fatalf("no generated query files found: %v", err)
	}
	seen := 0
	for _, f := range genFiles {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range genQueryRe.FindAllStringSubmatch(strings.ReplaceAll(string(raw), "\r\n", "\n"), -1) {
			name, body := m[2], normalizeSQL(m[3])
			seen++
			want, ok := source[name]
			if !ok {
				t.Errorf("%s: generated query %s has no source in queries/*.sql (stale gen?)", f, name)
				continue
			}
			// sqlc rewrites the select list (SELECT * expansion, sqlc.embed), so
			// compare the shape that offset drift breaks: how the statement
			// starts and how it ends.
			if !sqlStartRe.MatchString(body) {
				t.Errorf("%s: generated query %s does not start with a SQL keyword (sqlc offset corruption? check for non-ASCII characters in queries/*.sql)\n got: %s", f, name, head(body))
			}
			if !hasStar(want) && !strings.HasSuffix(body, lastTokens(want, 8)) {
				t.Errorf("%s: generated query %s ends differently from its source (sqlc offset corruption? check for non-ASCII characters in queries/*.sql)\n got: %s\nwant: %s", f, name, tail(body), tail(want))
			}
		}
	}
	if seen < len(source)/2 {
		t.Fatalf("matched only %d generated queries against %d sources; the extraction regex is stale", seen, len(source))
	}
}

var sqlStartRe = regexp.MustCompile(`(?i)^(SELECT|INSERT|UPDATE|DELETE|WITH|REPLACE)\b`)

var starRe = regexp.MustCompile(`(?i)count\(\*\)|\d\s*\*\s*\d`)

// hasStar reports whether sqlc expands a wildcard in the statement (SELECT *,
// RETURNING *), which changes the generated text away from the source.
func hasStar(s string) bool { return strings.Contains(starRe.ReplaceAllString(s, ""), "*") }

func lastTokens(s string, n int) string {
	f := strings.Fields(s)
	if len(f) > n {
		f = f[len(f)-n:]
	}
	return strings.Join(f, " ")
}

func head(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

func tail(s string) string {
	if len(s) > 160 {
		return "..." + s[len(s)-160:]
	}
	return s
}

// Non-ASCII bytes in query sources are what trigger the offset drift, so ban
// them outright; the failure message is then actionable before regeneration.
func TestQuerySourcesAreASCII(t *testing.T) {
	files, _ := filepath.Glob("queries/*.sql")
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			for _, r := range line {
				if r > 127 {
					t.Errorf("%s:%d contains non-ASCII %q; sqlc 1.31 mis-slices every later query (use ASCII)", f, i+1, r)
					break
				}
			}
		}
	}
}
