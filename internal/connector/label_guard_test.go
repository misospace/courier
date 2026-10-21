package connector

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// This file implements the no-free-form-label assertion of
// docs/connector-diagnostics.md §11.3: a label value must never be built
// from fmt.Sprintf, because a formatted string carries whatever the
// upstream handed over and can grow the series cardinality without bound.
// The guard has two independent halves, exactly as the contract describes:
//
//   - an AST walk (go/parser + go/ast) over the connector and adapter
//     packages that finds a formatted string in a label position, and
//   - a CI grep over the metric registration helpers that rejects a single
//     line pairing a label-map literal with fmt.Sprintf.
//
// A label position is one of:
//
//   - an argument of a WithLabelValues call,
//   - the key or the value of an element of a label-map literal, where a
//     label-map literal is a composite literal of type prometheus.Labels
//     or of type map[string]string, and
//   - the right-hand side of an assignment whose left-hand side indexes a
//     label map.
//
// A label position builds a free-form label when the expression, at any
// nesting depth, holds a call to fmt.Sprintf or a call to a formatting
// helper: a function or method declared in the same file whose body
// formats. The helper rule is a deliberately conservative, fail-closed
// approximation: a label position fed by a same-file helper that formats
// is a violation even when the helper launders the format away.
//
// The halves are independent on purpose: the AST half is precise but only
// sees syntactically obvious label positions, the grep half is blunt but
// catches the shape of the problem wherever it appears on one line. The
// negative tests below prove both halves can fail, so a green run means
// "no violation found", never "the guard found nothing because it looked
// in the wrong place".

// labelScanRoots are the package roots the guard walks: the connector
// envelope, the Dispatch and manual adapters, the core discovery metrics,
// and the controller's collectors — the packages that own the connector and
// core metric families. The paths are relative to this package's directory,
// which is the working directory of the test binary.
var labelScanRoots = []string{
	"../connector",
	"../source",
	"../controller",
}

// metricHelperMarkers identify a Go file as a metric registration helper:
// a file that builds metric families or label sets. Only those files are
// scanned by the grep half.
var metricHelperMarkers = []string{
	"prometheus.NewDesc(",
	"prometheus.NewCounterVec(",
	"prometheus.NewGaugeVec(",
	"prometheus.NewHistogramVec(",
	"prometheus.NewSummaryVec(",
	"prometheus.NewCounter(",
	"prometheus.NewGauge(",
	".WithLabelValues(",
	"prometheus.Labels{",
	"map[string]string{",
}

// labelViolation is one free-form label construction site.
type labelViolation struct {
	// half names the guard half that found the violation: "ast" or "grep".
	half string
	// file is the source file, relative to the module root when it is
	// inside the module.
	file string
	// line is the 1-based line of the offending expression.
	line int
	// reason explains what was found, including the offending expression.
	reason string
}

func (v labelViolation) String() string {
	return fmt.Sprintf("%s:%d: [%s] %s", v.file, v.line, v.half, v.reason)
}

// labelScanResult reports what the guard examined and what it found.
type labelScanResult struct {
	violations []labelViolation
	// files is the number of Go files the scan read.
	files int
	// astFiles is the number of files the AST walk parsed and inspected.
	astFiles int
	// grepFiles is the number of metric registration helpers the grep half
	// scanned.
	grepFiles int
}

// scanLabelSources walks the given roots and returns every free-form label
// value construction site it finds. It is the shared body of the CI
// assertion: the same function that judges the repository also judges the
// synthetic violations in the negative tests, so a passing negative test
// proves the guard can fail.
func scanLabelSources(roots []string) ([]labelViolation, error) {
	result, err := scanLabelSourcesDetailed(roots)
	if err != nil {
		return nil, err
	}
	return result.violations, nil
}

// scanLabelSourcesDetailed runs both halves of the guard over the given
// roots and reports both the violations and what was examined.
func scanLabelSourcesDetailed(roots []string) (labelScanResult, error) {
	var result labelScanResult
	files, err := labelScanFiles(roots)
	if err != nil {
		return result, err
	}
	result.files = len(files)
	root := moduleRoot()
	fset := token.NewFileSet()
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			return result, fmt.Errorf("label guard: read %s: %w", path, err)
		}
		parsed, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return result, fmt.Errorf("label guard: parse %s: %w", path, err)
		}
		result.astFiles++
		scanner := &labelScanner{
			fset:          fset,
			file:          parsed,
			path:          displayPath(path, root),
			src:           src,
			root:          root,
			labelMaps:     map[string]struct{}{},
			formatHelpers: map[string]struct{}{},
		}
		scanner.collectLabelMaps()
		scanner.collectFormatHelpers()
		scanner.scan()
		result.violations = append(result.violations, scanner.violations...)
		if isMetricHelper(src) {
			result.grepFiles++
			result.violations = append(result.violations, grepLabelViolations(scanner.path, src)...)
		}
	}
	sort.Slice(result.violations, func(i, j int) bool {
		a, b := result.violations[i], result.violations[j]
		if a.file != b.file {
			return a.file < b.file
		}
		if a.line != b.line {
			return a.line < b.line
		}
		return a.half < b.half
	})
	return result, nil
}

// labelScanFiles resolves the given roots into the set of Go files to scan.
// A root is a package directory (walked recursively) or a single file.
// Test files are excluded: the guard judges production label construction.
func labelScanFiles(roots []string) ([]string, error) {
	seen := map[string]struct{}{}
	var files []string
	add := func(path string) error {
		abs, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("label guard: resolve %s: %w", path, err)
		}
		if _, ok := seen[abs]; ok {
			return nil
		}
		seen[abs] = struct{}{}
		files = append(files, abs)
		return nil
	}
	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			return nil, fmt.Errorf("label guard: stat %s: %w", root, err)
		}
		if !info.IsDir() {
			if err := add(root); err != nil {
				return nil, err
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			return add(path)
		})
		if err != nil {
			return nil, fmt.Errorf("label guard: walk %s: %w", root, err)
		}
	}
	sort.Strings(files)
	return files, nil
}

// labelScanner is the AST half of the guard: it walks one parsed file and
// reports every fmt.Sprintf that sits in a label-value position.
type labelScanner struct {
	fset *token.FileSet
	file *ast.File
	// path is the display path of the file.
	path string
	// src is the file's source, used to quote the offending expression.
	src []byte
	// root is the module root, used to build display paths.
	root string
	// labelMaps holds the names of the file's map[string]string and
	// prometheus.Labels variables.
	labelMaps map[string]struct{}
	// formatHelpers holds the names of the file's functions and methods
	// whose body formats: a label position fed by one of them carries a
	// free-form label even when the format is laundered through the call.
	formatHelpers map[string]struct{}
	// violations holds what this file's walk found.
	violations []labelViolation
}

// collectLabelMaps records the file's label-map variables: the ones a
// label value may be written into.
func (s *labelScanner) collectLabelMaps() {
	for _, decl := range s.file.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if d.Tok != token.VAR {
				continue
			}
			for _, spec := range d.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || !isLabelMapType(value.Type) {
					continue
				}
				for _, name := range value.Names {
					s.labelMaps[name.Name] = struct{}{}
				}
			}
		case *ast.FuncDecl:
			s.collectParams(d.Type.Params)
		}
	}
	ast.Inspect(s.file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Tok != token.DEFINE {
			return true
		}
		for i, lhs := range assign.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name == "_" || i >= len(assign.Rhs) {
				continue
			}
			if isLabelMapExpr(assign.Rhs[i]) {
				s.labelMaps[ident.Name] = struct{}{}
			}
		}
		return true
	})
}

// collectParams records the label-map parameters of a function.
func (s *labelScanner) collectParams(fields *ast.FieldList) {
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		if !isLabelMapType(field.Type) {
			continue
		}
		for _, name := range field.Names {
			s.labelMaps[name.Name] = struct{}{}
		}
	}
}

// collectFormatHelpers records the file's formatting helpers: the functions
// and methods declared in this file whose body calls fmt.Sprintf. A label
// position fed by one of them carries a free-form label, so the helper is
// treated as a format even when the format is laundered through the call.
func (s *labelScanner) collectFormatHelpers() {
	for _, decl := range s.file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if !containsSprintf(fn.Body) {
			continue
		}
		s.formatHelpers[fn.Name.Name] = struct{}{}
	}
}

// scan walks the file and reports every label position that builds a
// free-form label value or key.
func (s *labelScanner) scan() {
	ast.Inspect(s.file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			s.checkCall(typed)
		case *ast.CompositeLit:
			s.checkCompositeLit(typed)
		case *ast.AssignStmt:
			s.checkAssign(typed)
		}
		return true
	})
}

// checkCall flags a free-form label value passed to a WithLabelValues call.
func (s *labelScanner) checkCall(call *ast.CallExpr) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "WithLabelValues" {
		return
	}
	for _, arg := range call.Args {
		if !s.buildsLabelValue(arg) {
			continue
		}
		s.report(arg, "free-form label value passed to WithLabelValues")
	}
}

// checkCompositeLit flags a free-form label value or key used in an element
// of a label-map literal: a prometheus.Labels literal or a map[string]string
// literal.
func (s *labelScanner) checkCompositeLit(lit *ast.CompositeLit) {
	if !isLabelMapType(lit.Type) {
		return
	}
	kind := labelLiteralKind(lit)
	for _, element := range lit.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if s.buildsLabelValue(pair.Value) {
			s.report(pair.Value, fmt.Sprintf("free-form label value in a %s literal", kind))
		}
		if s.buildsLabelValue(pair.Key) {
			s.report(pair.Key, fmt.Sprintf("free-form label key in a %s literal", kind))
		}
	}
}

// checkAssign flags a free-form label value written into a label map.
func (s *labelScanner) checkAssign(assign *ast.AssignStmt) {
	for i, lhs := range assign.Lhs {
		index, ok := lhs.(*ast.IndexExpr)
		if !ok || i >= len(assign.Rhs) {
			continue
		}
		if !s.isLabelMap(index.X) {
			continue
		}
		if !s.buildsLabelValue(assign.Rhs[i]) {
			continue
		}
		s.report(assign.Rhs[i], "free-form label value assigned into a label map")
	}
}

// isLabelMap reports whether the expression names a label map: a declared
// map[string]string or prometheus.Labels variable, or a prometheus.Labels
// field of a struct.
func (s *labelScanner) isLabelMap(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		_, ok := s.labelMaps[typed.Name]
		return ok
	case *ast.SelectorExpr:
		return typed.Sel.Name == "Labels"
	case *ast.StarExpr:
		return s.isLabelMap(typed.X)
	case *ast.ParenExpr:
		return s.isLabelMap(typed.X)
	}
	return false
}

// report records one violation, quoting the offending expression.
func (s *labelScanner) report(node ast.Node, reason string) {
	s.violations = append(s.violations, labelViolation{
		half:   "ast",
		file:   s.path,
		line:   s.fset.Position(node.Pos()).Line,
		reason: fmt.Sprintf("%s: %s", reason, s.sourceOf(node)),
	})
}

// sourceOf returns the source text of a node.
func (s *labelScanner) sourceOf(node ast.Node) string {
	start := s.fset.Position(node.Pos()).Offset
	end := s.fset.Position(node.End()).Offset
	if start < 0 || end > len(s.src) || start >= end {
		return "<expression>"
	}
	return strings.TrimSpace(string(s.src[start:end]))
}

// buildsLabelValue reports whether the expression builds a free-form label
// value: it holds a call to fmt.Sprintf, or a call to a formatting helper
// declared in the same file, at any nesting depth.
func (s *labelScanner) buildsLabelValue(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if found {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isFmtSprintf(call) || s.isFormatHelperCall(call) {
			found = true
			return false
		}
		return true
	})
	return found
}

// isFormatHelperCall reports whether the call reaches a formatting helper
// declared in the same file, by function or by method.
func (s *labelScanner) isFormatHelperCall(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		_, ok := s.formatHelpers[fun.Name]
		return ok
	case *ast.SelectorExpr:
		_, ok := s.formatHelpers[fun.Sel.Name]
		return ok
	}
	return false
}

// containsSprintf reports whether the node builds a value with fmt.Sprintf
// anywhere inside it.
func containsSprintf(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(node ast.Node) bool {
		if found {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isFmtSprintf(call) {
			found = true
			return false
		}
		return true
	})
	return found
}

// isFmtSprintf reports whether the call is a call to fmt.Sprintf.
func isFmtSprintf(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Sprintf" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "fmt"
}

// labelLiteralKind names the kind of a label-map literal for a violation
// message.
func labelLiteralKind(lit *ast.CompositeLit) string {
	if isPrometheusLabels(lit.Type) {
		return "prometheus.Labels"
	}
	return "map[string]string"
}

// isLabelMapType reports whether a type is a label map: map[string]string
// or prometheus.Labels.
func isLabelMapType(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.MapType:
		return isStringIdent(typed.Key) && isStringIdent(typed.Value)
	case *ast.SelectorExpr:
		return isPrometheusLabels(typed)
	}
	return false
}

// isLabelMapExpr reports whether an expression is a label-map literal.
func isLabelMapExpr(expr ast.Expr) bool {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	return isLabelMapType(lit.Type)
}

// isPrometheusLabels reports whether an expression names the
// prometheus.Labels type.
func isPrometheusLabels(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Labels" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "prometheus"
}

// isStringIdent reports whether an expression is the string type.
func isStringIdent(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "string"
}

// isMetricHelper reports whether a source file is a metric registration
// helper: a file that builds metric families or label sets.
func isMetricHelper(src []byte) bool {
	text := string(src)
	for _, marker := range metricHelperMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// grepLabelViolations is the CI grep half of the guard: it rejects a
// metric registration helper line that pairs a label-map literal with
// fmt.Sprintf. The gate is deliberately blunt and line-scoped; the AST
// half is the precise one.
func grepLabelViolations(path string, src []byte) []labelViolation {
	var violations []labelViolation
	for i, line := range strings.Split(string(src), "\n") {
		reason := ""
		switch {
		case strings.Contains(line, "WithLabelValues(fmt.Sprintf"):
			reason = "fmt.Sprintf label value passed to WithLabelValues"
		case strings.Contains(line, "prometheus.Labels{") && strings.Contains(line, "fmt.Sprintf"):
			reason = "prometheus.Labels literal paired with fmt.Sprintf on one line"
		case strings.Contains(line, "map[string]string{") && strings.Contains(line, "fmt.Sprintf"):
			reason = "map[string]string literal paired with fmt.Sprintf on one line"
		}
		if reason == "" {
			continue
		}
		violations = append(violations, labelViolation{
			half:   "grep",
			file:   path,
			line:   i + 1,
			reason: fmt.Sprintf("%s: %s", reason, strings.TrimSpace(line)),
		})
	}
	return violations
}

// moduleRoot returns the nearest ancestor of the working directory that
// holds a go.mod file, or "" when there is none.
func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// displayPath renders a source path relative to the module root when the
// file lives inside it, and absolute otherwise (a synthetic fixture in a
// temporary directory, for instance).
func displayPath(path, root string) string {
	if root != "" {
		if rel, err := filepath.Rel(root, path); err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.ToSlash(rel)
		}
	}
	return path
}

// TestNoFreeFormLabelValues is the no-free-form-label assertion (§11.3):
// no label value in the connector, adapter, discovery, or controller metric
// packages may be built from fmt.Sprintf. Both halves of the guard must
// come back clean.
func TestNoFreeFormLabelValues(t *testing.T) {
	result, err := scanLabelSourcesDetailed(labelScanRoots)
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	t.Logf("label guard: %d files scanned, %d AST-walked, %d metric registration helpers grepped",
		result.files, result.astFiles, result.grepFiles)
	if result.files == 0 {
		t.Fatal("label guard scanned no files: the roots do not resolve")
	}
	if result.astFiles != result.files {
		t.Fatalf("label guard AST-walked %d of %d files", result.astFiles, result.files)
	}
	if result.grepFiles == 0 {
		t.Fatal("label guard found no metric registration helper to grep")
	}
	for _, violation := range result.violations {
		t.Errorf("free-form label value: %s", violation)
	}
}

// TestFreeFormLabelGuardDetectsViolation proves the guard can fail: a
// synthetic file that builds label values with fmt.Sprintf must be
// reported, both in the WithLabelValues argument and in the
// prometheus.Labels literal. A guard that cannot fail proves nothing.
func TestFreeFormLabelGuardDetectsViolation(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

func emit(vec *prometheus.CounterVec, name string) {
	vec.WithLabelValues(fmt.Sprintf("%s", name)).Inc()
	labels := prometheus.Labels{"connector": fmt.Sprintf("%s", name)}
	_ = labels
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("guard reported no violation for a file that builds label values with fmt.Sprintf")
	}
	var sawWithLabelValues, sawLabelsLiteral bool
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		if violation.half != "ast" {
			continue
		}
		switch {
		case strings.Contains(violation.reason, "WithLabelValues"):
			sawWithLabelValues = true
		case strings.Contains(violation.reason, "prometheus.Labels"):
			sawLabelsLiteral = true
		}
	}
	if !sawWithLabelValues {
		t.Error("guard missed the WithLabelValues(fmt.Sprintf(...)) violation")
	}
	if !sawLabelsLiteral {
		t.Error("guard missed the prometheus.Labels{\"connector\": fmt.Sprintf(...)} violation")
	}
}

// TestFreeFormLabelGrepDetectsSingleLinePairing proves the CI grep half
// works on its own: a metric registration helper line that pairs a
// prometheus.Labels literal with fmt.Sprintf is rejected even where the
// AST half cannot attribute the fmt.Sprintf to a label value — here the
// line builds the label set and the formatted string side by side, so only
// the grep half sees the pairing. The fixture keeps the label literal's
// own key and value free of formatting on purpose: the AST half checks
// both positions, so a formatted key there would be attributed by both
// halves and would prove nothing about the grep half's independence.
func TestFreeFormLabelGrepDetectsSingleLinePairing(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

func emit(key string) {
	labels, summary := prometheus.Labels{"connector": key}, fmt.Sprintf("%s", key)
	_, _ = labels, summary
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	var grepHits, astHits int
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		switch violation.half {
		case "grep":
			grepHits++
		case "ast":
			astHits++
		}
	}
	if grepHits == 0 {
		t.Fatal("CI grep half did not flag a line pairing prometheus.Labels{ with fmt.Sprintf")
	}
	if astHits != 0 {
		t.Errorf("AST half flagged %d violations in the grep fixture: the two halves must stay independent", astHits)
	}
}

// TestFreeFormLabelGuardDetectsStringMapLiteral proves the AST half checks
// every label-map literal, not only prometheus.Labels: a map[string]string
// literal that formats a label value is a violation too. A guard that only
// knows the prometheus.Labels spelling misses the same mistake written the
// other way.
func TestFreeFormLabelGuardDetectsStringMapLiteral(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import "fmt"

func emit(name string) {
	labels := map[string]string{"connector": fmt.Sprintf("dispatch-%s", name)}
	_ = labels
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	var astHits int
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		if violation.half == "ast" {
			astHits++
		}
	}
	if astHits == 0 {
		t.Fatal("guard reported no AST violation for a map[string]string label literal built with fmt.Sprintf")
	}
}

// TestFreeFormLabelGuardDetectsFormattedLabelKey proves the AST half checks
// the key position of a label-map literal, not only the value: a formatted
// key is as unbounded as a formatted value. The literal is written across
// several lines so the CI grep half cannot be what catches it — no single
// line holds both the literal and the format — which proves the AST half
// reaches the key on its own.
func TestFreeFormLabelGuardDetectsFormattedLabelKey(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

func emit(key string) {
	labels := prometheus.Labels{
		fmt.Sprintf("x-%s", key): "value",
	}
	_ = labels
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	var grepHits, astHits int
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		switch violation.half {
		case "grep":
			grepHits++
		case "ast":
			astHits++
		}
	}
	if astHits == 0 {
		t.Fatal("guard reported no AST violation for a formatted label key in a prometheus.Labels literal")
	}
	if grepHits != 0 {
		t.Errorf("CI grep half flagged %d violations in the multi-line key fixture: the AST half must be what catches it", grepHits)
	}
}

// TestFreeFormLabelGuardDetectsHelperIndirection proves the AST half sees
// through a same-file formatting helper: a label position fed by a helper
// whose body formats is a violation even though the format never appears
// at the label position itself.
func TestFreeFormLabelGuardDetectsHelperIndirection(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

func helper(x string) string {
	return fmt.Sprintf("x-%s", x)
}

func emit(vec *prometheus.CounterVec, x string) {
	vec.WithLabelValues(helper(x)).Inc()
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	var astHits int
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		if violation.half == "ast" {
			astHits++
		}
	}
	if astHits == 0 {
		t.Fatal("guard reported no AST violation for a label value built by a same-file formatting helper")
	}
}

// TestFreeFormLabelGrepDetectsStringMapPairing proves the CI grep half
// stands on its own for the map[string]string literal: a single line that
// pairs the literal with fmt.Sprintf is rejected by the grep half even
// though the AST half reports the same construction. The half must not
// depend on the other half having run.
func TestFreeFormLabelGrepDetectsStringMapPairing(t *testing.T) {
	dir := t.TempDir()
	source := `package sample

import "fmt"

func emit(name string) {
	labels := map[string]string{"connector": fmt.Sprintf("dispatch-%s", name)}
	_ = labels
}
`
	path := filepath.Join(dir, "violation.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	violations, err := scanLabelSources([]string{dir})
	if err != nil {
		t.Fatalf("label guard: %v", err)
	}
	var grepHits, astHits int
	for _, violation := range violations {
		t.Logf("detected: %s", violation)
		switch violation.half {
		case "grep":
			grepHits++
		case "ast":
			astHits++
		}
	}
	if grepHits == 0 {
		t.Fatal("CI grep half did not flag a line pairing map[string]string{ with fmt.Sprintf")
	}
	if astHits == 0 {
		t.Fatal("AST half did not flag a line pairing map[string]string{ with fmt.Sprintf")
	}
}
