package runtime

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// These source-level checks complement the cached-attribute allocation test:
// disabled tracing must skip argument evaluation at the real transport call
// site, not only inside a helper or a test double.
func TestEndpointTracingCallSiteContracts(t *testing.T) {
	for _, direction := range []string{"datasource", "datasink"} {
		operationSuffix := "input"
		localPackage := "localsource"
		if direction == "datasink" {
			operationSuffix = "output"
			localPackage = "localsink"
		}
		cases := []struct{ path, operation string }{
			{"http/nethttp.go", "http." + operationSuffix},
			{"kafka/sarama.go", "kafka." + operationSuffix},
			{localPackage + "/custom.go", "local." + operationSuffix},
			{"temporal/temporal.go", "temporal." + operationSuffix},
		}
		for _, mode := range []string{"nostreaming", "clientstreaming", "serverstreaming", "bidistreaming"} {
			cases = append(cases, struct{ path, operation string }{"grpc/" + mode + ".go", "grpc." + operationSuffix})
		}
		for _, testCase := range cases {
			t.Run(direction+"/"+testCase.path, func(t *testing.T) {
				fileSet := token.NewFileSet()
				file, err := parser.ParseFile(fileSet, filepath.Join("..", direction, testCase.path), nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				starts, initializations := 0, 0
				for _, declaration := range file.Decls {
					function, ok := declaration.(*ast.FuncDecl)
					if !ok || function.Body == nil {
						continue
					}
					ast.Inspect(function.Body, func(node ast.Node) bool {
						if call, ok := node.(*ast.CallExpr); ok {
							if receiver := endpointTraceReceiver(call.Fun); receiver != "" {
								starts++
								checkEndpointTraceCall(t, fileSet, function, call, receiver, testCase.operation)
							}
						}
						assignment, ok := node.(*ast.AssignStmt)
						if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) != 1 {
							return true
						}
						call, ok := assignment.Rhs[0].(*ast.CallExpr)
						if !ok || traceExpression(fileSet, call.Fun) != "runtime.MakeEndpointSpanAttributes" {
							return true
						}
						initializations++
						if function.Recv != nil {
							t.Error("static endpoint attributes must be prepared in the constructor, not a request method")
						}
						field, ok := assignment.Lhs[0].(*ast.SelectorExpr)
						if !ok || field.Sel.Name != "spanAttributes" {
							t.Fatal("static attributes must be stored in the consumer cache")
						}
						receiver := traceExpression(fileSet, field.X)
						if !hasEndpointTraceGuard(fileSet, function.Body, assignment.Pos(), receiver, "") {
							t.Error("disabled tracer must not prepare static endpoint attributes")
						}
						return true
					})
				}
				if starts != 1 || initializations != 1 {
					t.Fatalf("expected one guarded span start and cache initialization, got %d / %d", starts, initializations)
				}
			})
		}
	}
}

func endpointTraceReceiver(expression ast.Expr) string {
	start, ok := expression.(*ast.SelectorExpr)
	if !ok || start.Sel.Name != "Start" {
		return ""
	}
	tracer, ok := start.X.(*ast.SelectorExpr)
	if !ok || tracer.Sel.Name != "tracer" {
		return ""
	}
	receiver, ok := tracer.X.(*ast.Ident)
	if !ok {
		return ""
	}
	return receiver.Name
}

func checkEndpointTraceCall(t *testing.T, fileSet *token.FileSet, function *ast.FuncDecl, call *ast.CallExpr, receiver, operation string) {
	t.Helper()
	if function.Recv == nil || len(call.Args) != 3 || !call.Ellipsis.IsValid() {
		t.Fatal("request span must consume a prepared attribute slice")
	}
	context := traceExpression(fileSet, call.Args[0])
	if !hasEndpointTraceGuard(fileSet, function.Body, call.Pos(), receiver, context) {
		t.Fatal("span start is missing its tracer and live sampling call-site guard")
	}
	if traceExpression(fileSet, call.Args[1]) != fmt.Sprintf("%q", operation) {
		t.Fatal("transport operation name changed")
	}
	if operation != "http.input" {
		if traceExpression(fileSet, call.Args[2]) != receiver+".spanAttributes[:]" {
			t.Fatal("static attributes must be passed from the consumer cache without request-time getters")
		}
		return
	}
	// HTTP method/path belong to the individual request and must not be cached
	// on the shared consumer, nor added after Start (samplers may inspect them).
	if traceExpression(fileSet, call.Args[2]) != "attributes[:]" {
		t.Fatal("HTTP input must use a request-local attribute array")
	}
	prepared := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Pos() >= call.Pos() || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 || traceExpression(fileSet, assignment.Lhs[0]) != "attributes" {
			return true
		}
		array, ok := assignment.Rhs[0].(*ast.CompositeLit)
		if !ok || len(array.Elts) != 6 || traceExpression(fileSet, array.Type) != "[6]tracing.Attribute" {
			t.Fatal("HTTP request attributes must contain the four cached labels and two dynamic labels")
		}
		for index := 0; index < 4; index++ {
			if traceExpression(fileSet, array.Elts[index]) != fmt.Sprintf("%s.spanAttributes[%d]", receiver, index) {
				t.Fatal("HTTP static attribute was rebuilt instead of copied from the cache")
			}
		}
		if traceExpression(fileSet, array.Elts[4]) != `tracing.StringAttr("method", r.Method)` || traceExpression(fileSet, array.Elts[5]) != `tracing.StringAttr("path", r.URL.Path)` {
			t.Fatal("HTTP method/path must be taken from the current request")
		}
		if !hasEndpointTraceGuard(fileSet, function.Body, assignment.Pos(), receiver, context) {
			t.Fatal("disabled tracing must not construct HTTP request attributes")
		}
		prepared = true
		return true
	})
	if !prepared {
		t.Fatal("missing guarded HTTP request attribute preparation")
	}
}

func hasEndpointTraceGuard(fileSet *token.FileSet, body *ast.BlockStmt, position token.Pos, receiver, context string) bool {
	guarded := false
	ast.Inspect(body, func(node ast.Node) bool {
		condition, ok := node.(*ast.IfStmt)
		if !ok || position <= condition.Body.Pos() || position >= condition.Body.End() {
			return true
		}
		if hasTraceConjunct(fileSet, condition.Cond, receiver+".tracer != nil") &&
			(context == "" || hasTraceConjunct(fileSet, condition.Cond, "tracing.SamplingEnabled("+context+")")) {
			guarded = true
		}
		return true
	})
	return guarded
}

func hasTraceConjunct(fileSet *token.FileSet, expression ast.Expr, expected string) bool {
	if grouped, ok := expression.(*ast.ParenExpr); ok {
		return hasTraceConjunct(fileSet, grouped.X, expected)
	}
	if binary, ok := expression.(*ast.BinaryExpr); ok && binary.Op == token.LAND {
		return hasTraceConjunct(fileSet, binary.X, expected) || hasTraceConjunct(fileSet, binary.Y, expected)
	}
	return traceExpression(fileSet, expression) == expected
}

func traceExpression(fileSet *token.FileSet, expression ast.Expr) string {
	var output bytes.Buffer
	if err := format.Node(&output, fileSet, expression); err != nil {
		return "<invalid expression>"
	}
	return output.String()
}
