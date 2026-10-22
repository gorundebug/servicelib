package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
)

// Only the accessor is under test here; engine creation is exercised by service
// startup tests. Embedding preserves the dependency contract without fake engines.
type capabilityDependencies struct {
	environment.ServiceDependencies
	capability *int
}

func (d *capabilityDependencies) BusinessCapability() *int { return d.capability }

func TestServiceDependenciesPreservesProvider(t *testing.T) {
	app := &ServiceApp{}
	if got := app.ResolvedDependencies(); got != nil {
		t.Fatalf("unconfigured dependencies = %T, want nil", got)
	}
	value := 7
	provider := &capabilityDependencies{capability: &value}
	app.dep = provider
	if got := app.ResolvedDependencies(); got != provider {
		t.Fatalf("dependencies = %T, want original provider", got)
	}
	capability, ok := app.ResolvedDependencies().(interface{ BusinessCapability() *int })
	if !ok || capability.BusinessCapability() != &value {
		t.Fatal("custom maker capability lost through service environment")
	}
	if app.ServiceDependencies() != nil {
		t.Fatal("resolved provider changed fallback hook semantics")
	}
}

var errDependencySelectionComplete = errors.New("dependency selection complete")

// Stop before engine/resource creation, after exercising real initRuntime
// dependency selection. Full service initialization is covered by integration.
func (*capabilityDependencies) LogsEngine(context.Context, environment.ServiceEnvironment) (log.LogsEngine, error) {
	return nil, errDependencySelectionComplete
}

type dependencyHookEnvironment struct {
	RuntimeEnvironment
	provider environment.ServiceDependencies
	calls    int
}

func (e *dependencyHookEnvironment) ServiceDependencies() environment.ServiceDependencies {
	e.calls++
	return e.provider
}

func TestResolvedDependenciesSelection(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		name := "hook"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			cfg, err := config.NewRuntimeConfig(startOrderConfig{service: &config.ServiceConfig{ID: 1, Name: "dependency-test"}})
			if err != nil {
				t.Fatal(err)
			}
			provider := &capabilityDependencies{}
			env := &dependencyHookEnvironment{provider: provider}
			var supplied environment.ServiceDependencies
			if explicit {
				supplied = provider
			}
			app := &ServiceApp{}
			err = app.initRuntime(context.Background(), "dependency-test", env, supplied, nil, cfg)
			if !errors.Is(err, errDependencySelectionComplete) {
				t.Fatalf("unexpected initialization result: %v", err)
			}
			if app.ResolvedDependencies() != provider {
				t.Fatal("selected provider was not retained")
			}
			wantCalls := 1
			if explicit {
				wantCalls = 0
			}
			if env.calls != wantCalls {
				t.Fatalf("hook calls = %d, want %d", env.calls, wantCalls)
			}
			if app.ServiceDependencies() != nil {
				t.Fatal("base fallback hook changed")
			}
		})
	}
}
