package api

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestDeploymentModelsPreserveExplicitPolicyWithoutAddingDefaults(t *testing.T) {
	const policy = `{"kubernetes":{"terminationGracePeriodSeconds":0,"command":[],"resources":{},"probes":{"readiness":{"enabled":false}},"secretEnv":{"TOKEN":{"name":"existing","key":"token","optional":false}}}}`
	input := `{"settings":{"name":"Example","deployment":` + policy + `},"services":[{"id":42,"deployment":` + policy + `}]}`
	var app StreamApp
	if err := json.Unmarshal([]byte(input), &app); err != nil {
		t.Fatal(err)
	}
	if app.Settings.Deployment == nil || app.Services[0].Deployment == nil {
		t.Fatal("deployment metadata discarded")
	}
	data, err := json.Marshal(app)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	var want any
	if err := json.Unmarshal([]byte(policy), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got["settings"].(map[string]any)["deployment"]) || !reflect.DeepEqual(want, got["services"].([]any)[0].(map[string]any)["deployment"]) {
		t.Fatal("explicit zero/false/empty deployment collections changed")
	}
	var old StreamApp
	if err := json.Unmarshal([]byte(`{"settings":{"name":"Old"},"services":[{"id":1}]}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Settings.Deployment != nil || old.Services[0].Deployment != nil {
		t.Fatal("old document acquired deployment policy")
	}
}

func TestDeploymentPortIsExplicitAndAtomic(t *testing.T) {
	for _, input := range []string{`1`, `65535`, `"http"`, `"9http"`, `"1-2http"`} {
		var port DeploymentPortValue
		if err := json.Unmarshal([]byte(input), &port); err != nil {
			t.Fatal(input, err)
		}
		output, err := json.Marshal(port)
		if err != nil || string(output) != input {
			t.Fatal("port changed", input, string(output), err)
		}
	}
	for _, input := range []string{`0`, `65536`, `1.5`, `null`, `true`, `{}`, `"123"`, `"http--api"`, `"-http"`, `"http-"`, `"UPPER"`, `"port-name-too-long"`} {
		port, err := NewDeploymentPortNumber(80)
		if err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal([]byte(input), &port) == nil {
			t.Fatal("invalid port accepted", input)
		}
		if number, ok := port.Number(); !ok || number != 80 {
			t.Fatal("invalid input changed previous port")
		}
	}
}

func TestDeploymentSchemaHasNoOpaqueObjectEscapeHatch(t *testing.T) {
	var spec map[string]any
	if err := yaml.Unmarshal(Spec(), &spec); err != nil {
		t.Fatal(err)
	}
	schemas := spec["components"].(map[string]any)["schemas"].(map[string]any)
	if _, exists := schemas["KubernetesObject"]; exists {
		t.Fatal("opaque Kubernetes object remains")
	}
	for name, raw := range schemas {
		if !strings.HasPrefix(name, "Deployment") && name != "KubernetesDeploymentSettings" {
			continue
		}
		schema := raw.(map[string]any)
		if schema["type"] == "object" && schema["additionalProperties"] == true {
			t.Fatal("untyped deployment model", name)
		}
	}
}

func TestComplexDeploymentModelsRetainActualDocumentValues(t *testing.T) {
	const input = `{"kubernetes":{
"initContainers":[{"name":"prepare","image":"example/prepare:v1","command":[],"env":[{"name":"TOKEN","valueFrom":{"secretKeyRef":{"name":"existing","key":"token","optional":false}}}],"ports":[{"name":"http","containerPort":8080,"protocol":"TCP"}],"volumeMounts":[{"name":"scratch","mountPath":"/scratch","readOnly":false}],"lifecycle":{"preStop":{"exec":{"command":["/app/drain"]}}}}],
"extraContainers":[{"name":"sidecar","image":"example/sidecar:v1","stdin":false,"tty":false}],
"extraEnv":[{"name":"POD_NAME","valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.name"}}}],
"extraEnvFrom":[{"prefix":"APP_","configMapRef":{"name":"settings","optional":false}}],
"extraVolumes":[{"name":"scratch","emptyDir":{"medium":"","sizeLimit":"1Gi"}},{"name":"data","persistentVolumeClaim":{"claimName":"existing","readOnly":false}},{"name":"token","projected":{"defaultMode":0,"sources":[{"serviceAccountToken":{"audience":"api","expirationSeconds":600,"path":"token"}}]}}],
"extraVolumeMounts":[{"name":"scratch","mountPath":"/scratch","readOnly":false}],
"tolerations":[{"key":"gpu","operator":"Exists","effect":"NoExecute","tolerationSeconds":0}],
"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"accelerator","operator":"In","values":["gpu"]}]}]}},"podAntiAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[{"weight":1,"podAffinityTerm":{"topologyKey":"kubernetes.io/hostname","labelSelector":{"matchLabels":{"app":"worker"}}}}]}},
"probes":{"startup":{"enabled":false,"spec":{}},"readiness":{"spec":{"httpGet":{"path":"/ready","port":"9http","scheme":"HTTP","httpHeaders":[{"name":"X-Probe","value":"ready"}]},"initialDelaySeconds":0,"periodSeconds":5}},"liveness":{"spec":{"grpc":{"port":9090,"service":""}}}}
}}`
	var policy DeploymentSettings
	if err := json.Unmarshal([]byte(input), &policy); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal([]byte(input), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("complex deployment model dropped or invented fields")
	}
}

func TestPartialDeploymentOverridesNeverInventRequiredValues(t *testing.T) {
	const input = `{"kubernetes":{"secretEnv":{"TOKEN":{"optional":false}},"extraEnv":[{"valueFrom":{"secretKeyRef":{"optional":false}}}],"securityContext":{"runAsNonRoot":false},"probes":{"readiness":{"spec":{"httpGet":{"path":"/ready"}}}}}}`
	var policy DeploymentSettings
	if err := json.Unmarshal([]byte(input), &policy); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal([]byte(input), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("partial override acquired missing required fields or defaults")
	}
}

func TestDeploymentIsDeclaredOptionalInClosedAPIRecords(t *testing.T) {
	var spec map[string]any
	if err := yaml.Unmarshal(Spec(), &spec); err != nil {
		t.Fatal(err)
	}
	schemas := spec["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"ProjectSettings", "Service"} {
		record := schemas[name].(map[string]any)
		if record["additionalProperties"] != false {
			t.Fatalf("%s lost its closed contract", name)
		}
		property := record["properties"].(map[string]any)["deployment"].(map[string]any)
		if property["$ref"] != "#/components/schemas/DeploymentSettings" {
			t.Fatalf("%s has no deployment contract", name)
		}
		for _, required := range record["required"].([]any) {
			if required == "deployment" {
				t.Fatal("deployment must remain opt-in")
			}
		}
	}
	kubernetes := schemas["KubernetesDeploymentSettings"].(map[string]any)
	if kubernetes["additionalProperties"] != false {
		t.Fatal("unknown generation policy must not be accepted")
	}
	for name, property := range kubernetes["properties"].(map[string]any) {
		if _, exists := property.(map[string]any)["default"]; exists {
			t.Fatalf("%s invents default policy", name)
		}
	}
}

func TestTypedDeploymentPolicyPreservesExplicitValues(t *testing.T) {
	const input = `{"kubernetes":{"deploymentStrategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":"25%","maxUnavailable":0}},"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":0}},"resources":{"requests":{"cpu":"500m"},"limits":{}},"podLabels":{"team":"media"},"podSecurityContext":{"runAsNonRoot":false,"runAsUser":0,"fsGroup":0,"supplementalGroups":[]},"securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":[]},"seccompProfile":{"type":"RuntimeDefault"}},"serviceAccount":{"name":"worker","automountServiceAccountToken":false}}}`
	var policy DeploymentSettings
	if err := json.Unmarshal([]byte(input), &policy); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal([]byte(input), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("typed deployment policy lost explicit false/zero/empty values")
	}
	for _, invalid := range []string{
		`{"kubernetes":{"podLabels":{"team":false}}}`,
		`{"kubernetes":{"securityContext":{"runAsNonRoot":"true"}}}`,
		`{"kubernetes":{"serviceAccount":{"automountServiceAccountToken":"false"}}}`,
	} {
		var bad DeploymentSettings
		if json.Unmarshal([]byte(invalid), &bad) == nil {
			t.Fatal("wrong typed deployment property accepted")
		}
	}
}

func TestDeploymentCountOrPercentIsAClosedScalarUnion(t *testing.T) {
	for _, encoded := range []string{`0`, `12`, `"0%"`, `"25%"`} {
		var value DeploymentCountOrPercent
		if err := json.Unmarshal([]byte(encoded), &value); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil || string(data) != encoded {
			t.Fatal("count/percentage changed", string(data), err)
		}
	}
	for _, encoded := range []string{`null`, `true`, `{}`, `[]`, `-1`, `1.5`, `"12"`, `"-5%"`, `"25 %"`} {
		value, err := NewDeploymentPercent("50%")
		if err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal([]byte(encoded), &value) == nil {
			t.Fatal("invalid count/percentage accepted", encoded)
		}
		if percent, ok := value.Percent(); !ok || percent != "50%" {
			t.Fatal("invalid input mutated prior scalar")
		}
	}
	if _, err := NewDeploymentCount(-1); err == nil {
		t.Fatal("negative count constructed")
	}
	if _, err := NewDeploymentPercent("10"); err == nil {
		t.Fatal("non-percent string constructed")
	}
}
