# Pulsar runtime authentication

`config.PulsarDataConnectorConfig.AdminToken string` maps to runtime YAML/mapstructure
`adminToken`. It is REST-only: a nonempty value replaces the admin Bearer credential;
empty preserves fallback to `Token`. Native official-client authentication continues
to use `Token`. HTTP failures never cause fallback to another credential.

A tenant-admin role suffices for mandatory namespace-policy validation and explicit
resource provisioning. Native producer/consumer roles can remain scoped to namespace
or topic ACLs. The process still holds both credentials. No preflight checks or
creation flags are weakened. The initial split supports token/token credentials;
AdminToken plus TLS client identity is explicitly rejected. CA-only TLS remains
supported. CR/LF in AdminToken is rejected. The runtime secret is not projected by
DataConnectorConfigToAPI; topology and generator secret binding are separate.

The isolated cross-language fixture is in sibling
`tsservicelib/test/native-probes/pulsar-auth.test.mjs`. Run there with
`PULSAR_AUTH_TEST=1 PULSAR_AUTH_CROSS_LANGUAGE=1 node --test test/native-probes/pulsar-auth.test.mjs`.
Set PULSAR_TEST_GO to override the Go executable. All 28 harness checks pass,
including Go race acceptance, configuration validation, scoped native roundtrip,
forbidden native operations with valid admin preflight, rejected admin credentials
without fallback, secret-safe diagnostics and bounded close. Only the test-owned
container, namespace resources, secret files and volumes are removed.
