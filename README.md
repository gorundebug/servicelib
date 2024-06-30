# Service Architect runtime library for GoLang

bazel mod tidy
bazel build //...
bazel run //:gazelle
bazel build //:gazelle -- update-repos -from_file=go.mod -to_macro=deps.bzl%go_dependencies
bazel clean --expunge

git tag v0.0.2
git push origin --tags
