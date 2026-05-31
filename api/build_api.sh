#!/bin/bash
#
# Copyright (c) 2024 Sergey Alexeev
# Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#

go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1
oapi-codegen -generate "std-http,models" -package api -o ./api.generated.go  ./serviceapi.yaml