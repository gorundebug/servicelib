/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/environment"
)

type JoinValueFunc func(values [][]interface{}) bool

type JoinStorage[K comparable] interface {
	Storage
	JoinValue(ctx context.Context, key K, index int, value interface{}, f JoinValueFunc)
}

type JoinStorageConfig interface {
	GetTTL() time.Duration
	GetRenewTTL() bool
	GetName() string
}

func MakeJoinStorage[K comparable](storageType api.JoinStorageType,
	env environment.ServiceEnvironment,
	cfg JoinStorageConfig) (JoinStorage[K], error) {
	switch storageType {
	case api.JoinStorageTypeHashMap:
		return MakeHashMapJoinStorage[K](env, cfg)
	default:
		return nil, fmt.Errorf("join storage type %d is not supported", storageType)
	}
}
