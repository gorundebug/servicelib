/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/environment"
	"time"
)

type JoinValueFunc func(values [][]interface{}) bool

type JoinStorage[K comparable] interface {
	Storage
	JoinValue(key K, index int, value interface{}, f JoinValueFunc)
}

type JoinStorageConfig interface {
	GetTTL() time.Duration
	GetRenewTTL() bool
	GetName() string
}

func MakeJoinStorage[K comparable](storageType api.JoinStorageType, env environment.ServiceEnvironment, cfg JoinStorageConfig) JoinStorage[K] {
	switch storageType {
	case api.HashMap:
		return MakeHashMapJoinStorage[K](env, cfg)
	default:
		env.Log().Fatalf("Join storage type %d is not supported", storageType)
		return nil
	}
}
