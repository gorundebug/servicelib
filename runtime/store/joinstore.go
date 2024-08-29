/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/api"
	"time"
)

type JoinValueFunc func(values [][]interface{}) bool

type JoinStorage[K comparable] interface {
	JoinValue(key K, index int, value interface{}, f JoinValueFunc)
}

func MakeJoinStorage[K comparable](storageType api.JoinStorageType, ttl time.Duration) JoinStorage[K] {
	switch storageType {
	case api.HashMap:
		return MakeHashMapJoinStorage[K](ttl)
	default:
		log.Fatalf("Join storage type %d is not supported", storageType)
		return nil
	}
}
