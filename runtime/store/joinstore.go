/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/telemetry/metrics"
	log "github.com/sirupsen/logrus"
	"time"
)

type JoinValueFunc func(values [][]interface{}) bool

type JoinStorage[K comparable] interface {
	Storage
	JoinValue(key K, index int, value interface{}, f JoinValueFunc)
}

func MakeJoinStorage[K comparable](m metrics.Metrics, storageType api.JoinStorageType, ttl time.Duration, renewTTL bool, streamName string) JoinStorage[K] {
	switch storageType {
	case api.HashMap:
		return MakeHashMapJoinStorage[K](m, ttl, renewTTL, streamName)
	default:
		log.Fatalf("Join storage type %d is not supported", storageType)
		return nil
	}
}
