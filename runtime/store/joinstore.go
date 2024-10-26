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

type JoinStorageConfig interface {
	GetJoinStorageType() api.JoinStorageType
	GetTTL() time.Duration
	GetRenewTTL() bool
	GetName() string
	GetServiceName() string
	GetMetrics() metrics.Metrics
}

func MakeJoinStorage[K comparable](cfg JoinStorageConfig) JoinStorage[K] {
	switch cfg.GetJoinStorageType() {
	case api.HashMap:
		return MakeHashMapJoinStorage[K](cfg)
	default:
		log.Fatalf("Join storage type %d is not supported", cfg.GetJoinStorageType())
		return nil
	}
}
