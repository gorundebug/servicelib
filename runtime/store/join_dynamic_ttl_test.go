package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type dynamicJoinTTL struct {
	ttl   atomic.Int64
	renew atomic.Bool
}

func (*dynamicJoinTTL) GetName() string          { return "dynamic-ttl" }
func (c *dynamicJoinTTL) GetTTL() time.Duration { return time.Duration(c.ttl.Load()) }
func (c *dynamicJoinTTL) GetRenewTTL() bool      { return c.renew.Load() }

func TestJoinDynamicTTLRetainsAcceptedTimer(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initial, next  time.Duration
		wantExpiry     bool
		minimumElapsed time.Duration
	}{
		{"extend", 300 * time.Millisecond, 500 * time.Millisecond, true, 500 * time.Millisecond},
		{"shorten", 300 * time.Millisecond, 30 * time.Millisecond, true, 300 * time.Millisecond},
		{"disable", 300 * time.Millisecond, 0, true, 300 * time.Millisecond},
		{"enable", 0, 30 * time.Millisecond, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &dynamicJoinTTL{}
			cfg.ttl.Store(int64(tc.initial))
			cfg.renew.Store(tc.initial > 0)
			storage, err := MakeHashMapJoinStorage[string](newMockEnv("dynamic-ttl", newMockMetrics()), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer storage.Stop(context.Background())
			defer storage.JoinValue(context.Background(), "key", 0, 30, func([][]interface{}) bool { return true })
			observed := make(chan time.Time, 8)
			callback := func([][]interface{}) bool { observed <- time.Now(); return false }
			started := time.Now()
			storage.JoinValue(context.Background(), "key", 0, 10, callback)
			<-observed
			cfg.ttl.Store(int64(tc.next))
			cfg.renew.Store(true)
			renewed := time.Now()
			storage.JoinValue(context.Background(), "key", 1, 20, callback)
			<-observed
			if !tc.wantExpiry {
				select {
				case <-observed:
					t.Fatal("renewal installed a timer absent at initial admission")
				case <-time.After(120 * time.Millisecond):
				}
				return
			}
			select {
			case expired := <-observed:
				origin := started
				if tc.name == "extend" {
					origin = renewed
				}
				if expired.Sub(origin) < tc.minimumElapsed {
					t.Fatalf("callback moved ahead of accepted timer: %v < %v", expired.Sub(origin), tc.minimumElapsed)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("renewal lost the accepted expiry callback")
			}
		})
	}
}
