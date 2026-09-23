package daemon

import (
	"testing"

	"github.com/sirrobot01/synod/internal/cluster"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
)

func TestRedisReadinessUsesNativeProber(t *testing.T) {
	if _, ok := probers[string(cluster.Redis)].(redisengine.Engine); !ok {
		t.Fatal("Redis readiness has no native prober")
	}
}
