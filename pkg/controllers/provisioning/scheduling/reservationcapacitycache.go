/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"context"
	"hash/maphash"
	"maps"
	"sync"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

type reservationCapacityCacheContextKey struct{}

// ReservationCapacityCache memoizes the ReservationManager's reserved-offering capacity map for one
// scheduling pass. The capacity map is a pure function of the per-NodePool instance type offerings,
// which are candidate-invariant, so consolidation rebuilding a scheduler per candidate rescans an
// identical offering set every time. The ReservationManager mutates its capacity map while
// reserving, so cache hits hand out a clone rather than the cached map itself. Fingerprinting
// combines NodePool identity (UID, generation) with provider instance type revisions: any NodePool
// without a UID or a revision makes the inputs unfingerprintable and bypasses the cache. The cache
// must not outlive a pass.
type ReservationCapacityCache struct {
	mu          sync.Mutex
	seed        maphash.Seed
	valid       bool
	fingerprint uint64
	capacity    map[string]int
}

func NewReservationCapacityCache() *ReservationCapacityCache {
	return &ReservationCapacityCache{seed: maphash.MakeSeed()}
}

func WithReservationCapacityCache(ctx context.Context, cache *ReservationCapacityCache) context.Context {
	return context.WithValue(ctx, reservationCapacityCacheContextKey{}, cache)
}

func ReservationCapacityCacheFromContext(ctx context.Context) *ReservationCapacityCache {
	cache, _ := ctx.Value(reservationCapacityCacheContextKey{}).(*ReservationCapacityCache)
	return cache
}

// reservationCapacityWithCache returns the reserved-offering capacity map for the given instance
// types, reusing the pass-scoped cached result (cloned, since the caller mutates it) when the
// inputs are content-identical to the previous construction. Without a cache on the context, or
// when any NodePool lacks a provider revision, behavior is identical to buildReservationCapacity.
func reservationCapacityWithCache(ctx context.Context, nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) map[string]int {
	cache := ReservationCapacityCacheFromContext(ctx)
	if cache == nil {
		return buildReservationCapacity(instanceTypes)
	}
	fingerprint, ok := cache.fingerprintInputs(instanceTypeRevisionsFromContext(ctx), nodePools, instanceTypes)
	if !ok {
		ReservationCapacityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return buildReservationCapacity(instanceTypes)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.valid && cache.fingerprint == fingerprint {
		ReservationCapacityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return maps.Clone(cache.capacity)
	}
	ReservationCapacityCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	capacity := buildReservationCapacity(instanceTypes)
	cache.fingerprint = fingerprint
	cache.capacity = maps.Clone(capacity)
	cache.valid = true
	return capacity
}

// fingerprintInputs hashes the per-NodePool instance type identity through provider revisions.
// A provider revision only guarantees identical instance type content while the NodePool's
// (UID, generation) also match, so the fingerprint covers NodePool identity the same way the
// domain group cache does: (UID, generation, instance type count, revision) per pool. NodePools
// without a UID or a revision cannot be fingerprinted cheaply, so the whole input set is reported
// unfingerprintable and the caller falls back to a full scan.
func (c *ReservationCapacityCache) fingerprintInputs(revisions map[string]uint64, nodePools []*v1.NodePool, instanceTypes map[string][]*cloudprovider.InstanceType) (uint64, bool) {
	var h maphash.Hash
	h.SetSeed(c.seed)
	writeUint64(&h, uint64(len(nodePools)))
	for _, np := range nodePools {
		revision, ok := revisions[np.Name]
		if !ok || np.UID == "" {
			return 0, false
		}
		h.WriteString(string(np.UID))
		h.WriteByte(0)
		writeUint64(&h, uint64(np.Generation)) //nolint:gosec
		writeUint64(&h, uint64(len(instanceTypes[np.Name])))
		writeUint64(&h, revision)
	}
	return h.Sum64(), true
}
