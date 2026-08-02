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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func reservedInstanceTypes(reservationID string, capacity int) []*cloudprovider.InstanceType {
	reserved := cloudprovider.Offering{
		Available:           true,
		Price:               1.0,
		ReservationCapacity: capacity,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
			corev1.LabelTopologyZone:         "zone-1",
			cloudprovider.ReservationIDLabel: reservationID,
		}),
	}
	spot := cloudprovider.Offering{
		Available: true,
		Price:     1.0,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeSpot,
			corev1.LabelTopologyZone: "zone-1",
		}),
	}
	return []*cloudprovider.InstanceType{fake.NewInstanceType("it-a", fake.WithOfferings(reserved, spot))}
}

func reservationCacheNodePools(names ...string) []*v1.NodePool {
	var nodePools []*v1.NodePool
	for _, name := range names {
		nodePools = append(nodePools, domainGroupCacheNodePool(name, "uid-"+name))
	}
	return nodePools
}

func TestReservationCapacityCacheReturnsEquivalentResultOnHit(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	uncached := buildReservationCapacity(instanceTypes)

	ctx := WithReservationCapacityCache(context.Background(), NewReservationCapacityCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})
	nodePools := reservationCacheNodePools("pool")
	first := reservationCapacityWithCache(ctx, nodePools, instanceTypes)
	if !reflect.DeepEqual(uncached, first) {
		t.Fatalf("cached result differs from uncached result:\nuncached: %#v\ncached: %#v", uncached, first)
	}
	second := reservationCapacityWithCache(ctx, nodePools, instanceTypes)
	if !reflect.DeepEqual(uncached, second) {
		t.Fatalf("cache hit result differs from uncached result:\nuncached: %#v\ncached: %#v", uncached, second)
	}
}

func TestReservationCapacityCacheHandsOutIndependentClones(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	ctx := WithReservationCapacityCache(context.Background(), NewReservationCapacityCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})
	nodePools := reservationCacheNodePools("pool")

	first := reservationCapacityWithCache(ctx, nodePools, instanceTypes)
	first["cr-1"] = 0 // simulate the ReservationManager consuming the capacity

	second := reservationCapacityWithCache(ctx, nodePools, instanceTypes)
	if second["cr-1"] != 3 {
		t.Fatalf("expected mutation of a previous result to not leak into subsequent results, got capacity %d", second["cr-1"])
	}
}

func TestReservationCapacityCacheRecomputesOnRevisionChange(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)

	nodePools := reservationCacheNodePools("pool")
	first := reservationCapacityWithCache(WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1}), nodePools, instanceTypes)
	if first["cr-1"] != 3 {
		t.Fatalf("expected capacity 3, got %d", first["cr-1"])
	}

	updated := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 5)}
	second := reservationCapacityWithCache(WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 2}), nodePools, updated)
	if second["cr-1"] != 5 {
		t.Fatalf("expected revision change to recompute capacity to 5, got %d", second["cr-1"])
	}
}

func TestReservationCapacityCacheBypassesWithoutRevisions(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)

	result := reservationCapacityWithCache(ctx, reservationCacheNodePools("pool"), instanceTypes)
	if result["cr-1"] != 3 {
		t.Fatalf("expected capacity 3, got %d", result["cr-1"])
	}
	if cache.valid {
		t.Fatal("expected cache to remain unpopulated when inputs cannot be fingerprinted")
	}
}

func TestReservationCapacityCacheBypassesWithPartialRevisions(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{
		"pool-a": reservedInstanceTypes("cr-1", 3),
		"pool-b": reservedInstanceTypes("cr-2", 2),
	}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool-a": 1})

	result := reservationCapacityWithCache(ctx, reservationCacheNodePools("pool-a", "pool-b"), instanceTypes)
	if result["cr-1"] != 3 || result["cr-2"] != 2 {
		t.Fatalf("expected bypass to compute full capacity, got %#v", result)
	}
	if cache.valid {
		t.Fatal("expected cache to remain unpopulated when any NodePool lacks a revision")
	}
}

func TestReservationCapacityCacheWithoutCacheMatchesDirectConstruction(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	uncached := buildReservationCapacity(instanceTypes)
	result := reservationCapacityWithCache(context.Background(), reservationCacheNodePools("pool"), instanceTypes)
	if !reflect.DeepEqual(uncached, result) {
		t.Fatalf("expected identical result without a cache on the context, got %#v vs %#v", result, uncached)
	}
}

func TestReservationCapacityCacheRecomputesOnGenerationChange(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})
	nodePools := reservationCacheNodePools("pool")

	first := reservationCapacityWithCache(ctx, nodePools, instanceTypes)
	if first["cr-1"] != 3 {
		t.Fatalf("expected capacity 3, got %d", first["cr-1"])
	}

	// A NodePool edit bumps the generation; the revision alone no longer identifies the content.
	nodePools[0].Generation = 2
	updated := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-2", 5)}
	second := reservationCapacityWithCache(ctx, nodePools, updated)
	if second["cr-2"] != 5 {
		t.Fatalf("expected generation change to recompute capacity, got %#v", second)
	}
	if _, ok := second["cr-1"]; ok {
		t.Fatalf("expected stale reservation id to be absent after recompute, got %#v", second)
	}
}

func TestReservationCapacityCacheRecomputesOnUIDChange(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})

	first := reservationCapacityWithCache(ctx, reservationCacheNodePools("pool"), instanceTypes)
	if first["cr-1"] != 3 {
		t.Fatalf("expected capacity 3, got %d", first["cr-1"])
	}

	// A recreated NodePool has a fresh UID with generation and revision starting over.
	recreated := domainGroupCacheNodePool("pool", "uid-recreated")
	updated := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-2", 5)}
	second := reservationCapacityWithCache(ctx, []*v1.NodePool{recreated}, updated)
	if second["cr-2"] != 5 {
		t.Fatalf("expected UID change to recompute capacity, got %#v", second)
	}
}

func TestReservationCapacityCacheBypassesWithoutNodePoolUID(t *testing.T) {
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": reservedInstanceTypes("cr-1", 3)}
	cache := NewReservationCapacityCache()
	ctx := WithReservationCapacityCache(context.Background(), cache)
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})

	nodePool := domainGroupCacheNodePool("pool", "")
	result := reservationCapacityWithCache(ctx, []*v1.NodePool{nodePool}, instanceTypes)
	if result["cr-1"] != 3 {
		t.Fatalf("expected capacity 3, got %d", result["cr-1"])
	}
	if cache.valid {
		t.Fatal("expected cache to remain unpopulated when a NodePool lacks a UID")
	}
}
