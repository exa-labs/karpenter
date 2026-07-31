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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

func domainGroupCacheNodePool(name string, uid string) *v1.NodePool {
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
	np.UID = types.UID(uid)
	np.Generation = 1
	return np
}

func domainGroupCacheInstanceTypes(zones ...string) []*cloudprovider.InstanceType {
	var offerings []cloudprovider.Offering
	for _, zone := range zones {
		offerings = append(offerings, cloudprovider.Offering{
			Available: true,
			Price:     1.0,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeSpot,
				corev1.LabelTopologyZone: zone,
			}),
		})
	}
	return []*cloudprovider.InstanceType{fake.NewInstanceType("it-a", fake.WithOfferings(offerings...))}
}

func TestDomainGroupCacheReturnsEquivalentResultAndReusesOnIdenticalInputs(t *testing.T) {
	nodePools := []*v1.NodePool{domainGroupCacheNodePool("pool", "uid-1")}
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": domainGroupCacheInstanceTypes("zone-1", "zone-2")}

	uncached := buildDomainGroups(nodePools, instanceTypes)

	ctx := WithDomainGroupCache(context.Background(), NewDomainGroupCache())
	first := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if !reflect.DeepEqual(uncached, first) {
		t.Fatalf("cached result differs from uncached result:\nuncached: %#v\ncached: %#v", uncached, first)
	}

	second := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
		t.Fatal("expected identical inputs to reuse the cached domain groups map")
	}
}

func TestDomainGroupCacheRecomputesWhenInputsChange(t *testing.T) {
	nodePools := []*v1.NodePool{domainGroupCacheNodePool("pool", "uid-1")}
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": domainGroupCacheInstanceTypes("zone-1")}

	ctx := WithDomainGroupCache(context.Background(), NewDomainGroupCache())
	first := domainGroupsWithCache(ctx, nodePools, instanceTypes)

	// NodePool generation change (spec change) must invalidate.
	nodePools[0].Generation = 2
	afterGeneration := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() == reflect.ValueOf(afterGeneration).Pointer() {
		t.Fatal("expected nodepool generation change to invalidate the cache")
	}

	// Instance type requirement content change (e.g. a zone disappearing on a provider cache
	// refill) must invalidate even though the nodepool is unchanged.
	instanceTypes["pool"] = domainGroupCacheInstanceTypes("zone-1", "zone-3")
	afterInstanceTypes := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(afterGeneration).Pointer() == reflect.ValueOf(afterInstanceTypes).Pointer() {
		t.Fatal("expected instance type requirement change to invalidate the cache")
	}
	if _, ok := afterInstanceTypes[corev1.LabelTopologyZone]["zone-3"]; !ok {
		t.Fatal("expected recomputed domain groups to contain the new zone")
	}

	// A nodepool being added must invalidate.
	nodePools = append(nodePools, domainGroupCacheNodePool("pool-2", "uid-2"))
	instanceTypes["pool-2"] = domainGroupCacheInstanceTypes("zone-4")
	afterNodePoolAdd := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(afterInstanceTypes).Pointer() == reflect.ValueOf(afterNodePoolAdd).Pointer() {
		t.Fatal("expected nodepool addition to invalidate the cache")
	}
	if !reflect.DeepEqual(afterNodePoolAdd, buildDomainGroups(nodePools, instanceTypes)) {
		t.Fatal("expected recomputed domain groups to match an uncached build")
	}
}

func TestDomainGroupCacheRevisionFastPath(t *testing.T) {
	nodePools := []*v1.NodePool{domainGroupCacheNodePool("pool", "uid-1")}
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": domainGroupCacheInstanceTypes("zone-1")}

	ctx := WithDomainGroupCache(context.Background(), NewDomainGroupCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})
	first := domainGroupsWithCache(ctx, nodePools, instanceTypes)

	// Same revision: reuse without consulting requirement content. The provider guarantees content
	// is identical for the same (UID, generation, revision), so a same-length content change here
	// simulates what the revision contract makes unobservable.
	instanceTypes["pool"] = domainGroupCacheInstanceTypes("zone-9")
	sameRevision := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() != reflect.ValueOf(sameRevision).Pointer() {
		t.Fatal("expected an unchanged revision to reuse the cached domain groups map")
	}

	// Revision bump (provider cache refill) must invalidate even though UID/generation are unchanged.
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 8})
	afterRevision := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() == reflect.ValueOf(afterRevision).Pointer() {
		t.Fatal("expected a revision change to invalidate the cache")
	}
	if _, ok := afterRevision[corev1.LabelTopologyZone]["zone-9"]; !ok {
		t.Fatal("expected recomputed domain groups to contain the new zone")
	}

	// Generation change must still invalidate with a constant revision.
	nodePools[0].Generation = 2
	afterGeneration := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(afterRevision).Pointer() == reflect.ValueOf(afterGeneration).Pointer() {
		t.Fatal("expected nodepool generation change to invalidate the cache")
	}
}

func TestDomainGroupCacheMixedRevisionsFallBackToContentHashingPerPool(t *testing.T) {
	nodePools := []*v1.NodePool{
		domainGroupCacheNodePool("pool", "uid-1"),
		domainGroupCacheNodePool("pool-2", "uid-2"),
	}
	instanceTypes := map[string][]*cloudprovider.InstanceType{
		"pool":   domainGroupCacheInstanceTypes("zone-1"),
		"pool-2": domainGroupCacheInstanceTypes("zone-2"),
	}

	// Only pool has a revision; pool-2 must still be protected by content hashing.
	ctx := WithDomainGroupCache(context.Background(), NewDomainGroupCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 3})
	first := domainGroupsWithCache(ctx, nodePools, instanceTypes)

	instanceTypes["pool-2"] = domainGroupCacheInstanceTypes("zone-2", "zone-3")
	afterContentChange := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() == reflect.ValueOf(afterContentChange).Pointer() {
		t.Fatal("expected a content change on a revisionless pool to invalidate the cache")
	}
	if !reflect.DeepEqual(afterContentChange, buildDomainGroups(nodePools, instanceTypes)) {
		t.Fatal("expected recomputed domain groups to match an uncached build")
	}
}

func TestDomainGroupCacheBypassesUnkeyableInputs(t *testing.T) {
	nodePools := []*v1.NodePool{domainGroupCacheNodePool("pool", "")} // no UID
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": domainGroupCacheInstanceTypes("zone-1")}

	ctx := WithDomainGroupCache(context.Background(), NewDomainGroupCache())
	first := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	second := domainGroupsWithCache(ctx, nodePools, instanceTypes)
	if reflect.ValueOf(first).Pointer() == reflect.ValueOf(second).Pointer() {
		t.Fatal("expected unkeyable inputs to bypass the cache and rebuild")
	}
	if !reflect.DeepEqual(first, buildDomainGroups(nodePools, instanceTypes)) {
		t.Fatal("expected bypass path to match an uncached build")
	}
}

func TestDomainGroupCacheWithoutContextMatchesUncachedBuild(t *testing.T) {
	nodePools := []*v1.NodePool{domainGroupCacheNodePool("pool", "uid-1")}
	instanceTypes := map[string][]*cloudprovider.InstanceType{"pool": domainGroupCacheInstanceTypes("zone-1")}

	got := domainGroupsWithCache(context.Background(), nodePools, instanceTypes)
	if !reflect.DeepEqual(got, buildDomainGroups(nodePools, instanceTypes)) {
		t.Fatal("expected no-cache path to match an uncached build")
	}
}

func TestNewReservationManagerMatchesCapacityTypeSemantics(t *testing.T) {
	reserved := cloudprovider.Offering{
		Available:           true,
		Price:               1.0,
		ReservationCapacity: 3,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
			corev1.LabelTopologyZone:         "zone-1",
			cloudprovider.ReservationIDLabel: "cr-1",
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
	rm := NewReservationManager(map[string][]*cloudprovider.InstanceType{
		"pool": {fake.NewInstanceType("it-a", fake.WithOfferings(reserved, spot))},
	})
	if !rm.CanReserve("host-1", &reserved) {
		t.Fatal("expected reserved offering to be reservable")
	}
	if got := rm.RemainingCapacity(&reserved); got != 3 {
		t.Fatalf("expected reserved capacity 3, got %d", got)
	}
	if got := rm.RemainingCapacity(&spot); got != 0 {
		t.Fatalf("expected spot offering to have no reserved capacity, got %d", got)
	}
}
