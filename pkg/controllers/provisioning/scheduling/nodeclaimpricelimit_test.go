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
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func pricedInstanceType(name string, offerings ...cloudprovider.Offering) *cloudprovider.InstanceType {
	return fake.NewInstanceType(name, fake.WithOfferings(offerings...))
}

func offering(capacityType string, price float64, available bool) cloudprovider.Offering {
	return cloudprovider.Offering{
		Available: available,
		Price:     price,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  capacityType,
			corev1.LabelTopologyZone: "zone-1",
		}),
	}
}

func instanceTypeNames(its []*cloudprovider.InstanceType) []string {
	names := make([]string, 0, len(its))
	for _, it := range its {
		names = append(names, it.Name)
	}
	return names
}

func TestInstanceTypesBelowPrice(t *testing.T) {
	large := pricedInstanceType("large", offering(v1.CapacityTypeSpot, 6.75, true))
	medium := pricedInstanceType("medium", offering(v1.CapacityTypeSpot, 2.84, true), offering(v1.CapacityTypeOnDemand, 7.79, true))
	unavailableCheap := pricedInstanceType("unavailable-cheap", offering(v1.CapacityTypeSpot, 0.27, false), offering(v1.CapacityTypeOnDemand, 9.0, true))
	unpriced := pricedInstanceType("unpriced")
	unpriced.Offerings = nil
	its := []*cloudprovider.InstanceType{large, medium, unavailableCheap, unpriced}

	for _, tc := range []struct {
		name  string
		limit float64
		want  []string
	}{
		{name: "no limit keeps everything", limit: 0, want: []string{"large", "medium", "unavailable-cheap", "unpriced"}},
		{name: "negative limit keeps everything", limit: -1, want: []string{"large", "medium", "unavailable-cheap", "unpriced"}},
		// the candidate's own type prices itself out, an available cheaper offering keeps medium, and the
		// cheap offering of unavailable-cheap can't launch so only its 9.0 on-demand offering counts
		{name: "candidate price drops its own type", limit: 6.75, want: []string{"medium", "unpriced"}},
		{name: "limit below every offering keeps only the unpriced type", limit: 0.1, want: []string{"unpriced"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := instanceTypeNames(instanceTypesBelowPrice(its, tc.limit))
			if len(got) != len(tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("expected %v, got %v", tc.want, got)
				}
			}
		})
	}
}

// A pass interleaves unlimited simulations with price-limited split retries for the same NodePool, so the two must
// not evict each other or hand each other's templates out.
func TestNodeClaimTemplateCacheSeparatesPriceLimits(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	all := []*cloudprovider.InstanceType{
		pricedInstanceType("large", offering(v1.CapacityTypeSpot, 6.75, true)),
		pricedInstanceType("medium", offering(v1.CapacityTypeSpot, 2.84, true)),
	}
	limited := instanceTypesBelowPrice(all, 6.75)
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})

	calls := 0
	unlimited, _ := nodeClaimTemplateWithCache(ctx, np, all, karpopts.MinValuesPolicyStrict, 0, templateCacheBuild(np, all, &calls))
	capped, _ := nodeClaimTemplateWithCache(ctx, np, limited, karpopts.MinValuesPolicyStrict, 6.75, templateCacheBuild(np, limited, &calls))
	if calls != 2 {
		t.Fatalf("expected a build per price limit, got %d", calls)
	}
	if len(unlimited.InstanceTypeOptions) != 2 || len(capped.InstanceTypeOptions) != 1 {
		t.Fatalf("expected 2 and 1 instance type options, got %d and %d", len(unlimited.InstanceTypeOptions), len(capped.InstanceTypeOptions))
	}

	// both entries survive: a second round of each is served from the cache
	nodeClaimTemplateWithCache(ctx, np, all, karpopts.MinValuesPolicyStrict, 0, templateCacheBuild(np, all, &calls))
	nodeClaimTemplateWithCache(ctx, np, limited, karpopts.MinValuesPolicyStrict, 6.75, templateCacheBuild(np, limited, &calls))
	if calls != 2 {
		t.Fatalf("expected both price limits to hit the cache, got %d builds", calls)
	}
}
