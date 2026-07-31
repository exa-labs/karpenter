//go:build test_performance

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
	"fmt"
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

func BenchmarkDomainGroupsNoCache(b *testing.B) {
	benchmarkDomainGroups(b, false)
}

func BenchmarkDomainGroupsCache(b *testing.B) {
	benchmarkDomainGroups(b, true)
}

// benchmarkDomainGroups approximates a production-shaped input: several NodePools, each resolving
// hundreds of instance types with multi-zone spot and on-demand offerings.
func benchmarkDomainGroups(b *testing.B, useCache bool) {
	zones := []string{"zone-1", "zone-2", "zone-3", "zone-4"}
	nodePools := make([]*v1.NodePool, 8)
	instanceTypes := map[string][]*cloudprovider.InstanceType{}
	for i := range nodePools {
		name := fmt.Sprintf("pool-%d", i)
		np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
		np.UID = types.UID(fmt.Sprintf("uid-%d", i))
		np.Generation = 1
		nodePools[i] = np
		its := make([]*cloudprovider.InstanceType, 200)
		for j := range its {
			var offerings []cloudprovider.Offering
			for _, zone := range zones {
				for _, capacityType := range []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand} {
					offerings = append(offerings, cloudprovider.Offering{
						Available: true,
						Price:     1.0,
						Requirements: scheduling.NewLabelRequirements(map[string]string{
							v1.CapacityTypeLabelKey:  capacityType,
							corev1.LabelTopologyZone: zone,
						}),
					})
				}
			}
			its[j] = fake.NewInstanceType(fmt.Sprintf("it-%d-%d", i, j), fake.WithOfferings(offerings...))
		}
		instanceTypes[name] = its
	}

	ctx := context.Background()
	if useCache {
		ctx = WithDomainGroupCache(ctx, NewDomainGroupCache())
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		domainGroupsWithCache(ctx, nodePools, instanceTypes)
	}
}
