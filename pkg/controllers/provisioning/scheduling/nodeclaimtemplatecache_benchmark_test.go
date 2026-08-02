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

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
)

// BenchmarkNodeClaimTemplateConstruction compares the full template construction + instance type
// pre-filter against a cache hit, at a realistic instance type count.
func BenchmarkNodeClaimTemplateConstruction(b *testing.B) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	var names []string
	for i := 0; i < 1600; i++ {
		names = append(names, fmt.Sprintf("it-%d", i))
	}
	its := templateCacheInstanceTypes(names...)

	build := func() *NodeClaimTemplate {
		nct := NewNodeClaimTemplate(np)
		nct.InstanceTypeOptions, _, _ = filterInstanceTypesByRequirements(its, nct.Requirements, &corev1.Pod{}, corev1.ResourceList{}, []DaemonOverheadGroup{{InstanceTypes: its, HostPortUsage: nil}}, corev1.ResourceList{}, false)
		return nct
	}

	b.Run("uncached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if nct := build(); nct == nil {
				b.Fatal("expected a template")
			}
		}
	})

	b.Run("cached", func(b *testing.B) {
		ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
		ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})
		nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, build) // warm
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if nct, ok := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, build); !ok || nct == nil {
				b.Fatal("expected a template")
			}
		}
	})
}
