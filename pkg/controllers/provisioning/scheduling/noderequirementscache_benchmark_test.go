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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// productionShapeNodes mimics a production node: ~20 labels per node, mixing well-known and
// provider-specific keys.
func productionShapeNodes(count int) []*state.StateNode {
	nodes := make([]*state.StateNode, 0, count)
	for i := 0; i < count; i++ {
		labels := map[string]string{}
		for j := 0; j < 20; j++ {
			labels[fmt.Sprintf("label-key-%d.domain.example.com/part", j)] = fmt.Sprintf("value-%d-%d", i%7, j)
		}
		labels[corev1.LabelTopologyZone] = fmt.Sprintf("us-west-2%c", 'a'+rune(i%3))
		nodes = append(nodes, makeStateNode(fmt.Sprintf("uid-%d", i), "100", labels))
	}
	return nodes
}

// BenchmarkNodeLabelRequirements compares recomputing label requirements for every candidate
// (upstream behavior) against the pass-scoped cache. Shape: 1000 nodes, 100 candidates per pass.
func BenchmarkNodeLabelRequirements(b *testing.B) {
	nodes := productionShapeNodes(1000)
	const candidatesPerPass = 100

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for c := 0; c < candidatesPerPass; c++ {
				for _, n := range nodes {
					_ = labelRequirementsForStateNode(nil, n)
				}
			}
		}
	})
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cache := NewNodeRequirementsCache()
			for c := 0; c < candidatesPerPass; c++ {
				for _, n := range nodes {
					_ = labelRequirementsForStateNode(cache, n)
				}
			}
		}
	})
}
