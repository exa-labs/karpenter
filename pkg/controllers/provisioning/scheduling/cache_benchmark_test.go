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

//go:build test_performance

package scheduling

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
)

func BenchmarkDaemonCompatibility800NodesNoCache(b *testing.B) {
	benchmarkDaemonCompatibility800Nodes(b, false)
}

func BenchmarkDaemonCompatibility800NodesCache(b *testing.B) {
	benchmarkDaemonCompatibility800Nodes(b, true)
}

func benchmarkDaemonCompatibility800Nodes(b *testing.B, useCache bool) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	nodes := make([]*state.StateNode, 800)
	for i := range nodes {
		nodes[i] = &state.StateNode{
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID(fmt.Sprintf("node-%d", i)),
					Name:            fmt.Sprintf("node-%d", i),
					ResourceVersion: "1",
					Labels: map[string]string{
						"pool": fmt.Sprintf("pool-%d", i%5),
					},
				},
			},
		}
	}
	daemonSetPods := make([]*corev1.Pod, 20)
	for i := range daemonSetPods {
		daemonSetPods[i] = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				UID:             types.UID(fmt.Sprintf("daemon-%d", i)),
				Name:            fmt.Sprintf("daemon-%d", i),
				ResourceVersion: "1",
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{
					"pool": fmt.Sprintf("pool-%d", i%5),
				},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var cache *DaemonOverheadCache
		if useCache {
			cache = NewDaemonOverheadCache()
			cache.updateDaemonSetGeneration(daemonSetPods)
		}
		s := &Scheduler{daemonOverheadCache: cache}
		for candidate := 0; candidate < 135; candidate++ {
			for nodeIndex, node := range nodes {
				if nodeIndex == candidate%len(nodes) {
					continue
				}
				s.getCompatibleDaemonPods(ctx, node, node.Taints(), daemonSetPods)
			}
		}
	}
}
