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

package state

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func simulationCopyStateNode(podCount int) *StateNode {
	n := NewNode()
	n.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	n.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim-1"}}
	for i := range podCount {
		key := types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("pod-%d", i)}
		requests := corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		}
		n.podRequests[key] = requests
		n.podLimits[key] = requests
		n.hostPortUsage.Add(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}},
			[]scheduling.HostPort{{Port: int32(1024 + i), Protocol: corev1.ProtocolTCP}}, //nolint:gosec
		)
	}
	return n
}

func TestSimulationCopyIsolatesMutableSimulationState(t *testing.T) {
	original := simulationCopyStateNode(3)
	sim := original.SimulationCopy()

	// Simulation mutates host port and volume usage via ExistingNode.Add; those must be isolated.
	sim.HostPortUsage().Add(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "sim-pod"}},
		[]scheduling.HostPort{{Port: 9999, Protocol: corev1.ProtocolTCP}},
	)
	if err := original.HostPortUsage().Conflicts(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "other"}},
		[]scheduling.HostPort{{Port: 9999, Protocol: corev1.ProtocolTCP}},
	); err != nil {
		t.Fatalf("expected simulation host port usage to not leak into the original, got conflict: %v", err)
	}

	// Map shells must be independent: adding/removing pods on one side must not show on the other.
	simKey := types.NamespacedName{Namespace: "default", Name: "sim-pod"}
	sim.podRequests[simKey] = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	if _, ok := original.podRequests[simKey]; ok {
		t.Fatal("expected pod request additions on the copy to not leak into the original")
	}
	liveKey := types.NamespacedName{Namespace: "default", Name: "live-pod"}
	original.podRequests[liveKey] = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	if _, ok := sim.podRequests[liveKey]; ok {
		t.Fatal("expected pod request additions on the original to not leak into the copy")
	}
}

func TestSimulationCopySharesImmutableState(t *testing.T) {
	original := simulationCopyStateNode(1)
	sim := original.SimulationCopy()

	if sim.Node != original.Node {
		t.Fatal("expected the Node object to be shared")
	}
	if sim.NodeClaim != original.NodeClaim {
		t.Fatal("expected the NodeClaim object to be shared")
	}
	key := types.NamespacedName{Namespace: "default", Name: "pod-0"}
	simCPU := sim.podRequests[key][corev1.ResourceCPU]
	originalCPU := original.podRequests[key][corev1.ResourceCPU]
	if !simCPU.Equal(originalCPU) {
		t.Fatal("expected inner ResourceLists to carry identical content")
	}
}

func TestSimulationCopyHandlesNilFields(t *testing.T) {
	n := &StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}
	sim := n.SimulationCopy()
	if sim.hostPortUsage != nil || sim.volumeUsage != nil {
		t.Fatal("expected nil usage trackers to remain nil")
	}
	if sim.Node != n.Node {
		t.Fatal("expected the Node object to be shared")
	}
}

func BenchmarkStateNodeDeepCopy(b *testing.B) {
	n := simulationCopyStateNode(50)
	b.ReportAllocs()
	for b.Loop() {
		_ = n.DeepCopy()
	}
}

func BenchmarkStateNodeSimulationCopy(b *testing.B) {
	n := simulationCopyStateNode(50)
	b.ReportAllocs()
	for b.Loop() {
		_ = n.SimulationCopy()
	}
}
