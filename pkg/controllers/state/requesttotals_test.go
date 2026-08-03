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
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func requestTotalsPod(name string, cpu string, daemon bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
				},
			}},
		},
	}
	if daemon {
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "DaemonSet",
			Name:       "ds",
			UID:        types.UID("ds-uid"),
			Controller: boolPtr(true),
		}}
	}
	return pod
}

func boolPtr(b bool) *bool { return &b }

func cpuMilli(rl corev1.ResourceList) int64 {
	q := rl[corev1.ResourceCPU]
	return q.MilliValue()
}

func requestTotalsStateNode(t *testing.T) *StateNode {
	t.Helper()
	ctx := context.Background()
	kubeClient := fakecr.NewClientBuilder().Build()
	n := NewNode()
	n.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	if err := n.updateForPod(ctx, kubeClient, requestTotalsPod("pod-1", "100m", false)); err != nil {
		t.Fatal(err)
	}
	if err := n.updateForPod(ctx, kubeClient, requestTotalsPod("ds-1", "50m", true)); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRequestTotalsMemoization(t *testing.T) {
	n := requestTotalsStateNode(t)

	// Fill the memoized totals as SimulationCopy does under the cluster lock.
	n.ensureRequestTotals()
	if cpuMilli(n.PodRequests()) != 150 {
		t.Fatalf("unexpected pod requests total: %v", n.PodRequests())
	}
	if cpuMilli(n.DaemonSetRequests()) != 50 {
		t.Fatalf("unexpected daemonset requests total: %v", n.DaemonSetRequests())
	}
}

func TestRequestTotalsInvalidation(t *testing.T) {
	n := requestTotalsStateNode(t)
	n.ensureRequestTotals()

	// A pod update must invalidate the memoized totals.
	if err := n.updateForPod(context.Background(), fakecr.NewClientBuilder().Build(), requestTotalsPod("pod-1", "200m", false)); err != nil {
		t.Fatal(err)
	}
	if n.podRequestsTotal != nil {
		t.Fatal("expected pod requests total to be invalidated by a pod update")
	}
	if cpuMilli(n.PodRequests()) != 250 {
		t.Fatalf("unexpected pod requests total after update: %v", n.PodRequests())
	}

	// Pod cleanup must invalidate both totals.
	n.ensureRequestTotals()
	n.cleanupForPod(types.NamespacedName{Namespace: "default", Name: "ds-1"})
	if n.podRequestsTotal != nil || n.daemonSetRequestsTotal != nil {
		t.Fatal("expected totals to be invalidated by pod cleanup")
	}
	n.ensureRequestTotals()
	dsTotal := n.DaemonSetRequests()
	if cpuMilli(n.PodRequests()) != 200 || cpuMilli(dsTotal) != 0 {
		t.Fatalf("unexpected totals after cleanup: %v %v", n.PodRequests(), dsTotal)
	}
}

func TestRequestTotalsSharedWithSimulationCopy(t *testing.T) {
	ctx := context.Background()
	kubeClient := fakecr.NewClientBuilder().Build()
	n := NewNode()
	n.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	if err := n.updateForPod(ctx, kubeClient, requestTotalsPod("pod-1", "100m", false)); err != nil {
		t.Fatal(err)
	}

	// The prefilled memoized total (as SimulationCopyNodes does under the write lock) is shared
	// by the copy; without a prefill the copy computes its own without writing to the live node.
	unprefilled := n.SimulationCopy()
	if unprefilled.podRequestsTotal == nil || n.podRequestsTotal != nil {
		t.Fatal("expected an unprefilled SimulationCopy to compute totals without writing back")
	}
	n.ensureRequestTotals()
	sim := n.SimulationCopy()
	if sim.podRequestsTotal == nil || n.podRequestsTotal == nil {
		t.Fatal("expected SimulationCopy to share the prefilled memoized totals")
	}
	if cpuMilli(sim.PodRequests()) != 100 {
		t.Fatalf("unexpected copy pod requests: %v", sim.PodRequests())
	}

	// Invalidation on the live node must not affect an existing copy's snapshot.
	if err := n.updateForPod(ctx, kubeClient, requestTotalsPod("pod-2", "300m", false)); err != nil {
		t.Fatal(err)
	}
	if cpuMilli(sim.PodRequests()) != 100 {
		t.Fatalf("expected the copy to keep its snapshot total, got %v", sim.PodRequests())
	}
	if cpuMilli(n.PodRequests()) != 400 {
		t.Fatalf("unexpected live pod requests after update: %v", n.PodRequests())
	}
}
