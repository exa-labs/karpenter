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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTopologyPassCachePodLists(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-1", Labels: map[string]string{"app": "a"}}}
	kubeClient := fakecr.NewClientBuilder().WithObjects(pod).Build()
	ctx := WithTopologyPassCache(context.Background(), NewTopologyPassCache())
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "a"}}

	first, err := listTopologyPods(ctx, kubeClient, "default", selector)
	if err != nil || len(first) != 1 {
		t.Fatalf("unexpected first list: %v %v", first, err)
	}

	// A pod created after the first read must not appear: the cache pins the pass's snapshot.
	if err := kubeClient.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod-2", Labels: map[string]string{"app": "a"}}}); err != nil {
		t.Fatal(err)
	}
	second, err := listTopologyPods(ctx, kubeClient, "default", selector)
	if err != nil || len(second) != 1 {
		t.Fatalf("expected cached pod list within the pass, got %v %v", second, err)
	}

	// A different namespace or selector is a different cache key.
	other, err := listTopologyPods(ctx, kubeClient, "default", &metav1.LabelSelector{MatchLabels: map[string]string{"app": "b"}})
	if err != nil || len(other) != 0 {
		t.Fatalf("unexpected list for different selector: %v %v", other, err)
	}

	// Without a cache in context, reads go straight through.
	fresh, err := listTopologyPods(context.Background(), kubeClient, "default", selector)
	if err != nil || len(fresh) != 2 {
		t.Fatalf("expected uncached list to see both pods, got %v %v", fresh, err)
	}
}

func TestTopologyPassCacheNodeLookups(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: map[string]string{"zone": "a"}}}
	kubeClient := fakecr.NewClientBuilder().WithObjects(node).Build()
	ctx := WithTopologyPassCache(context.Background(), NewTopologyPassCache())

	first, err := getTopologyNode(ctx, kubeClient, "node-1")
	if err != nil || first == nil {
		t.Fatalf("unexpected first get: %v %v", first, err)
	}
	second, err := getTopologyNode(ctx, kubeClient, "node-1")
	if err != nil || second != first {
		t.Fatalf("expected the cached node object to be shared, got %p vs %p (%v)", first, second, err)
	}

	// NotFound results are cached as nil without error.
	missing, err := getTopologyNode(ctx, kubeClient, "node-2")
	if err != nil || missing != nil {
		t.Fatalf("expected cached NotFound to yield nil, got %v %v", missing, err)
	}
	missingAgain, err := getTopologyNode(ctx, kubeClient, "node-2")
	if err != nil || missingAgain != nil {
		t.Fatalf("expected cached NotFound on repeat, got %v %v", missingAgain, err)
	}
}
