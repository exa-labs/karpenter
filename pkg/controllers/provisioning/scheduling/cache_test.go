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
	"reflect"
	"sort"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"k8s.io/client-go/tools/record"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestDaemonOverheadCacheUsesNodeAttributesAndReusesResults(t *testing.T) {
	cache := NewDaemonOverheadCache()
	s := &Scheduler{daemonOverheadCache: cache}
	node := &state.StateNode{
		Node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:             types.UID("node-uid"),
				Name:            "node",
				ResourceVersion: "1",
				Labels:          map[string]string{"topology.kubernetes.io/zone": "a"},
			},
		},
	}
	daemon := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"topology.kubernetes.io/zone": "a"},
		},
	}
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})

	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemon}); len(got) != 1 {
		t.Fatalf("expected daemon pod to be compatible, got %d pods", len(got))
	}

	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemon}); len(got) != 1 {
		t.Fatalf("expected cached result for unchanged node attributes, got %d pods", len(got))
	}

	node.Labels()["topology.kubernetes.io/zone"] = "b"
	node.Node.ResourceVersion = "2"
	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemon}); len(got) != 0 {
		t.Fatalf("expected daemon pod to be incompatible after node label change, got %d pods", len(got))
	}

	node.Labels()["topology.kubernetes.io/zone"] = "a"
	node.Node.ResourceVersion = "1"
	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemon}); len(got) != 1 {
		t.Fatalf("expected cached result after restoring node label, got %d pods", len(got))
	}
}

func TestDaemonOverheadCacheInvalidatesWhenDaemonSetPodsChange(t *testing.T) {
	cache := NewDaemonOverheadCache()
	s := &Scheduler{daemonOverheadCache: cache}
	node := &state.StateNode{
		Node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:             types.UID("node-uid"),
				Name:            "node",
				ResourceVersion: "1",
				Labels:          map[string]string{"topology.kubernetes.io/zone": "a"},
			},
		},
	}
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	daemonA := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID("daemon-a"),
			Name:            "daemon-a",
			ResourceVersion: "1",
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"topology.kubernetes.io/zone": "a"},
		},
	}
	daemonB := daemonA.DeepCopy()
	daemonB.UID = types.UID("daemon-b")
	daemonB.Name = "daemon-b"
	daemonB.ResourceVersion = "2"
	daemonB.Spec.NodeSelector["topology.kubernetes.io/zone"] = "b"

	cache.updateDaemonSetGeneration([]*corev1.Pod{daemonA})
	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemonA}); len(got) != 1 {
		t.Fatalf("expected daemon A to be compatible, got %d pods", len(got))
	}

	cache.updateDaemonSetGeneration([]*corev1.Pod{daemonB})
	if got := s.getCompatibleDaemonPods(ctx, node, node.Taints(), []*corev1.Pod{daemonB}); len(got) != 0 {
		t.Fatalf("expected daemon B to be incompatible after generation change, got %d pods", len(got))
	}
}

func TestDaemonOverheadCachePreservesSchedulingResults(t *testing.T) {
	cached := solveCacheFixture(t, true)
	uncached := solveCacheFixture(t, false)

	if len(cached.NewNodeClaims) != len(uncached.NewNodeClaims) {
		t.Fatalf("new NodeClaim count differs: cached=%d uncached=%d", len(cached.NewNodeClaims), len(uncached.NewNodeClaims))
	}
	if len(cached.ExistingNodes) != len(uncached.ExistingNodes) {
		t.Fatalf("existing node count differs: cached=%d uncached=%d", len(cached.ExistingNodes), len(uncached.ExistingNodes))
	}
	if len(cached.PodErrors) != len(uncached.PodErrors) {
		t.Fatalf("pod error count differs: cached=%d uncached=%d", len(cached.PodErrors), len(uncached.PodErrors))
	}
	for i := range cached.NewNodeClaims {
		if len(cached.NewNodeClaims[i].Pods) != len(uncached.NewNodeClaims[i].Pods) {
			t.Fatalf("new NodeClaim %d pod count differs: cached=%d uncached=%d", i, len(cached.NewNodeClaims[i].Pods), len(uncached.NewNodeClaims[i].Pods))
		}
	}
	for i := range cached.ExistingNodes {
		cachedPods := lo.Map(cached.ExistingNodes[i].Pods, func(p *corev1.Pod, _ int) string { return p.Name })
		uncachedPods := lo.Map(uncached.ExistingNodes[i].Pods, func(p *corev1.Pod, _ int) string { return p.Name })
		sort.Strings(cachedPods)
		sort.Strings(uncachedPods)
		if !reflect.DeepEqual(cachedPods, uncachedPods) {
			t.Fatalf("existing node %d pod assignments differ: cached=%v uncached=%v", i, cachedPods, uncachedPods)
		}
	}
}

func solveCacheFixture(t *testing.T, useCache bool) Results {
	t.Helper()
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	client := fakecr.NewFakeClient()
	cloudProvider := fake.NewCloudProvider()
	instanceTypes := fake.InstanceTypes(2)
	cloudProvider.InstanceTypes = instanceTypes
	clock := clock.RealClock{}
	cluster := state.NewCluster(clock, client, cloudProvider)
	nodePool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool"}})
	stateNodes := make([]*state.StateNode, 3)
	for i := range stateNodes {
		name := fmt.Sprintf("node-%d", i)
		stateNodes[i] = state.NewNode()
		stateNodes[i].Node = &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:             types.UID(name),
				Name:            name,
				ResourceVersion: "1",
				Labels: map[string]string{
					v1.NodePoolLabelKey:      nodePool.Name,
					corev1.LabelHostname:     name,
					corev1.LabelTopologyZone: fmt.Sprintf("zone-%d", i),
				},
			},
			Status: corev1.NodeStatus{
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("16Gi"),
				},
			},
		}
	}
	pods := make([]*corev1.Pod, 4)
	for i := range pods {
		pods[i] = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), UID: types.UID(fmt.Sprintf("pod-%d", i))},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "main",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("500m"),
					}},
				}},
			},
		}
	}
	daemonSetPods := []*corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{UID: "daemon", Name: "daemon", ResourceVersion: "1"},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Containers: []corev1.Container{{
				Name: "daemon",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"),
				}},
			}},
		},
	}}
	instanceTypesByNodePool := map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}
	topology, err := NewTopology(ctx, client, cluster, stateNodes, []*v1.NodePool{nodePool}, instanceTypesByNodePool, pods)
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	if useCache {
		ctx = WithDaemonOverheadCache(ctx, NewDaemonOverheadCache())
	}
	scheduler := NewScheduler(
		ctx,
		client,
		[]*v1.NodePool{nodePool},
		cluster,
		stateNodes,
		topology,
		instanceTypesByNodePool,
		daemonSetPods,
		events.NewRecorder(&record.FakeRecorder{}),
		clock,
		nil,
		nil,
	)
	results, err := scheduler.Solve(ctx, pods)
	if err != nil {
		t.Fatalf("solving fixture: %v", err)
	}
	return results
}
