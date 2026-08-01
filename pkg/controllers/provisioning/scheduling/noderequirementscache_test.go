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
	"reflect"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func makeStateNode(uid, resourceVersion string, labels map[string]string) *state.StateNode {
	n := state.NewNode()
	n.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "node-" + uid,
			UID:             types.UID(uid),
			ResourceVersion: resourceVersion,
			Labels:          labels,
		},
	}
	return n
}

func TestNodeRequirementsCacheReturnsSharedResultForSameStateNode(t *testing.T) {
	cache := NewNodeRequirementsCache()
	node := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})

	first := labelRequirementsForStateNode(cache, node)
	second := labelRequirementsForStateNode(cache, node)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("expected identical requirements, got %v and %v", first, second)
	}
	if first.Get(corev1.LabelTopologyZone) != second.Get(corev1.LabelTopologyZone) {
		t.Fatalf("expected the cached *Requirement pointers to be shared across lookups")
	}
}

func TestNodeRequirementsCacheRecomputesWhenResourceVersionChanges(t *testing.T) {
	cache := NewNodeRequirementsCache()
	node := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})
	first := labelRequirementsForStateNode(cache, node)

	updated := makeStateNode("uid-1", "101", map[string]string{corev1.LabelTopologyZone: "us-west-2b"})
	second := labelRequirementsForStateNode(cache, updated)

	if first.Get(corev1.LabelTopologyZone).Any() == second.Get(corev1.LabelTopologyZone).Any() {
		t.Fatalf("expected recomputed requirements after resource version change")
	}
}

func TestNodeRequirementsCacheBypassesWhenNodeCannotBeKeyed(t *testing.T) {
	cache := NewNodeRequirementsCache()
	node := makeStateNode("", "", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})

	requirements := labelRequirementsForStateNode(cache, node)
	if requirements.Get(corev1.LabelTopologyZone).Any() != "us-west-2a" {
		t.Fatalf("expected requirements to be computed directly on bypass")
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if len(cache.requirementsByKey) != 0 {
		t.Fatalf("expected nothing to be cached on bypass, got %d entries", len(cache.requirementsByKey))
	}
}

func TestNodeRequirementsCacheDistinguishesStateNodesFromNodeObjects(t *testing.T) {
	cache := NewNodeRequirementsCache()
	stateNode := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})
	nodeObject := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "node-uid-1",
			UID:             types.UID("uid-1"),
			ResourceVersion: "100",
			Labels:          map[string]string{corev1.LabelTopologyZone: "us-west-2b"},
		},
	}

	stateRequirements := labelRequirementsForStateNode(cache, stateNode)
	objectRequirements := labelRequirementsForNodeObject(cache, nodeObject)

	if stateRequirements.Get(corev1.LabelTopologyZone).Any() != "us-west-2a" {
		t.Fatalf("unexpected state node requirements: %v", stateRequirements)
	}
	if objectRequirements.Get(corev1.LabelTopologyZone).Any() != "us-west-2b" {
		t.Fatalf("expected node object requirements to be cached under a distinct key, got %v", objectRequirements)
	}
}

func TestNodeRequirementsCacheStateNodeKeyCoversNodeClaim(t *testing.T) {
	cache := NewNodeRequirementsCache()
	node := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})
	node.NodeClaim = &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "claim-1",
			UID:             types.UID("claim-uid-1"),
			ResourceVersion: "5",
			Labels:          map[string]string{v1.NodePoolLabelKey: "pool-a"},
		},
	}
	first := labelRequirementsForStateNode(cache, node)
	// The node is unregistered, so StateNode.Labels() resolves to the NodeClaim's labels.
	if first.Get(v1.NodePoolLabelKey).Any() != "pool-a" {
		t.Fatalf("expected node claim labels in requirements, got %v", first)
	}

	// A NodeClaim label change bumps its resource version, which must invalidate the entry.
	updated := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})
	updated.NodeClaim = &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "claim-1",
			UID:             types.UID("claim-uid-1"),
			ResourceVersion: "6",
			Labels:          map[string]string{v1.NodePoolLabelKey: "pool-b"},
		},
	}
	second := labelRequirementsForStateNode(cache, updated)
	if second.Get(v1.NodePoolLabelKey).Any() != "pool-b" {
		t.Fatalf("expected recomputed requirements after node claim resource version change, got %v", second)
	}
}

func TestNewExistingNodeDoesNotMutateSharedRequirements(t *testing.T) {
	cache := NewNodeRequirementsCache()
	node := makeStateNode("uid-1", "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})

	shared := labelRequirementsForStateNode(cache, node)
	topology := &Topology{domainGroups: map[string]TopologyDomainGroup{}}
	existingNode := NewExistingNode(node, topology, nil, shared, corev1.ResourceList{}, nil, false)

	if !existingNode.requirements.Has(corev1.LabelHostname) {
		t.Fatalf("expected the existing node clone to gain the hostname requirement")
	}
	if shared.Has(corev1.LabelHostname) {
		t.Fatalf("hostname requirement leaked into the shared cached requirements")
	}
	cached := labelRequirementsForStateNode(cache, node)
	if cached.Has(corev1.LabelHostname) {
		t.Fatalf("hostname requirement leaked into the cache")
	}
}

func TestNodeRequirementsCacheConcurrentAccess(t *testing.T) {
	cache := NewNodeRequirementsCache()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				node := makeStateNode(fmt.Sprintf("uid-%d", j%10), "100", map[string]string{corev1.LabelTopologyZone: "us-west-2a"})
				requirements := labelRequirementsForStateNode(cache, node)
				if requirements.Get(corev1.LabelTopologyZone).Any() != "us-west-2a" {
					t.Errorf("unexpected requirements: %v", requirements)
					return
				}
			}
		}()
	}
	wg.Wait()
}
