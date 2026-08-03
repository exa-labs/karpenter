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
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type nodeRequirementsCacheContextKey struct{}

// NodeRequirementsCache memoizes the label-derived scheduling requirements of nodes for one
// scheduling pass. Converting a node's labels into Requirements is a pure function of the labels,
// so the result can be shared across every candidate simulation within a pass. Cached Requirements
// are shared and MUST be treated as read-only; callers that need to mutate them must clone first
// (the individual *Requirement values are immutable and safe to share across clones).
type NodeRequirementsCache struct {
	mu                sync.RWMutex
	requirementsByKey map[string]scheduling.Requirements
}

func NewNodeRequirementsCache() *NodeRequirementsCache {
	return &NodeRequirementsCache{
		requirementsByKey: map[string]scheduling.Requirements{},
	}
}

func WithNodeRequirementsCache(ctx context.Context, cache *NodeRequirementsCache) context.Context {
	return context.WithValue(ctx, nodeRequirementsCacheContextKey{}, cache)
}

func NodeRequirementsCacheFromContext(ctx context.Context) *NodeRequirementsCache {
	cache, _ := ctx.Value(nodeRequirementsCacheContextKey{}).(*NodeRequirementsCache)
	return cache
}

// stateNodeRequirementsKey identifies the label content of a StateNode. StateNode.Labels() merges
// NodeClaim and Node labels, so the key must cover the identity and version of both objects.
func stateNodeRequirementsKey(node *state.StateNode) (string, bool) {
	if node == nil || (node.Node == nil && node.NodeClaim == nil) {
		return "", false
	}
	nodeUID, nodeResourceVersion := "", ""
	if node.Node != nil {
		var ok bool
		nodeUID, nodeResourceVersion, ok = cacheObjectKey(node.Node.UID, node.Node.ResourceVersion)
		if !ok {
			return "", false
		}
	}
	nodeClaimUID, nodeClaimResourceVersion := "", ""
	if node.NodeClaim != nil {
		var ok bool
		nodeClaimUID, nodeClaimResourceVersion, ok = cacheObjectKey(node.NodeClaim.UID, node.NodeClaim.ResourceVersion)
		if !ok {
			return "", false
		}
	}
	return strings.Join([]string{"s", nodeUID, nodeResourceVersion, nodeClaimUID, nodeClaimResourceVersion}, "\x00"), true
}

// nodeObjectRequirementsKey identifies the label content of a corev1.Node.
func nodeObjectRequirementsKey(node *corev1.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	uid, resourceVersion, ok := cacheObjectKey(node.UID, node.ResourceVersion)
	if !ok {
		return "", false
	}
	return strings.Join([]string{"n", uid, resourceVersion}, "\x00"), true
}

func (c *NodeRequirementsCache) requirements(key string) (scheduling.Requirements, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	requirements, ok := c.requirementsByKey[key]
	return requirements, ok
}

func (c *NodeRequirementsCache) setRequirements(key string, requirements scheduling.Requirements) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requirementsByKey[key] = requirements
}

// labelRequirementsForStateNode returns the shared, read-only label requirements for a state node.
// When the cache is nil or the node cannot be keyed safely, requirements are computed directly.
func labelRequirementsForStateNode(cache *NodeRequirementsCache, node *state.StateNode) scheduling.Requirements {
	if cache == nil {
		return scheduling.NewLabelRequirements(node.Labels())
	}
	key, ok := stateNodeRequirementsKey(node)
	if !ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return scheduling.NewLabelRequirements(node.Labels())
	}
	if requirements, ok := cache.requirements(key); ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return requirements
	}
	requirements := scheduling.NewLabelRequirements(node.Labels())
	cache.setRequirements(key, requirements)
	NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	return requirements
}

// existingNodeRequirementsForStateNode returns the shared, read-only requirements used to
// construct an ExistingNode: the node's label requirements plus its hostname requirement. The
// merged map is a pure function of the node's labels and hostname, both covered by the node/
// NodeClaim resource versions in the cache key, so it can be shared across every candidate
// simulation within a pass. ExistingNode never mutates its requirements map in place after
// construction (scheduling replaces the map wholesale), which is what makes sharing safe.
func existingNodeRequirementsForStateNode(cache *NodeRequirementsCache, node *state.StateNode, labelRequirements scheduling.Requirements) scheduling.Requirements {
	build := func() scheduling.Requirements {
		requirements := scheduling.NewRequirements(labelRequirements.Values()...)
		requirements.Add(scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, node.HostName()))
		return requirements
	}
	if cache == nil {
		return build()
	}
	key, ok := stateNodeRequirementsKey(node)
	if !ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return build()
	}
	key = "h\x00" + key
	if requirements, ok := cache.requirements(key); ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return requirements
	}
	requirements := build()
	cache.setRequirements(key, requirements)
	NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	return requirements
}

// labelRequirementsForNodeObject returns the shared, read-only label requirements for a Node object.
func labelRequirementsForNodeObject(cache *NodeRequirementsCache, node *corev1.Node) scheduling.Requirements {
	if cache == nil {
		return scheduling.NewLabelRequirements(node.Labels)
	}
	key, ok := nodeObjectRequirementsKey(node)
	if !ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		return scheduling.NewLabelRequirements(node.Labels)
	}
	if requirements, ok := cache.requirements(key); ok {
		NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		return requirements
	}
	requirements := scheduling.NewLabelRequirements(node.Labels)
	cache.setRequirements(key, requirements)
	NodeRequirementCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	return requirements
}
