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
	"hash/maphash"
	"maps"
	"slices"
	"sync"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type nodeClaimTemplateCacheContextKey struct{}

// NodeClaimTemplateCache memoizes the per-NodePool NodeClaimTemplate construction and instance
// type pre-filtering done at the top of NewScheduler for one scheduling pass. Both are pure
// functions of the NodePool spec and its instance type list, which are candidate-invariant, so
// consolidation rebuilding a scheduler per candidate re-filters an identical instance type set
// every time. Fingerprinting combines NodePool identity (UID, generation) with the provider's
// instance type revision plus the minValues policy that parameterizes the filter; a NodePool
// without a UID or a revision bypasses the cache. The scheduler treats its templates as
// read-only (NewNodeClaim copies the struct and builds fresh Requirements before mutating), but
// hits still hand out a shallow struct copy with a cloned Requirements map so a cached template
// can never be aliased across schedulers. The cache must not outlive a pass.
type NodeClaimTemplateCache struct {
	mu      sync.Mutex
	seed    maphash.Seed
	entries map[string]nodeClaimTemplateCacheEntry
}

type nodeClaimTemplateCacheEntry struct {
	fingerprint uint64
	template    *NodeClaimTemplate // nil records a NodePool whose requirements filtered out all instance types
}

func NewNodeClaimTemplateCache() *NodeClaimTemplateCache {
	return &NodeClaimTemplateCache{seed: maphash.MakeSeed(), entries: map[string]nodeClaimTemplateCacheEntry{}}
}

func WithNodeClaimTemplateCache(ctx context.Context, cache *NodeClaimTemplateCache) context.Context {
	return context.WithValue(ctx, nodeClaimTemplateCacheContextKey{}, cache)
}

func NodeClaimTemplateCacheFromContext(ctx context.Context) *NodeClaimTemplateCache {
	cache, _ := ctx.Value(nodeClaimTemplateCacheContextKey{}).(*NodeClaimTemplateCache)
	return cache
}

// nodeClaimTemplateWithCache returns the pre-filtered NodeClaimTemplate for a NodePool, reusing
// the pass-scoped cached result when the NodePool's identity and instance type revision are
// unchanged. build runs the full construction (including recorder events and logging for pools
// whose requirements filter out every instance type); cached negative results skip those
// emissions, which otherwise repeat identically for every candidate in a pass.
func nodeClaimTemplateWithCache(ctx context.Context, np *v1.NodePool, instanceTypes []*cloudprovider.InstanceType, minValuesPolicy karpopts.MinValuesPolicy, build func() *NodeClaimTemplate) (*NodeClaimTemplate, bool) {
	cache := NodeClaimTemplateCacheFromContext(ctx)
	if cache == nil {
		nct := build()
		return nct, nct != nil
	}
	fingerprint, ok := cache.fingerprintInputs(instanceTypeRevisionsFromContext(ctx), np, instanceTypes, minValuesPolicy)
	if !ok {
		NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeBypass})
		nct := build()
		return nct, nct != nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[np.Name]; ok && entry.fingerprint == fingerprint {
		NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeHit})
		if entry.template == nil {
			return nil, false
		}
		return copyNodeClaimTemplate(entry.template), true
	}
	NodeClaimTemplateCacheEventsTotal.Inc(map[string]string{outcomeLabel: cacheOutcomeMiss})
	nct := build()
	var cached *NodeClaimTemplate
	if nct != nil {
		cached = copyNodeClaimTemplate(nct)
	}
	cache.entries[np.Name] = nodeClaimTemplateCacheEntry{fingerprint: fingerprint, template: cached}
	return nct, nct != nil
}

// copyNodeClaimTemplate returns a shallow struct copy with fresh mutable containers: the
// Requirements map, the InstanceTypeOptions slice (sorted in place by OrderByPrice during
// NodeClaim finalization), and the ObjectMeta Labels/Annotations maps (the scheduler writes the
// min-values-relaxed annotation into a NodeClaim's shared map in place). Individual *Requirement
// and *InstanceType values are immutable once built (Requirements.Add replaces entries with new
// Intersection values rather than mutating in place), so sharing them across copies is safe.
func copyNodeClaimTemplate(nct *NodeClaimTemplate) *NodeClaimTemplate {
	out := *nct
	out.InstanceTypeOptions = slices.Clone(nct.InstanceTypeOptions)
	out.Labels = maps.Clone(nct.Labels)
	out.Annotations = maps.Clone(nct.Annotations)
	out.Requirements = scheduling.NewRequirements()
	for _, r := range nct.Requirements {
		out.Requirements[r.Key] = r
	}
	return &out
}

// fingerprintInputs hashes the inputs that determine a NodePool's pre-filtered template: the
// NodePool identity (UID, generation) covering its spec, the provider instance type revision
// (which only guarantees identical content for the same UID+generation) with the list length as
// a cross-check, and the minValues policy that changes filter behavior.
func (c *NodeClaimTemplateCache) fingerprintInputs(revisions map[string]uint64, np *v1.NodePool, instanceTypes []*cloudprovider.InstanceType, minValuesPolicy karpopts.MinValuesPolicy) (uint64, bool) {
	revision, ok := revisions[np.Name]
	if !ok || np.UID == "" {
		return 0, false
	}
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(string(np.UID))
	h.WriteByte(0)
	writeUint64(&h, uint64(np.Generation)) //nolint:gosec
	writeUint64(&h, uint64(len(instanceTypes)))
	writeUint64(&h, revision)
	h.WriteString(string(minValuesPolicy))
	return h.Sum64(), true
}
