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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

type daemonOverheadCacheContextKey struct{}

// DaemonOverheadCache memoizes candidate-invariant existing-node scheduling data for one scheduling pass.
// The cache must not be shared across passes because node labels and taints can change.
type DaemonOverheadCache struct {
	mu                       sync.RWMutex
	daemonPodsByKey          map[string][]*corev1.Pod
	daemonRequestsByKey      map[string]corev1.ResourceList
	overheadGroupsByPool     map[string]overheadGroupsCacheEntry
	daemonSetGeneration      string
	daemonSetGenerationValid bool
}

// overheadGroupsCacheEntry stores the daemon overhead groups computed for one NodeClaimTemplate,
// keyed by the template's cache fingerprint so any change to the NodePool spec or its instance
// type set invalidates the entry. DaemonSet changes invalidate the whole cache via
// updateDaemonSetGeneration.
type overheadGroupsCacheEntry struct {
	fingerprint uint64
	groups      []DaemonOverheadGroup
}

func NewDaemonOverheadCache() *DaemonOverheadCache {
	return &DaemonOverheadCache{
		daemonPodsByKey:      map[string][]*corev1.Pod{},
		daemonRequestsByKey:  map[string]corev1.ResourceList{},
		overheadGroupsByPool: map[string]overheadGroupsCacheEntry{},
	}
}

func (c *DaemonOverheadCache) updateDaemonSetGeneration(daemonSetPods []*corev1.Pod) {
	generation, ok := daemonSetPodsGeneration(daemonSetPods)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok || !c.daemonSetGenerationValid || c.daemonSetGeneration != generation {
		c.daemonPodsByKey = map[string][]*corev1.Pod{}
		c.daemonRequestsByKey = map[string]corev1.ResourceList{}
		c.overheadGroupsByPool = map[string]overheadGroupsCacheEntry{}
		c.daemonSetGeneration = generation
		c.daemonSetGenerationValid = ok
	}
}

func WithDaemonOverheadCache(ctx context.Context, cache *DaemonOverheadCache) context.Context {
	return context.WithValue(ctx, daemonOverheadCacheContextKey{}, cache)
}

func DaemonOverheadCacheFromContext(ctx context.Context) *DaemonOverheadCache {
	cache, _ := ctx.Value(daemonOverheadCacheContextKey{}).(*DaemonOverheadCache)
	return cache
}

func daemonSetPodsGeneration(daemonSetPods []*corev1.Pod) (string, bool) {
	entries := make([]string, len(daemonSetPods))
	for i, pod := range daemonSetPods {
		content, err := json.Marshal(struct {
			Namespace string
			Name      string
			Spec      corev1.PodSpec
		}{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			Spec:      pod.Spec,
		})
		if err != nil {
			return "", false
		}
		entries[i] = string(content)
	}
	sort.Strings(entries)
	sum := sha256.Sum256([]byte(strings.Join(entries, "\x01")))
	return hex.EncodeToString(sum[:]), true
}

func nodeCacheKey(node *state.StateNode, ignoreDRA bool) (string, bool) {
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
	return strings.Join([]string{
		nodeUID,
		nodeResourceVersion,
		nodeClaimUID,
		nodeClaimResourceVersion,
		strconv.FormatBool(ignoreDRA),
	}, "\x00"), true
}

func cacheObjectKey(uid types.UID, resourceVersion string) (string, string, bool) {
	if uid == "" || resourceVersion == "" {
		return "", "", false
	}
	return string(uid), resourceVersion, true
}

func (c *DaemonOverheadCache) daemonPods(key string) ([]*corev1.Pod, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pods, ok := c.daemonPodsByKey[key]
	return pods, ok
}

func (c *DaemonOverheadCache) setDaemonPods(key string, pods []*corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.daemonPodsByKey[key] = pods
}

// daemonRequests returns a deep copy of the cached summed daemon resource requests for a node.
// A deep copy is required because NewExistingNode mutates the ResourceList it is handed
// (SubtractFrom and clamping), and Quantity arithmetic can mutate shared inner state.
func (c *DaemonOverheadCache) daemonRequests(key string) (corev1.ResourceList, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	requests, ok := c.daemonRequestsByKey[key]
	if !ok {
		return nil, false
	}
	return requests.DeepCopy(), true
}

func (c *DaemonOverheadCache) setDaemonRequests(key string, requests corev1.ResourceList) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.daemonRequestsByKey[key] = requests.DeepCopy()
}

// overheadGroups returns the cached daemon overhead groups for a NodePool when the template
// fingerprint matches. The returned slice and its contents are shared across schedulers and MUST
// be treated as read-only; NewNodeClaim already deep copies the per-NodeClaim mutable piece
// (HostPortUsage) before any mutation.
func (c *DaemonOverheadCache) overheadGroups(nodePoolName string, fingerprint uint64) ([]DaemonOverheadGroup, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.overheadGroupsByPool[nodePoolName]
	if !ok || entry.fingerprint != fingerprint {
		return nil, false
	}
	return entry.groups, true
}

func (c *DaemonOverheadCache) setOverheadGroups(nodePoolName string, fingerprint uint64, groups []DaemonOverheadGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.overheadGroupsByPool[nodePoolName] = overheadGroupsCacheEntry{fingerprint: fingerprint, groups: groups}
}
