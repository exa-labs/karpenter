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

	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

type daemonOverheadCacheContextKey struct{}

// DaemonOverheadCache memoizes candidate-invariant existing-node scheduling data for one scheduling pass.
// The cache must not be shared across passes because node labels and taints can change.
type DaemonOverheadCache struct {
	mu                       sync.RWMutex
	daemonPodsByKey          map[string][]*corev1.Pod
	daemonSetGeneration      string
	daemonSetGenerationValid bool
}

func NewDaemonOverheadCache() *DaemonOverheadCache {
	return &DaemonOverheadCache{
		daemonPodsByKey: map[string][]*corev1.Pod{},
	}
}

func (c *DaemonOverheadCache) updateDaemonSetGeneration(daemonSetPods []*corev1.Pod) {
	generation, ok := daemonSetPodsGeneration(daemonSetPods)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok || !c.daemonSetGenerationValid || c.daemonSetGeneration != generation {
		c.daemonPodsByKey = map[string][]*corev1.Pod{}
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
	if node.Node != nil && (node.Node.UID == "" || node.Node.ResourceVersion == "") {
		return "", false
	}
	if node.NodeClaim != nil && (node.NodeClaim.UID == "" || node.NodeClaim.ResourceVersion == "") {
		return "", false
	}

	nodeUID, nodeResourceVersion := "", ""
	if node.Node != nil {
		nodeUID = string(node.Node.UID)
		nodeResourceVersion = node.Node.ResourceVersion
	}
	nodeClaimUID, nodeClaimResourceVersion := "", ""
	if node.NodeClaim != nil {
		nodeClaimUID = string(node.NodeClaim.UID)
		nodeClaimResourceVersion = node.NodeClaim.ResourceVersion
	}
	return strings.Join([]string{
		nodeUID,
		nodeResourceVersion,
		nodeClaimUID,
		nodeClaimResourceVersion,
		strconv.FormatBool(ignoreDRA),
	}, "\x00"), true
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
