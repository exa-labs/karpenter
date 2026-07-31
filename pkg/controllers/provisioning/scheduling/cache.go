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
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

type daemonOverheadCacheContextKey struct{}

// DaemonOverheadCache memoizes candidate-invariant existing-node scheduling data for one scheduling pass.
// The cache must not be shared across passes because node labels and taints can change.
type DaemonOverheadCache struct {
	mu                  sync.RWMutex
	daemonPodsByKey     map[string][]*corev1.Pod
	instanceTypes       map[string]*cloudprovider.InstanceType
	daemonSetGeneration string
}

func NewDaemonOverheadCache() *DaemonOverheadCache {
	return &DaemonOverheadCache{
		daemonPodsByKey: map[string][]*corev1.Pod{},
		instanceTypes:   map[string]*cloudprovider.InstanceType{},
	}
}

func (c *DaemonOverheadCache) updateDaemonSetGeneration(daemonSetPods []*corev1.Pod) {
	generation := daemonSetPodsGeneration(daemonSetPods)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.daemonSetGeneration != generation {
		c.daemonPodsByKey = map[string][]*corev1.Pod{}
		c.daemonSetGeneration = generation
	}
}

func WithDaemonOverheadCache(ctx context.Context, cache *DaemonOverheadCache) context.Context {
	return context.WithValue(ctx, daemonOverheadCacheContextKey{}, cache)
}

func DaemonOverheadCacheFromContext(ctx context.Context) *DaemonOverheadCache {
	cache, _ := ctx.Value(daemonOverheadCacheContextKey{}).(*DaemonOverheadCache)
	return cache
}

func daemonSetPodsGeneration(daemonSetPods []*corev1.Pod) string {
	entries := make([]string, len(daemonSetPods))
	for i, pod := range daemonSetPods {
		entries[i] = strings.Join([]string{
			string(pod.UID),
			pod.Namespace,
			pod.Name,
			pod.ResourceVersion,
		}, "\x00")
	}
	sort.Strings(entries)
	return strings.Join(entries, "\x01")
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

func (c *DaemonOverheadCache) instanceType(key string) (*cloudprovider.InstanceType, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	instanceType, ok := c.instanceTypes[key]
	return instanceType, ok
}

func (c *DaemonOverheadCache) setInstanceType(key string, instanceType *cloudprovider.InstanceType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instanceTypes[key] = instanceType
}
