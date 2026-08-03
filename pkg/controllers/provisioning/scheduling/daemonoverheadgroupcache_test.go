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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	operatoroptions "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func overheadGroupTestTemplate(fingerprint uint64, valid bool) *NodeClaimTemplate {
	it := fake.NewInstanceType("test-instance-type")
	return &NodeClaimTemplate{
		NodePoolName:          "pool-a",
		InstanceTypeOptions:   []*cloudprovider.InstanceType{it},
		Requirements:          scheduling.NewRequirements(),
		cacheFingerprint:      fingerprint,
		cacheFingerprintValid: valid,
	}
}

func TestDaemonOverheadGroupCacheHitAndInvalidation(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	cache := NewDaemonOverheadCache()
	daemon := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "ds-pod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			}},
		},
	}
	cache.updateDaemonSetGeneration([]*corev1.Pod{daemon})
	nct := overheadGroupTestTemplate(1, true)

	first := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{daemon})[nct]
	if len(first) != 1 || first[0].DaemonOverhead.Cpu().MilliValue() != 100 {
		t.Fatalf("unexpected groups: %+v", first)
	}
	second := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{daemon})[nct]
	if &first[0] != &second[0] {
		t.Fatalf("expected cached groups to be shared across builds")
	}

	// A fingerprint change (NodePool spec or instance type set changed) must invalidate the entry.
	changed := overheadGroupTestTemplate(2, true)
	changed.NodePoolName = "pool-a"
	third := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{changed}, []*corev1.Pod{daemon})[changed]
	if &first[0] == &third[0] {
		t.Fatalf("expected recomputed groups after fingerprint change")
	}

	// A daemonset change must invalidate all entries.
	updatedDaemon := daemon.DeepCopy()
	updatedDaemon.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("200m")
	cache.updateDaemonSetGeneration([]*corev1.Pod{updatedDaemon})
	fourth := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, []*corev1.Pod{updatedDaemon})[nct]
	if fourth[0].DaemonOverhead.Cpu().MilliValue() != 200 {
		t.Fatalf("expected recomputed overhead after daemonset change, got %v", fourth[0].DaemonOverhead)
	}
}

func TestDaemonOverheadGroupCacheBypassesWithoutFingerprint(t *testing.T) {
	ctx := operatoroptions.ToContext(context.Background(), &operatoroptions.Options{})
	cache := NewDaemonOverheadCache()
	cache.updateDaemonSetGeneration(nil)
	nct := overheadGroupTestTemplate(0, false)

	first := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, nil)[nct]
	second := buildDaemonOverheadGroups(ctx, cache, []*NodeClaimTemplate{nct}, nil)[nct]
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("unexpected groups: %+v %+v", first, second)
	}
	if &first[0] == &second[0] {
		t.Fatalf("expected bypass to recompute groups when the template has no fingerprint")
	}
	if len(cache.overheadGroupsByPool) != 0 {
		t.Fatalf("expected nothing cached on bypass")
	}
}
