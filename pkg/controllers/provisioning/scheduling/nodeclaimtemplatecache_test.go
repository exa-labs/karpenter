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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func templateCacheInstanceTypes(names ...string) []*cloudprovider.InstanceType {
	var its []*cloudprovider.InstanceType
	for _, name := range names {
		its = append(its, fake.NewInstanceType(name, fake.WithOfferings(cloudprovider.Offering{
			Available: true,
			Price:     1.0,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: "zone-1",
			}),
		})))
	}
	return its
}

func templateCacheBuild(np *v1.NodePool, its []*cloudprovider.InstanceType, calls *int) func() *NodeClaimTemplate {
	return func() *NodeClaimTemplate {
		*calls++
		nct := NewNodeClaimTemplate(np)
		nct.InstanceTypeOptions = its
		return nct
	}
}

func TestNodeClaimTemplateCacheReusesResultOnHit(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a", "it-b")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})

	calls := 0
	first, ok := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if !ok || first == nil {
		t.Fatal("expected a template on miss")
	}
	second, ok := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if !ok || second == nil {
		t.Fatal("expected a template on hit")
	}
	if calls != 1 {
		t.Fatalf("expected build to run once, ran %d times", calls)
	}
	if len(second.InstanceTypeOptions) != len(first.InstanceTypeOptions) {
		t.Fatalf("expected identical instance type options, got %d vs %d", len(second.InstanceTypeOptions), len(first.InstanceTypeOptions))
	}
	if first.NodePoolName != second.NodePoolName || len(first.Requirements) != len(second.Requirements) {
		t.Fatalf("expected equivalent templates, got %#v vs %#v", first, second)
	}
}

func TestNodeClaimTemplateCacheHandsOutIndependentCopies(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a", "it-b")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})

	calls := 0
	first, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	// simulate the in-place mutations NodeClaim finalization can perform
	first.InstanceTypeOptions[0], first.InstanceTypeOptions[1] = first.InstanceTypeOptions[1], first.InstanceTypeOptions[0]
	first.Requirements.Add(scheduling.NewRequirement("mutated-key", corev1.NodeSelectorOpIn, "value"))

	second, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if second.InstanceTypeOptions[0].Name != "it-a" {
		t.Fatalf("expected slice mutation of a previous result to not leak, got %q first", second.InstanceTypeOptions[0].Name)
	}
	if second.Requirements.Has("mutated-key") {
		t.Fatal("expected requirements mutation of a previous result to not leak")
	}
}

func TestNodeClaimTemplateCacheIsolatesObjectMetaMaps(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})

	calls := 0
	build := func() *NodeClaimTemplate {
		calls++
		nct := NewNodeClaimTemplate(np)
		nct.InstanceTypeOptions = its
		nct.Annotations = map[string]string{"existing": "value"}
		nct.Labels = map[string]string{"existing": "value"}
		return nct
	}
	first, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, build)
	// simulate the scheduler writing the min-values-relaxed annotation into a NodeClaim
	// that shallow-copied this template
	first.Annotations[v1.NodeClaimMinValuesRelaxedAnnotationKey] = "true"
	first.Labels["mutated"] = "true"

	second, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, build)
	if calls != 1 {
		t.Fatalf("expected build to run once, ran %d times", calls)
	}
	if _, ok := second.Annotations[v1.NodeClaimMinValuesRelaxedAnnotationKey]; ok {
		t.Fatal("expected annotation mutation of a previous result to not leak")
	}
	if _, ok := second.Labels["mutated"]; ok {
		t.Fatal("expected label mutation of a previous result to not leak")
	}
	if second.Annotations["existing"] != "value" || second.Labels["existing"] != "value" {
		t.Fatal("expected pre-existing ObjectMeta entries to be preserved")
	}
}

func TestNodeClaimTemplateCacheIsolatesRequirementMinValues(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})

	calls := 0
	minValues := 5
	build := func() *NodeClaimTemplate {
		calls++
		nct := NewNodeClaimTemplate(np)
		nct.InstanceTypeOptions = its
		nct.Requirements.Add(scheduling.NewRequirementWithFlexibility(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, &minValues, "it-a", "it-b"))
		return nct
	}
	first, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyBestEffort, build)
	// simulate the best-effort relaxation writing MinValues on the requirement in place
	relaxed := 1
	first.Requirements.Get(corev1.LabelInstanceTypeStable).MinValues = &relaxed

	second, _ := nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyBestEffort, build)
	if calls != 1 {
		t.Fatalf("expected build to run once, ran %d times", calls)
	}
	got := second.Requirements.Get(corev1.LabelInstanceTypeStable).MinValues
	if got == nil || *got != 5 {
		t.Fatalf("expected MinValues relaxation of a previous result to not leak, got %v", got)
	}
}

func TestNodeClaimTemplateCacheCachesNegativeResults(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 7})

	calls := 0
	build := func() *NodeClaimTemplate {
		calls++
		return nil
	}
	if _, ok := nodeClaimTemplateWithCache(ctx, np, nil, karpopts.MinValuesPolicyStrict, build); ok {
		t.Fatal("expected no template for a filtered-out NodePool")
	}
	if _, ok := nodeClaimTemplateWithCache(ctx, np, nil, karpopts.MinValuesPolicyStrict, build); ok {
		t.Fatal("expected cached negative result")
	}
	if calls != 1 {
		t.Fatalf("expected build to run once, ran %d times", calls)
	}
}

func TestNodeClaimTemplateCacheRecomputesOnRevisionChange(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a", "it-b")
	cache := NewNodeClaimTemplateCache()
	ctx := WithNodeClaimTemplateCache(context.Background(), cache)

	calls := 0
	nodeClaimTemplateWithCache(WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1}), np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	nodeClaimTemplateWithCache(WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 2}), np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if calls != 2 {
		t.Fatalf("expected revision change to recompute, build ran %d times", calls)
	}
}

func TestNodeClaimTemplateCacheRecomputesOnGenerationAndUIDChange(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})

	calls := 0
	nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))

	edited := np.DeepCopy()
	edited.Generation = np.Generation + 1
	nodeClaimTemplateWithCache(ctx, edited, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(edited, its, &calls))
	if calls != 2 {
		t.Fatalf("expected generation change to recompute, build ran %d times", calls)
	}

	recreated := domainGroupCacheNodePool("pool", "uid-recreated")
	nodeClaimTemplateWithCache(ctx, recreated, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(recreated, its, &calls))
	if calls != 3 {
		t.Fatalf("expected UID change to recompute, build ran %d times", calls)
	}
}

func TestNodeClaimTemplateCacheRecomputesOnMinValuesPolicyChange(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a")
	ctx := WithNodeClaimTemplateCache(context.Background(), NewNodeClaimTemplateCache())
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})

	calls := 0
	nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyBestEffort, templateCacheBuild(np, its, &calls))
	if calls != 2 {
		t.Fatalf("expected minValues policy change to recompute, build ran %d times", calls)
	}
}

func TestNodeClaimTemplateCacheBypassesWithoutRevisionOrUID(t *testing.T) {
	its := templateCacheInstanceTypes("it-a")
	cache := NewNodeClaimTemplateCache()
	ctx := WithNodeClaimTemplateCache(context.Background(), cache)

	np := domainGroupCacheNodePool("pool", "uid-pool")
	calls := 0
	// no revisions on the context
	nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	nodeClaimTemplateWithCache(ctx, np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if calls != 2 {
		t.Fatalf("expected bypass without revisions, build ran %d times", calls)
	}
	if len(cache.entries) != 0 {
		t.Fatal("expected cache to remain unpopulated on bypass")
	}

	// revision present but no UID
	ctx = WithInstanceTypeRevisions(ctx, map[string]uint64{"pool": 1})
	noUID := domainGroupCacheNodePool("pool", "")
	nodeClaimTemplateWithCache(ctx, noUID, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(noUID, its, &calls))
	if len(cache.entries) != 0 {
		t.Fatal("expected cache to remain unpopulated when the NodePool lacks a UID")
	}
}

func TestNodeClaimTemplateCacheWithoutCacheMatchesDirectConstruction(t *testing.T) {
	np := domainGroupCacheNodePool("pool", "uid-pool")
	its := templateCacheInstanceTypes("it-a")
	calls := 0
	first, ok := nodeClaimTemplateWithCache(context.Background(), np, its, karpopts.MinValuesPolicyStrict, templateCacheBuild(np, its, &calls))
	if !ok || first == nil || calls != 1 {
		t.Fatalf("expected direct construction without a cache, ok=%v calls=%d", ok, calls)
	}
}
