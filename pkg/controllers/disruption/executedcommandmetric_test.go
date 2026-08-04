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

package disruption_test

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type stubMethod struct {
	consolidationType string
}

func (s stubMethod) ShouldDisrupt(context.Context, *disruption.Candidate) bool { return true }
func (s stubMethod) ComputeCommands(context.Context, map[string]int, ...*disruption.Candidate) ([]disruption.Command, error) {
	return nil, nil
}
func (s stubMethod) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnderutilized }
func (s stubMethod) Class() string               { return "stub" }
func (s stubMethod) ConsolidationType() string   { return s.consolidationType }

func candidateInPool(name string) *disruption.Candidate {
	return &disruption.Candidate{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}}
}

func TestObserveExecutedConsolidationCommandRecordsReplacementCount(t *testing.T) {
	cases := []struct {
		name         string
		replacements int
		decision     string
		bucket       string
	}{
		{name: "delete", replacements: 0, decision: "delete", bucket: "0"},
		{name: "single replacement", replacements: 1, decision: "replace", bucket: "1"},
		{name: "two replacements", replacements: 2, decision: "replace", bucket: "2"},
		{name: "three replacements", replacements: 3, decision: "replace", bucket: "3"},
		{name: "over the bucketed maximum", replacements: 7, decision: "replace", bucket: "4+"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replacements := make([]*disruption.Replacement, tc.replacements)
			for i := range replacements {
				replacements[i] = &disruption.Replacement{}
			}
			disruption.ObserveExecutedConsolidationCommand(disruption.Command{
				Method:       stubMethod{consolidationType: "executed-unit"},
				Candidates:   []*disruption.Candidate{candidateInPool("executed-pool")},
				Replacements: replacements,
			})

			families, err := crmetrics.Registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_nodes_total", map[string]string{
				"consolidation_type": "executed-unit",
				"nodepool":           "executed-pool",
				"decision":           tc.decision,
				"replacement_count":  tc.bucket,
			}) {
				t.Fatalf("executed command metric was not recorded for %d replacements", tc.replacements)
			}
		})
	}
}

func TestObserveExecutedConsolidationCommandRecordsEveryCandidate(t *testing.T) {
	disruption.ObserveExecutedConsolidationCommand(disruption.Command{
		Method: stubMethod{consolidationType: "executed-multi"},
		Candidates: []*disruption.Candidate{
			candidateInPool("pool-a"),
			candidateInPool("pool-b"),
		},
		Replacements: []*disruption.Replacement{{}},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range []string{"pool-a", "pool-b"} {
		if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_nodes_total", map[string]string{
			"consolidation_type": "executed-multi",
			"nodepool":           pool,
			"decision":           "replace",
			"replacement_count":  "1",
		}) {
			t.Fatalf("multi-candidate command did not record %s", pool)
		}
	}
}

// stubNodeClaimReader serves the launched NodeClaims a command created, and nothing else, so the
// launch observer is exercised for both a resolvable and an unresolvable replacement.
type stubNodeClaimReader struct {
	nodeClaims map[string]*v1.NodeClaim
}

func (s stubNodeClaimReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	nodeClaim, ok := s.nodeClaims[key.Name]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "nodeclaims"}, key.Name)
	}
	target, ok := obj.(*v1.NodeClaim)
	if !ok {
		return fmt.Errorf("unexpected object type %T", obj)
	}
	nodeClaim.DeepCopyInto(target)
	return nil
}

func (s stubNodeClaimReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

func replacement(name, nodePool string, capacityTypes ...string) *disruption.Replacement {
	template := pscheduling.NodeClaimTemplate{NodePoolName: nodePool}
	if len(capacityTypes) > 0 {
		template.Requirements = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...))
	} else {
		template.Requirements = scheduling.NewRequirements()
	}
	return &disruption.Replacement{
		NodeClaim: &pscheduling.NodeClaim{NodeClaimTemplate: template},
		Name:      name,
	}
}

// candidateOfType builds a candidate whose node reports an instance type, which is how the
// observers resolve a candidate the NodePool no longer offers that type from.
func candidateOfType(nodePool, instanceType string) *disruption.Candidate {
	candidate := candidateInPool(nodePool)
	candidate.StateNode = &state.StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{corev1.LabelInstanceTypeStable: instanceType},
	}}}
	return candidate
}

func launchedNodeClaim(name, instanceType, capacityType string) *v1.NodeClaim {
	return &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			corev1.LabelInstanceTypeStable: instanceType,
			v1.CapacityTypeLabelKey:        capacityType,
		},
	}}
}

func TestObserveExecutedReplacementLaunchesRecordsEveryTarget(t *testing.T) {
	reader := stubNodeClaimReader{nodeClaims: map[string]*v1.NodeClaim{
		"launch-a": launchedNodeClaim("launch-a", "small-type", "spot"),
		"launch-b": launchedNodeClaim("launch-b", "medium-type", "on-demand"),
	}}
	disruption.ObserveExecutedReplacementLaunches(context.Background(), reader, disruption.Command{
		Method:     stubMethod{consolidationType: "launch-unit"},
		Candidates: []*disruption.Candidate{candidateInPool("launch-pool")},
		Replacements: []*disruption.Replacement{
			replacement("launch-a", "launch-pool"),
			replacement("launch-b", "launch-pool"),
		},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ instanceType, capacityType string }{
		{instanceType: "small-type", capacityType: "spot"},
		{instanceType: "medium-type", capacityType: "on-demand"},
	} {
		if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_launches_total", map[string]string{
			"consolidation_type": "launch-unit",
			"nodepool":           "launch-pool",
			"to_instance_type":   target.instanceType,
			"to_capacity_type":   target.capacityType,
		}) {
			t.Fatalf("1->N command did not record the %s replacement", target.instanceType)
		}
	}
}

func TestObserveExecutedReplacementLaunchesWithoutALaunchedNodeClaim(t *testing.T) {
	// an unreadable NodeClaim must not fall back to the simulation's instance type options: they
	// routinely number in the hundreds, and the provider picked exactly one of them
	disruption.ObserveExecutedReplacementLaunches(context.Background(), stubNodeClaimReader{}, disruption.Command{
		Method:       stubMethod{consolidationType: "launch-unknown"},
		Candidates:   []*disruption.Candidate{candidateInPool("unknown-pool")},
		Replacements: []*disruption.Replacement{replacement("missing", "unknown-pool", v1.CapacityTypeSpot)},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_launches_total", map[string]string{
		"consolidation_type": "launch-unknown",
		"nodepool":           "unknown-pool",
		"to_instance_type":   "unknown",
		"to_capacity_type":   v1.CapacityTypeSpot,
	}) {
		t.Fatal("a replacement without a readable NodeClaim did not record its requirement's capacity type")
	}
}

func TestObserveExecutedReplacementLaunchesCollapsesMixedSources(t *testing.T) {
	// a multi-node command can disrupt many types; recording every combination it happens to
	// cover would multiply cardinality by combinations rather than by types
	disruption.ObserveExecutedReplacementLaunches(context.Background(), stubNodeClaimReader{}, disruption.Command{
		Method: stubMethod{consolidationType: "launch-mixed"},
		Candidates: []*disruption.Candidate{
			candidateOfType("mixed-pool", "small-type"),
			candidateOfType("mixed-pool", "large-type"),
		},
		Replacements: []*disruption.Replacement{replacement("mixed", "mixed-pool")},
	})
	disruption.ObserveExecutedReplacementLaunches(context.Background(), stubNodeClaimReader{}, disruption.Command{
		Method: stubMethod{consolidationType: "launch-uniform"},
		Candidates: []*disruption.Candidate{
			candidateOfType("uniform-pool", "small-type"),
			candidateOfType("uniform-pool", "small-type"),
		},
		Replacements: []*disruption.Replacement{replacement("uniform", "uniform-pool")},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_launches_total", map[string]string{
		"consolidation_type": "launch-mixed",
		"from_instance_type": "multiple",
	}) {
		t.Fatal("a command disrupting several instance types should collapse its source type")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_launches_total", map[string]string{
		"consolidation_type": "launch-uniform",
		"from_instance_type": "small-type",
	}) {
		t.Fatal("a command disrupting one instance type should record it")
	}
}

func TestObserveExecutedReplacementLaunchesIgnoresDeleteCommands(t *testing.T) {
	disruption.ObserveExecutedReplacementLaunches(context.Background(), stubNodeClaimReader{}, disruption.Command{
		Method:     stubMethod{consolidationType: "launch-delete"},
		Candidates: []*disruption.Candidate{candidateInPool("delete-pool")},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_launches_total", map[string]string{
		"nodepool": "delete-pool",
	}) {
		t.Fatal("a delete command should not record a replacement launch")
	}
}

func TestObserveExecutedConsolidationCommandIgnoresCommandWithoutMethod(t *testing.T) {
	disruption.ObserveExecutedConsolidationCommand(disruption.Command{
		Candidates: []*disruption.Candidate{candidateInPool("methodless-pool")},
	})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_nodes_total", map[string]string{
		"nodepool": "methodless-pool",
	}) {
		t.Fatal("a command without a method should not record an executed-command series")
	}
}
