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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
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
			if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_commands_total", map[string]string{
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
		if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_commands_total", map[string]string{
			"consolidation_type": "executed-multi",
			"nodepool":           pool,
			"decision":           "replace",
			"replacement_count":  "1",
		}) {
			t.Fatalf("multi-candidate command did not record %s", pool)
		}
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
	if hasMetric(families, "karpenter_voluntary_disruption_consolidation_executed_commands_total", map[string]string{
		"nodepool": "methodless-pool",
	}) {
		t.Fatal("a command without a method should not record an executed-command series")
	}
}
