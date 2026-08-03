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
	"testing"

	prometheus "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

func TestConsolidationMetricsRecordLabels(t *testing.T) {
	disruption.ObserveConsolidationCandidateSkip("unit", "unit-pool", "unit-reason")
	disruption.ObserveConsolidationPass("unit", disruption.PassOutcomeNoOp, 265)
	disruption.ObserveConsolidationCandidateDepthByNodePool("unit", map[string]int{
		"unit-pool": 7,
	})
	disruption.ObserveConsolidationReplacementAttempt("unit", "unit-pool", 0)
	disruption.ObserveConsolidationReplacementAttempt("unit", "unit-pool", 1)
	disruption.ObserveConsolidationReplacementAttempt("unit", "unit-pool", 2)
	disruption.ObserveEligibleNodesByNodePool([]*disruption.Candidate{
		{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "unit-pool"}}},
	}, "unit-method", "", "unit-reason")
	disruption.ObserveEligibleNodesByNodePool([]*disruption.Candidate{
		{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "unit-pool"}}},
	}, "unit-method", "", "other-reason")
	disruption.ObserveUnseenNodePools("unit", []string{"unseen-pool"})

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_candidate_skips_total", map[string]string{
		"consolidation_type": "unit",
		"nodepool":           "unit-pool",
		"reason":             "unit-reason",
	}) {
		t.Fatal("candidate skip metric was not recorded with expected labels")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_pass_outcomes_total", map[string]string{
		"consolidation_type": "unit",
		"outcome":            disruption.PassOutcomeNoOp,
	}) {
		t.Fatal("pass outcome metric was not recorded with expected labels")
	}
	for _, replacementCount := range []string{"0", "1", "2+"} {
		if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_replacement_attempts_total", map[string]string{
			"consolidation_type": "unit",
			"nodepool":           "unit-pool",
			"replacement_count":  replacementCount,
		}) {
			t.Fatalf("replacement attempt metric was not recorded for count %s", replacementCount)
		}
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_candidate_depth", map[string]string{
		"consolidation_type": "unit",
	}) {
		t.Fatal("candidate depth metric was not recorded")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_candidate_depth_by_nodepool", map[string]string{
		"consolidation_type": "unit",
		"nodepool":           "unit-pool",
	}) {
		t.Fatal("per-nodepool candidate depth metric was not recorded")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_unseen_nodepools_total", map[string]string{
		"consolidation_type": "unit",
		"nodepool":           "unseen-pool",
	}) {
		t.Fatal("unseen nodepool metric was not recorded")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_eligible_nodes_by_nodepool", map[string]string{
		"consolidation_type": "",
		"nodepool":           "unit-pool",
		"reason":             "unit-reason",
	}) {
		t.Fatal("eligible nodepool metric was not recorded with expected labels")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_eligible_nodes_by_nodepool", map[string]string{
		"consolidation_type": "",
		"nodepool":           "unit-pool",
		"reason":             "other-reason",
	}) {
		t.Fatal("eligible nodepool metric lost a sibling reason series")
	}
}

func TestEligibleNodesByNodePoolMethodsDoNotCollide(t *testing.T) {
	// StaticDrift and Drift both report reason=drifted with an empty consolidation type over disjoint NodePool
	// sets; the later pass must not delete or overwrite the earlier pass's series
	disruption.ObserveEligibleNodesByNodePool([]*disruption.Candidate{
		{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "static-pool"}}},
	}, "static-drift", "", "drifted")
	disruption.ObserveEligibleNodesByNodePool([]*disruption.Candidate{
		{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "dynamic-pool"}}},
	}, "drift", "", "drifted")

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_eligible_nodes_by_nodepool", map[string]string{
		"consolidation_type": "",
		"nodepool":           "static-pool",
		"reason":             "drifted",
	}) {
		t.Fatal("a later method's pass deleted an earlier method's eligible nodepool series")
	}
	if !hasMetric(families, "karpenter_voluntary_disruption_eligible_nodes_by_nodepool", map[string]string{
		"consolidation_type": "",
		"nodepool":           "dynamic-pool",
		"reason":             "drifted",
	}) {
		t.Fatal("the later method's eligible nodepool series was not recorded")
	}
}

func hasMetric(families []*prometheus.MetricFamily, name string, labels map[string]string) bool {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := true
			for key, value := range labels {
				found := false
				for _, label := range metric.Label {
					if label.GetName() == key && label.GetValue() == value {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				return true
			}
		}
	}
	return false
}
