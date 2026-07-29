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
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

func TestConsolidationMetricsRecordLabels(t *testing.T) {
	disruption.ObserveConsolidationCandidateSkip("unit", "unit-pool", "unit-reason")
	disruption.ObserveConsolidationPass("unit", disruption.PassOutcomeNoOp, 265)

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
	if !hasMetric(families, "karpenter_voluntary_disruption_consolidation_candidate_depth", map[string]string{
		"consolidation_type": "unit",
	}) {
		t.Fatal("candidate depth metric was not recorded")
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
