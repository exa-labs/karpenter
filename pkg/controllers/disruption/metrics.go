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

package disruption

import (
	"context"
	"sort"
	"strings"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	outcomeLabel                 = "outcome"
	reasonLabel                  = "reason"
	capacityTypeTransitionLabel  = "capacity_type_transition"
)

const (
	PassOutcomeCompleted              = "completed"
	PassOutcomeTimedOut               = "timed_out"
	PassOutcomeBudgetConstrained      = "budget_constrained"
	PassOutcomeNoOp                   = "no_op"
	CandidateSkipBudgetExhausted      = "budget_exhausted"
	CandidateSkipThreshold            = "cannot_pass_threshold"
	CandidateSkipNoOp                 = "noop_decision"
	CandidateSkipComputeError         = "compute_error"
	CandidateSkipPodsDidNotSchedule   = "pods_did_not_schedule"
	CandidateSkipMultipleReplacements = "multiple_replacements_required"
)

var (
	consolidationCandidateBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 150, 200, 250, 300, 400, 500, 750, 1000}
	durationBuckets               = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 5, 10, 30, 60, 120, 180, 300}
)

func init() {
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: MultiNodeConsolidationType})
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: SingleNodeConsolidationType})
}

var (
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]string{metrics.ReasonLabel},
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	ConsolidationCandidateDepth = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_depth",
			Help:      "Number of candidates evaluated in a single-node consolidation pass, or the deepest batch attempted by a multi-node binary search. Labeled by consolidation type.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	AcceptedCandidatePosition = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_accepted_candidate_position",
			Help:      "Zero-based position of the candidate that produced an accepted consolidation command. Multi-node batches emit once per candidate NodePool, so the sample count may exceed pass count.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	SchedulerConstructionDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "scheduler_construction_duration_seconds",
			Help:      "Duration spent constructing a scheduler during consolidation simulation.",
			Buckets:   durationBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationSimulationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_simulation_duration_seconds",
			Help:      "Raw duration spent solving a consolidation simulation, excluding scheduler construction.",
			Buckets:   durationBuckets,
		},
		[]string{ConsolidationTypeLabel},
	)
	ConsolidationPassOutcomesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_pass_outcomes_total",
			Help:      "Number of consolidation passes by outcome and consolidation type.",
		},
		[]string{ConsolidationTypeLabel, outcomeLabel},
	)
	ConsolidationCandidateSkipsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_skips_total",
			Help:      "Number of skipped consolidation candidates by type, NodePool, and reason.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, reasonLabel},
	)
	ConsolidationRequiredReplacements = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_required_replacements",
			Help:      "Number of replacement NodeClaims required by a consolidation simulation that cannot be represented as one replacement.",
			Buckets:   []float64{2, 3, 4, 5, 8, 10, 20, 50, 100},
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	ConsolidationRealizedSavingsDollarsPerHourTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_realized_savings_dollars_per_hour_total",
			Help:      "Cumulative realized hourly savings from successful consolidation commands.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel, capacityTypeTransitionLabel},
	)
)

func ObserveConsolidationCandidateSkip(consolidationType, nodePool, reason string) {
	ConsolidationCandidateSkipsTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		metrics.NodePoolLabel:  nodePool,
		reasonLabel:            reason,
	})
}

func ObserveConsolidationPass(consolidationType, outcome string, depth int) {
	ConsolidationCandidateDepth.Observe(float64(depth), map[string]string{ConsolidationTypeLabel: consolidationType})
	ConsolidationPassOutcomesTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		outcomeLabel:           outcome,
	})
}

func ObserveAcceptedCandidate(cmd Command, consolidationType string, position int) {
	for _, candidate := range cmd.Candidates {
		AcceptedCandidatePosition.Observe(float64(position), map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  candidate.NodePool.Name,
		})
	}
}

func ObserveRealizedSavings(ctx context.Context, kubeClient client.Reader, cmd Command) {
	transition := capacityTypeTransition(ctx, kubeClient, cmd)
	for _, candidate := range cmd.Candidates {
		ConsolidationRealizedSavingsDollarsPerHourTotal.Add(cmd.EstimatedSavings()/float64(len(cmd.Candidates)), map[string]string{
			metrics.NodePoolLabel:       candidate.NodePool.Name,
			decisionLabel:               string(cmd.Decision()),
			capacityTypeTransitionLabel: transition,
		})
	}
}

func capacityTypeTransition(ctx context.Context, kubeClient client.Reader, cmd Command) string {
	sources := make([]string, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		sources = append(sources, candidate.capacityType)
	}
	sources = uniqueSorted(sources)
	destinations := make([]string, 0, len(cmd.Replacements))
	for _, replacement := range cmd.Replacements {
		nodeClaim := &v1.NodeClaim{}
		if replacement.Name != "" && kubeClient.Get(ctx, types.NamespacedName{Name: replacement.Name}, nodeClaim) == nil {
			if capacityType := nodeClaim.Labels[v1.CapacityTypeLabelKey]; capacityType != "" {
				destinations = append(destinations, capacityType)
				continue
			}
		}
		if requirement := replacement.Requirements.Get(v1.CapacityTypeLabelKey); requirement != nil {
			destinations = append(destinations, requirement.Values()...)
		}
	}
	destinations = uniqueSorted(destinations)
	if len(destinations) == 0 {
		destinations = []string{"none"}
	}
	return strings.Join(sources, ",") + "->" + strings.Join(destinations, ",")
}

func uniqueSorted(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	result := make([]string, 0, len(sorted))
	for _, value := range sorted {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
