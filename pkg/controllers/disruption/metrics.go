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
	"strconv"
	"strings"
	"sync"
	"time"

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
	stageLabel                   = "stage"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	outcomeLabel                 = "outcome"
	reasonLabel                  = "reason"
	replacementCountLabel        = "replacement_count"
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
	CandidateSkipApprovalRejected     = "approval_rejected"
)

const (
	SplitOutcomeCommand             = "command"
	SplitOutcomeNoOp                = "no_op"
	SplitOutcomeError               = "error"
	SplitOutcomeAttemptCapExhausted = "attempt_cap_exhausted"
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
	ConsolidationCandidateDepthByNodePool = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_candidate_depth_by_nodepool",
			Help:      "Number of candidates evaluated per NodePool in a consolidation pass. Labeled by consolidation type and NodePool.",
			Buckets:   consolidationCandidateBuckets,
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
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
	PassStageSecondsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "pass_stage_seconds_total",
			Help:      "Cumulative wall-clock seconds consolidation passes spent per stage. Stages are non-overlapping, so rates across stages show how the pass time budget divides between cluster state copying, pod gathering, scheduler construction, simulation solving, candidate validation, and the deliberate validation wait.",
		},
		[]string{ConsolidationTypeLabel, stageLabel},
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
			Help:      "Number of skipped single-node consolidation candidates by type, NodePool, and reason, plus budget-exhausted candidates from both methods.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, reasonLabel},
	)
	ConsolidationReplacementAttemptsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_replacement_attempts_total",
			Help:      "Number of single-node consolidation simulations by replacement count.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, replacementCountLabel},
	)
	ConsolidationRequiredReplacements = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_required_replacements",
			Help:      "Number of replacement NodeClaims required by single-node consolidation simulations needing more than one replacement, whether or not the count is within the configured maximum. Compare against candidate skips with reason multiple_replacements_required to see how many were blocked by the limit.",
			Buckets:   []float64{2, 3, 4, 5, 8, 10, 20, 50, 100},
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel},
	)
	ConsolidationExecutedNodesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_executed_nodes_total",
			Help:      "Number of nodes disrupted by consolidation commands that executed successfully, by type, NodePool, decision, and the number of replacement NodeClaims the command launched. Counted per disrupted node rather than per command so a multi-node command attributes to each candidate's own NodePool. Compare against consolidation_replacement_attempts_total to see how many simulated multi-replacement options actually execute.",
		},
		[]string{ConsolidationTypeLabel, metrics.NodePoolLabel, decisionLabel, replacementCountLabel},
	)
	ConsolidationSplitAttemptsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_split_attempts_total",
			Help:      "Number of split fallback simulations by NodePool and outcome, where a single-node candidate that no cheaper single replacement could absorb is re-simulated with its own price as a ceiling on new capacity. attempt_cap_exhausted counts candidates the fallback declined to retry because the pass already spent its attempt budget.",
		},
		[]string{metrics.NodePoolLabel, outcomeLabel},
	)
	ConsolidationSplitSecondsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_split_seconds_total",
			Help:      "Cumulative wall-clock seconds spent in split fallback simulations. This time is also counted by the pass stage counters it runs inside, so it measures how much of a pass's timeout the fallback consumes at the expense of candidate traversal depth.",
		},
		[]string{metrics.NodePoolLabel},
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
	EligibleNodesByNodePool = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes_by_nodepool",
			Help:      "Number of nodes eligible for disruption by NodePool, disruption reason, and consolidation type.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	UnseenNodePoolsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "unseen_nodepools_total",
			Help:      "Number of NodePools with zero candidates evaluated in a timed-out consolidation pass.",
		},
		[]string{metrics.NodePoolLabel, ConsolidationTypeLabel},
	)
)

var (
	eligibleNodePoolsMu sync.Mutex
	eligibleNodePools   = map[string]eligibleNodePoolSeries{}
)

type eligibleNodePoolSeries struct {
	labels map[string]string
	scope  string
	count  int
}

const (
	stageStateCopy    = "state_copy"
	stagePodGather    = "pod_gather"
	stageConstruction = "scheduler_construction"
	stageSimulation   = "simulation"
	// stageValidation covers candidate revalidation only; the nested command re-simulation accounts
	// for itself under the simulation stages and the deliberate delay under stageValidationWait,
	// keeping the stages additive.
	stageValidation     = "validation"
	stageValidationWait = "validation_wait"
)

// observePassStage accumulates wall-clock time since start into the pass stage counter. It is a
// no-op outside a consolidation pass (no consolidation type on the context).
func observePassStage(ctx context.Context, stage string, start time.Time) {
	if consolidationType := consolidationTypeFromContext(ctx); consolidationType != "" {
		PassStageSecondsTotal.Add(time.Since(start).Seconds(), map[string]string{ConsolidationTypeLabel: consolidationType, stageLabel: stage})
	}
}

// startPassStage starts timing a stage and returns a function that records the elapsed time the
// first time it is invoked; later invocations are no-ops. Calling it at stage completion and also
// deferring it keeps stages non-overlapping while still accounting for the budget consumed by
// stages cut short by an error or timeout.
func startPassStage(ctx context.Context, stage string) func() {
	start := time.Now()
	var once sync.Once
	return func() {
		once.Do(func() { observePassStage(ctx, stage, start) })
	}
}

// ObserveEligibleNodesByNodePool records the number of eligible candidates per NodePool for a single disruption
// method's pass. method distinguishes passes that share the same reason and consolidation type labels (e.g.
// StaticDrift and Drift both report reason=drifted): each method owns its own set of series, so one method's pass
// never deletes or overwrites another's. Methods sharing labels must observe disjoint NodePool sets.
func ObserveEligibleNodesByNodePool(candidates []*Candidate, method, consolidationType, reason string) {
	scope := method + "\x00" + labelKeyWithout(map[string]string{
		metrics.ReasonLabel:    reason,
		ConsolidationTypeLabel: consolidationType,
	}, metrics.NodePoolLabel)
	current := map[string]eligibleNodePoolSeries{}
	for _, candidate := range candidates {
		labels := map[string]string{
			metrics.NodePoolLabel:  candidate.NodePool.Name,
			metrics.ReasonLabel:    reason,
			ConsolidationTypeLabel: consolidationType,
		}
		key := method + "\x00" + labelKeyWithout(labels)
		series := current[key]
		series.labels = labels
		series.scope = scope
		series.count++
		current[key] = series
	}

	eligibleNodePoolsMu.Lock()
	defer eligibleNodePoolsMu.Unlock()
	for key, series := range eligibleNodePools {
		if series.scope == scope {
			if _, ok := current[key]; ok {
				continue
			}
			EligibleNodesByNodePool.Delete(series.labels)
			delete(eligibleNodePools, key)
		}
	}
	for key, series := range current {
		EligibleNodesByNodePool.Set(float64(series.count), series.labels)
		eligibleNodePools[key] = series
	}
}

func labelKeyWithout(labels map[string]string, excluded ...string) string {
	excludedLabels := map[string]struct{}{}
	for _, label := range excluded {
		excludedLabels[label] = struct{}{}
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if _, excluded := excludedLabels[key]; !excluded {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(labels[key])
		builder.WriteByte(0)
	}
	return builder.String()
}

func ObserveUnseenNodePools(consolidationType string, nodePools []string) {
	for _, nodePool := range nodePools {
		UnseenNodePoolsTotal.Inc(map[string]string{
			metrics.NodePoolLabel:  nodePool,
			ConsolidationTypeLabel: consolidationType,
		})
	}
}

func ObserveConsolidationCandidateSkip(consolidationType, nodePool, reason string) {
	ConsolidationCandidateSkipsTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		metrics.NodePoolLabel:  nodePool,
		reasonLabel:            reason,
	})
}

// ObserveConsolidationSplitAttempt records the outcome of one split fallback attempt, or of a
// candidate the fallback declined because the pass exhausted its attempt budget.
func ObserveConsolidationSplitAttempt(nodePool, outcome string) {
	ConsolidationSplitAttemptsTotal.Inc(map[string]string{
		metrics.NodePoolLabel: nodePool,
		outcomeLabel:          outcome,
	})
}

// ObserveConsolidationSplitDuration accumulates the wall-clock time a split fallback simulation took.
func ObserveConsolidationSplitDuration(nodePool string, duration time.Duration) {
	ConsolidationSplitSecondsTotal.Add(duration.Seconds(), map[string]string{
		metrics.NodePoolLabel: nodePool,
	})
}

// executedReplacementCountBucket bounds label cardinality while separating the
// counts a bounded 1->N replacement limit can produce.
func executedReplacementCountBucket(replacementCount int) string {
	switch {
	case replacementCount < 0:
		return "0"
	case replacementCount <= 3:
		return strconv.Itoa(replacementCount)
	default:
		return "4+"
	}
}

// ObserveExecutedConsolidationCommand records a command that finished
// successfully as one observation per node it disrupted, so a multi-node
// command attributes to each candidate's own NodePool instead of having to pick
// one. The counter is named for that unit: consolidation_executed_nodes_total.
func ObserveExecutedConsolidationCommand(cmd Command) {
	if cmd.Method == nil {
		return
	}
	bucket := executedReplacementCountBucket(len(cmd.Replacements))
	for _, candidate := range cmd.Candidates {
		ConsolidationExecutedNodesTotal.Inc(map[string]string{
			ConsolidationTypeLabel: cmd.ConsolidationType(),
			metrics.NodePoolLabel:  candidate.NodePool.Name,
			decisionLabel:          string(cmd.Decision()),
			replacementCountLabel:  bucket,
		})
	}
}

func ObserveConsolidationReplacementAttempt(consolidationType, nodePool string, replacementCount int) {
	bucket := "2+"
	switch replacementCount {
	case 0:
		bucket = "0"
	case 1:
		bucket = "1"
	}
	ConsolidationReplacementAttemptsTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		metrics.NodePoolLabel:  nodePool,
		replacementCountLabel:  bucket,
	})
}

func ObserveConsolidationPass(consolidationType, outcome string, depth int) {
	ConsolidationCandidateDepth.Observe(float64(depth), map[string]string{ConsolidationTypeLabel: consolidationType})
	ConsolidationPassOutcomesTotal.Inc(map[string]string{
		ConsolidationTypeLabel: consolidationType,
		outcomeLabel:           outcome,
	})
}

func ObserveConsolidationCandidateDepthByNodePool(consolidationType string, depths map[string]int) {
	for nodePool, depth := range depths {
		ConsolidationCandidateDepthByNodePool.Observe(float64(depth), map[string]string{
			ConsolidationTypeLabel: consolidationType,
			metrics.NodePoolLabel:  nodePool,
		})
	}
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
