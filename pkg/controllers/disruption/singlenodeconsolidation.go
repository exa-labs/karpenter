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
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/logging"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/metrics"
	kscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

const SingleNodeConsolidationTimeoutDuration = 60 * time.Minute

// SingleNodeConsolidation is the consolidation controller that performs single-node consolidation.
type SingleNodeConsolidation struct {
	consolidation
	// previouslyFailedCandidates tracks node names that failed consolidation
	// (NoOp or error) in the previous cycle. These are deprioritized so that
	// untried candidates get evaluated first.
	previouslyFailedCandidates sets.Set[string]
}

func NewSingleNodeConsolidation(consolidation consolidation) *SingleNodeConsolidation {
	return &SingleNodeConsolidation{
		consolidation:              consolidation,
		previouslyFailedCandidates: sets.New[string](),
	}
}

// ComputeCommand generates a disruption command given candidates
// nolint:gocyclo
func (s *SingleNodeConsolidation) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (Command, scheduling.Results, error) {
	if s.IsConsolidated() {
		return Command{}, scheduling.Results{}, nil
	}
	candidates = s.sortCandidatesByCostSavings(ctx, candidates)
	disruptionEligibleNodesGauge.With(map[string]string{
		methodLabel:            s.Type(),
		consolidationTypeLabel: s.ConsolidationType(),
	}).Set(float64(len(candidates)))

	v := NewValidation(consolidationTTL, s.clock, s.cluster, s.kubeClient, s.provisioner, s.cloudProvider, s.recorder, s.queue)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false

	// Track which candidates fail this cycle so they can be deprioritized next time.
	failedThisCycle := sets.New[string]()

	logging.FromContext(ctx).Debugf("single-node consolidation: evaluating %d candidates (sorted by instance price desc)", len(candidates))
	for i, candidate := range candidates {
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since single node consolidation commands can only have one candidate.
		if disruptionBudgetMapping[candidate.nodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		if s.clock.Now().After(timeout) {
			disruptionConsolidationTimeoutTotalCounter.WithLabelValues(s.ConsolidationType()).Inc()
			logging.FromContext(ctx).Debugf("abandoning single-node consolidation due to timeout after evaluating %d candidates", i)
			s.previouslyFailedCandidates = failedThisCycle
			return Command{}, scheduling.Results{}, nil
		}

		logging.FromContext(ctx).Debugf("single-node consolidation: start computeConsolidation for %s (price=%.4f)", candidate.Name(), candidateInstancePrice(candidate))
		startComputeConsolidation := time.Now()
		// compute a possible consolidation option
		cmd, results, err := s.computeConsolidation(ctx, candidate)
		if err != nil {
			logging.FromContext(ctx).Errorf("computing consolidation %s", err)
			failedThisCycle.Insert(candidate.Name())
			continue
		}
		if cmd.Action() == NoOpAction {
			logging.FromContext(ctx).Debugf("single-node consolidation: computeConsolidation for %s returned NoOp, took %s", candidate.Name(), time.Since(startComputeConsolidation))
			failedThisCycle.Insert(candidate.Name())
			continue
		}
		logging.FromContext(ctx).Debugf("single-node consolidation: computeConsolidation for %s took %s", candidate.Name(), time.Since(startComputeConsolidation))

		startIsValid := time.Now()
		isValid, err := v.IsValid(ctx, cmd)
		if err != nil {
			return Command{}, scheduling.Results{}, fmt.Errorf("validating consolidation, %w", err)
		}
		if !isValid {
			logging.FromContext(ctx).Debugf("abandoning single-node consolidation attempt due to pod churn, command is no longer valid, %s", cmd)
			return Command{}, scheduling.Results{}, nil
		}
		logging.FromContext(ctx).Debugf("single-node consolidation: validation for %s took %s", candidate.Name(), time.Since(startIsValid))
		s.previouslyFailedCandidates = sets.New[string]()
		return cmd, results, nil
	}

	s.previouslyFailedCandidates = failedThisCycle

	if !constrainedByBudgets {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		s.markConsolidated()
	}
	return Command{}, scheduling.Results{}, nil
}

// sortCandidatesByCostSavings sorts candidates by instance price descending
// (highest potential savings first) with DisruptionCost as a tiebreaker.
// Candidates that failed consolidation in the previous cycle are pushed to
// the end so that untried candidates get evaluated first.
func (s *SingleNodeConsolidation) sortCandidatesByCostSavings(ctx context.Context, candidates []*Candidate) []*Candidate {
	sort.Slice(candidates, func(i, j int) bool {
		priceI := candidateInstancePrice(candidates[i])
		priceJ := candidateInstancePrice(candidates[j])
		if priceI != priceJ {
			return priceI > priceJ
		}
		return candidates[i].disruptionCost < candidates[j].disruptionCost
	})

	// Deprioritize previously-failed candidates by pushing them to the end
	// while preserving their relative order (stable sort).
	if s.previouslyFailedCandidates.Len() > 0 {
		logging.FromContext(ctx).Debugf("deprioritizing %d previously failed candidates", s.previouslyFailedCandidates.Len())
		sort.SliceStable(candidates, func(i, j int) bool {
			fi := s.previouslyFailedCandidates.Has(candidates[i].Name())
			fj := s.previouslyFailedCandidates.Has(candidates[j].Name())
			return !fi && fj
		})
	}

	return candidates
}

// candidateInstancePrice returns the hourly instance price for a candidate node.
// Returns 0.0 if the price cannot be determined.
func candidateInstancePrice(c *Candidate) float64 {
	if c.instanceType == nil {
		return 0.0
	}
	reqs := kscheduling.NewLabelRequirements(c.Labels())
	compatibleOfferings := c.instanceType.Offerings.Available().Compatible(reqs)
	if len(compatibleOfferings) == 0 {
		return 0.0
	}
	return compatibleOfferings.Cheapest().Price
}

func (s *SingleNodeConsolidation) Type() string {
	return metrics.ConsolidationReason
}

func (s *SingleNodeConsolidation) ConsolidationType() string {
	return "single"
}
