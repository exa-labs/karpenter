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
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var SingleNodeConsolidationTimeoutDuration = 3 * time.Minute

const SingleNodeConsolidationType = "single"

// SingleNodeConsolidation is the consolidation controller that performs single-node consolidation.
type SingleNodeConsolidation struct {
	consolidation
	PreviouslyUnseenNodePools  sets.Set[string]
	previouslyFailedCandidates sets.Set[string]
	validator                  Validator
}

func NewSingleNodeConsolidation(c consolidation, opts ...option.Function[MethodOptions]) *SingleNodeConsolidation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewSingleConsolidationValidator(c))}, opts...)...)
	return &SingleNodeConsolidation{
		consolidation:              c,
		PreviouslyUnseenNodePools:  sets.New[string](),
		previouslyFailedCandidates: sets.New[string](),
		validator:                  o.validator,
	}
}

// ComputeCommand generates a disruption command given candidates
// nolint:gocyclo
func (s *SingleNodeConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = s.SortCandidates(ctx, candidates)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false
	failedThisCycle := sets.New[string]()

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info("abandoning single-node consolidation due to timeout", "candidates_evaluated", i)

			s.PreviouslyUnseenNodePools = unseenNodePools
			s.previouslyFailedCandidates = failedThisCycle

			return []Command{}, nil
		}
		// Track that we've seen this nodepool
		unseenNodePools.Delete(candidate.NodePool.Name)

		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since single node consolidation commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		// Filter out empty candidates. If there was an empty node that wasn't consolidated before this, we should
		// assume that it was due to budgets. If we don't filter out budgets, users who set a budget for `empty`
		// can find their nodes disrupted here.
		if len(candidate.reschedulablePods) == 0 {
			continue
		}

		// compute a possible consolidation option
		cmd, err := s.computeConsolidation(ctx, candidate)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed computing consolidation")
			failedThisCycle.Insert(candidate.Name())
			continue
		}
		if cmd.Decision() == NoOpDecision {
			failedThisCycle.Insert(candidate.Name())
			continue
		}
		if _, err = s.validator.Validate(ctx, cmd, consolidationTTL); err != nil {
			if IsValidationError(err) {
				reason := getValidationFailureReason(err)
				cmd.EmitRejectedEvents(s.recorder, reason)
				s.previouslyFailedCandidates = failedThisCycle
				return []Command{}, nil
			}
			return []Command{}, fmt.Errorf("validating consolidation, %w", err)
		}
		// Successful consolidation — reset failed tracking since cluster state will change
		s.previouslyFailedCandidates = sets.New[string]()
		return []Command{cmd}, nil
	}

	if !constrainedByBudgets {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		s.markConsolidated()
	}

	s.PreviouslyUnseenNodePools = unseenNodePools
	s.previouslyFailedCandidates = failedThisCycle

	return []Command{}, nil
}

func (s *SingleNodeConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *SingleNodeConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *SingleNodeConsolidation) ConsolidationType() string {
	return SingleNodeConsolidationType
}

// SortCandidates sorts candidates by potential cost savings (instance price descending)
// and interweaves across nodepools to ensure fair evaluation. Candidates that failed
// consolidation in the previous cycle are deprioritized within each nodepool so that
// untried candidates get evaluated first.
func (s *SingleNodeConsolidation) SortCandidates(ctx context.Context, candidates []*Candidate) []*Candidate {
	// Sort by instance price descending (highest potential savings first).
	// Tiebreaker: DisruptionCost ascending (lower-cost evictions preferred among same-priced instances).
	sort.Slice(candidates, func(i, j int) bool {
		priceI := candidateInstancePrice(candidates[i])
		priceJ := candidateInstancePrice(candidates[j])
		if priceI != priceJ {
			return priceI > priceJ
		}
		return candidates[i].DisruptionCost < candidates[j].DisruptionCost
	})

	grouped := lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name })

	// Within each pool, push previously-failed candidates to the end so that
	// candidates that haven't been tried yet get evaluated first. This prevents
	// repeatedly timing out on the same unconsolidatable nodes every cycle.
	if s.previouslyFailedCandidates.Len() > 0 {
		log.FromContext(ctx).V(1).Info("deprioritizing previously failed candidates", "count", s.previouslyFailedCandidates.Len())
		for poolName, poolCandidates := range grouped {
			sort.SliceStable(poolCandidates, func(i, j int) bool {
				fi := s.previouslyFailedCandidates.Has(poolCandidates[i].Name())
				fj := s.previouslyFailedCandidates.Has(poolCandidates[j].Name())
				return !fi && fj
			})
			grouped[poolName] = poolCandidates
		}
	}

	return s.shuffleCandidates(ctx, grouped)
}

func (s *SingleNodeConsolidation) shuffleCandidates(ctx context.Context, nodePoolCandidates map[string][]*Candidate) []*Candidate {
	var result []*Candidate
	// Log any timed out nodepools that we're prioritizing
	if s.PreviouslyUnseenNodePools.Len() != 0 {
		log.FromContext(ctx).V(1).Info("prioritizing nodepools that have not yet been considered due to timeouts in previous runs", "nodepools", strings.Join(s.PreviouslyUnseenNodePools.UnsortedList(), ", "))
	}
	sortedNodePools := s.PreviouslyUnseenNodePools.UnsortedList()
	sortedNodePools = append(sortedNodePools, lo.Filter(lo.Keys(nodePoolCandidates), func(nodePoolName string, _ int) bool {
		return !s.PreviouslyUnseenNodePools.Has(nodePoolName)
	})...)

	// Find the maximum number of candidates in any nodepool
	maxCandidatesPerNodePool := lo.MaxBy(lo.Values(nodePoolCandidates), func(a, b []*Candidate) bool {
		return len(a) > len(b)
	})

	// Interweave candidates from different nodepools
	for i := range maxCandidatesPerNodePool {
		for _, nodePoolName := range sortedNodePools {
			if i < len(nodePoolCandidates[nodePoolName]) {
				result = append(result, nodePoolCandidates[nodePoolName][i])
			}
		}
	}

	return result
}

// candidateInstancePrice returns the hourly instance price for a candidate node.
// Returns 0.0 if the price cannot be determined (missing instance type or offerings).
func candidateInstancePrice(c *Candidate) float64 {
	if c.instanceType == nil {
		return 0.0
	}
	reqs := scheduling.NewLabelRequirements(c.Labels())
	compatibleOfferings := c.instanceType.Offerings.Compatible(reqs)
	if len(compatibleOfferings) == 0 {
		return 0.0
	}
	return compatibleOfferings.Cheapest().Price
}
