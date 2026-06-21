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
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

type ValidationError struct {
	error
}

func NewValidationError(err error) *ValidationError {
	return &ValidationError{error: err}
}

func IsValidationError(err error) bool {
	if err == nil {
		return false
	}
	var validationError *ValidationError
	return errors.As(err, &validationError)
}

// BudgetValidationError indicates validation failed due to disruption budget constraints
type BudgetValidationError struct {
	*ValidationError
}

func NewBudgetValidationError(err error) *BudgetValidationError {
	return &BudgetValidationError{ValidationError: NewValidationError(err)}
}

func (e *BudgetValidationError) Unwrap() error {
	return e.ValidationError
}

// SchedulingValidationError indicates validation failed due to scheduling constraints
type SchedulingValidationError struct {
	*ValidationError
}

func NewSchedulingValidationError(err error) *SchedulingValidationError {
	return &SchedulingValidationError{ValidationError: NewValidationError(err)}
}

func (e *SchedulingValidationError) Unwrap() error {
	return e.ValidationError
}

// ChurnValidationError indicates validation failed due to churn detection
type ChurnValidationError struct {
	*ValidationError
}

func NewChurnValidationError(err error) *ChurnValidationError {
	return &ChurnValidationError{ValidationError: NewValidationError(err)}
}

func (e *ChurnValidationError) Unwrap() error {
	return e.ValidationError
}

type Validator interface {
	Validate(context.Context, Command, time.Duration) (Command, error)
}

// Validation is used to perform validation on a consolidation command.  It makes an assumption that when re-used, all
// of the commands passed to IsValid were constructed based off of the same consolidation state.  This allows it to
// skip the validation TTL for all but the first command.
type validation struct {
	clock         clock.Clock
	cluster       *state.Cluster
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	provisioner   *provisioning.Provisioner
	recorder      events.Recorder
	queue         *Queue
	reason        v1.DisruptionReason
}

type EmptinessValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func NewEmptinessValidator(c consolidation) *EmptinessValidator {
	e := &Emptiness{consolidation: c}
	return &EmptinessValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonEmpty,
		},
		filter:         e.ShouldDisrupt,
		validationType: e.ConsolidationType(),
	}
}

func (e *EmptinessValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return Command{}, errors.New("interrupted")
		case <-e.clock.After(validationPeriod):
		}
	}
	validatedCandidates, err := e.validateCandidates(ctx, cmd.Candidates...)
	if err != nil {
		return Command{}, err
	}
	cmd.Candidates = validatedCandidates
	return cmd, nil
}

type ConsolidationValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func NewSingleConsolidationValidator(c consolidation) *ConsolidationValidator {
	s := &SingleNodeConsolidation{consolidation: c}
	return &ConsolidationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         s.ShouldDisrupt,
		validationType: s.ConsolidationType(),
	}
}

func NewMultiConsolidationValidator(c consolidation) *ConsolidationValidator {
	m := &MultiNodeConsolidation{consolidation: c}
	return &ConsolidationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         m.ShouldDisrupt,
		validationType: m.ConsolidationType(),
	}
}

func (c *ConsolidationValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if err := c.isValid(ctx, cmd, validationPeriod); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (c *ConsolidationValidator) isValid(ctx context.Context, cmd Command, validationPeriod time.Duration) error {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return errors.New("context canceled")
		case <-c.clock.After(validationPeriod):
		}
	}
	validatedCandidates, err := c.validateCandidates(ctx, cmd.Candidates...)
	if err != nil {
		return err
	}
	if err := c.validateCommand(ctx, cmd, validatedCandidates); err != nil {
		return err
	}
	// Revalidate candidates after validating the command. This mitigates the chance of a race condition outlined in
	// the following GitHub issue: https://github.com/kubernetes-sigs/karpenter/issues/1167.
	if _, err = c.validateCandidates(ctx, validatedCandidates...); err != nil {
		return err
	}
	return nil
}

func (e *EmptinessValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	// This GetCandidates call filters out nodes that were nominated
	validatedCandidates, err := GetCandidates(ctx, e.cluster, e.kubeClient, e.recorder, e.clock, e.cloudProvider, e.filter, GracefulDisruptionClass, e.queue)
	if err != nil {
		return nil, fmt.Errorf("constructing validation candidates, %w", err)
	}
	validatedCandidates = mapCandidates(candidates, validatedCandidates)
	if len(validatedCandidates) == 0 {
		FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: e.validationType})
		return nil, NewChurnValidationError(fmt.Errorf("%d candidates are no longer valid", len(candidates)))
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, e.cluster, e.clock, e.kubeClient, e.cloudProvider, e.recorder, e.reason)
	if err != nil {
		return nil, fmt.Errorf("building disruption budgets, %w", err)
	}

	if valid := lo.Filter(validatedCandidates, func(cn *Candidate, _ int) bool {
		if e.cluster.IsNodeNominated(cn.ProviderID()) {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: e.validationType})
			return false
		}
		if disruptionBudgetMapping[cn.NodePool.Name] == 0 {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: e.validationType})
			return false
		}
		disruptionBudgetMapping[cn.NodePool.Name]--
		return true
	}); len(valid) > 0 {
		return valid, nil
	}
	return nil, NewBudgetValidationError(fmt.Errorf("%d candidates failed validation because it they were nominated for a pod or would violate disruption budgets", len(candidates)))
}

// ValidateCandidates gets the current representation of the provided candidates and ensures that they are all still valid.
// For a candidate to still be valid, the following conditions must be met:
//
//	a. It must pass the global candidate filtering logic (no blocking PDBs, no do-not-disrupt annotation, etc)
//	b. It must not have any pods nominated for it
//	c. It must still be disruptable without violating node disruption budgets
//
// If these conditions are met for all candidates, ValidateCandidates returns a slice with the updated representations.
func (c *ConsolidationValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	// GracefulDisruptionClass is hardcoded here because ValidateCandidates is only used for consolidation disruption. All consolidation disruption is graceful disruption.
	validatedCandidates, err := GetCandidates(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, c.filter, GracefulDisruptionClass, c.queue)
	if err != nil {
		return nil, fmt.Errorf("constructing validation candidates, %w", err)
	}
	validatedCandidates = mapCandidates(candidates, validatedCandidates)
	// If we filtered out any candidates, return nil as some NodeClaims in the consolidation decision have changed.
	if len(validatedCandidates) != len(candidates) {
		FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
		return nil, NewChurnValidationError(fmt.Errorf("%d candidates are no longer valid", len(candidates)-len(validatedCandidates)))
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, c.reason)
	if err != nil {
		return nil, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Return nil if any candidate meets either of the following conditions:
	//  a. A pod was nominated to the candidate
	//  b. Disrupting the candidate would violate node disruption budgets
	for _, vc := range validatedCandidates {
		if c.cluster.IsNodeNominated(vc.ProviderID()) {
			FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
			return nil, NewBudgetValidationError(fmt.Errorf("a candidate was nominated during validation"))
		}
		if disruptionBudgetMapping[vc.NodePool.Name] == 0 {
			FailedValidationsTotal.Add(float64(len(candidates)), map[string]string{ConsolidationTypeLabel: c.validationType})
			return nil, NewBudgetValidationError(fmt.Errorf("a candidate can no longer be disrupted without violating budgets"))
		}
		disruptionBudgetMapping[vc.NodePool.Name]--
	}
	return validatedCandidates, nil
}

// ValidateCommand validates a command for a Method
func (v *validation) validateCommand(ctx context.Context, cmd Command, candidates []*Candidate) error {
	// None of the chosen candidate are valid for execution, so retry
	if len(candidates) == 0 {
		return NewValidationError(fmt.Errorf("no candidates"))
	}
	results, err := simulateScheduling(ctx, v.kubeClient, v.cluster, v.provisioner, []pscheduling.Options{pscheduling.DisableReservedCapacityFallback}, candidates...)
	if err != nil {
		return fmt.Errorf("simluating scheduling, %w", err)
	}
	if !results.AllNonPendingPodsScheduled() {
		return NewSchedulingValidationError(errors.New(results.NonPendingPodSchedulingErrors()))
	}

	// We want to ensure that the re-simulated scheduling using the current cluster state produces the same result.
	if len(results.NewNodeClaims) == 0 {
		if len(cmd.Replacements) == 0 {
			// scheduling produced zero new NodeClaims and we weren't expecting any, so this is valid.
			return nil
		}
		// if it produced no new NodeClaims, but we were expecting one we should re-simulate as there is likely a better
		// consolidation option now
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}

	if len(results.NewNodeClaims) != len(cmd.Replacements) {
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}

	if !replacementsMatchNodeClaims(cmd.Replacements, results.NewNodeClaims) {
		return NewSchedulingValidationError(fmt.Errorf("scheduling simulation produced new results"))
	}
	return nil
}

func replacementsMatchNodeClaims(replacements []*Replacement, nodeClaims []*pscheduling.NodeClaim) bool {
	unmatchedNodeClaims := lo.SliceToMap(nodeClaims, func(nc *pscheduling.NodeClaim) (*pscheduling.NodeClaim, struct{}) {
		return nc, struct{}{}
	})
	for _, replacement := range replacements {
		match, ok := lo.Find(nodeClaims, func(nodeClaim *pscheduling.NodeClaim) bool {
			if _, ok := unmatchedNodeClaims[nodeClaim]; !ok {
				return false
			}
			if !instanceTypesAreSubset(replacement.InstanceTypeOptions, nodeClaim.InstanceTypeOptions) {
				return false
			}
			return nodeClaim.Requirements.Compatible(replacement.Requirements) == nil
		})
		if !ok {
			return false
		}
		delete(unmatchedNodeClaims, match)
	}
	return true
}

// getValidationFailureReason categorizes validation errors into specific failure types
func getValidationFailureReason(err error) string {
	if err == nil {
		return "unknown"
	}

	var budgetErr *BudgetValidationError
	var schedErr *SchedulingValidationError
	var churnErr *ChurnValidationError

	switch {
	case errors.As(err, &budgetErr):
		return "budget"
	case errors.As(err, &schedErr):
		return "scheduling"
	case errors.As(err, &churnErr):
		return "churn"
	default:
		return "unknown"
	}
}
