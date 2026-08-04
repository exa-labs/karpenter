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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// scriptedValidator answers Validate from a fixed script, one entry per call, so a test can
// place a rejection at an exact position in a batch. Calls past the script are accepted.
type scriptedValidator struct {
	errs  []error
	calls int
	// periods records the validation period each call was given, which is how the tests assert
	// that only the first admission of a pass pays the settling wait.
	periods []time.Duration
}

func (v *scriptedValidator) Validate(_ context.Context, cmd disruption.Command, period time.Duration) (disruption.Command, error) {
	v.periods = append(v.periods, period)
	i := v.calls
	v.calls++
	if i < len(v.errs) && v.errs[i] != nil {
		return disruption.Command{}, v.errs[i]
	}
	return cmd, nil
}

// blockingValidator queues a rival command for a proposal's own candidate just as that proposal
// is validated, which is how a losing race with the queue is produced deterministically.
type blockingValidator struct {
	method    disruption.Method
	blockCall int
	calls     int
}

func (v *blockingValidator) Validate(ctx context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	defer func() { v.calls++ }()
	if v.calls == v.blockCall {
		rival := disruption.Command{Method: v.method, Candidates: cmd.Candidates}
		if err := queue.StartCommand(ctx, &rival); err != nil {
			return disruption.Command{}, err
		}
	}
	return cmd, nil
}

// immediateValidator runs the real consolidation validation without its settling wait, which the
// suite's fake clock never advances through.
type immediateValidator struct {
	inner disruption.Validator
}

func (v immediateValidator) Validate(ctx context.Context, cmd disruption.Command, _ time.Duration) (disruption.Command, error) {
	return v.inner.Validate(ctx, cmd, 0)
}

var _ = Describe("Batched Single-Node Consolidation", func() {
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var rs *appsv1.ReplicaSet
	var validator *scriptedValidator
	var singleNode *disruption.SingleNodeConsolidation
	labels := map[string]string{"app": "batched-consolidation"}

	// newSingleNodeConsolidation builds a single-node method backed by the given validator.
	newSingleNodeConsolidation := func(v disruption.Validator) *disruption.SingleNodeConsolidation {
		return disruption.NewSingleNodeConsolidation(
			disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(v),
		)
	}

	// podOn returns a ReplicaSet-owned pod requesting cpu, so its node is a real consolidation
	// candidate rather than an empty node.
	podOn := func(cpu string) *corev1.Pod {
		return test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "ReplicaSet",
					Name:               rs.Name,
					UID:                rs.UID,
					Controller:         lo.ToPtr(true),
					BlockOwnerDeletion: lo.ToPtr(true),
				}},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			},
		})
	}

	// applyNodes puts one pod of the given size on each node and syncs cluster state.
	applyNodes := func(count int, cpu string) {
		for i := range count {
			pod := podOn(cpu)
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pod)
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes[:count], nodeClaims[:count])
	}

	// admissionFailures reads the running total of proposals rejected at the given admission
	// stage, which the tests compare as a delta since the registry is process-wide.
	admissionFailures := func(stage, reason string) float64 {
		GinkgoHelper()
		metric, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_consolidation_admission_failures_total", map[string]string{
			"consolidation_type": disruption.SingleNodeConsolidationType,
			"stage":              stage,
			"reason":             reason,
		})
		if !found {
			return 0
		}
		return metric.GetCounter().GetValue()
	}

	candidatesFor := func(m *disruption.SingleNodeConsolidation) []*disruption.Candidate {
		GinkgoHelper()
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, m.ShouldDisrupt, m.Class(), queue)
		Expect(err).To(Succeed())
		return candidates
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MaxConsolidationCommandsPerPass: lo.ToPtr(3)}))
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            leastExpensiveInstance.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.Labels[v1.NodePoolLabelKey] = nodePool.Name
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		for _, n := range nodes {
			n.Labels[v1.NodePoolLabelKey] = nodePool.Name
		}
		rs = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)

		validator = &scriptedValidator{}
		singleNode = newSingleNodeConsolidation(validator)
	})

	AfterEach(func() {
		disruption.SingleNodeConsolidationTimeoutDuration = 3 * time.Minute
		ExpectCleanedUp(ctx, env.Client)
	})

	It("admits several non-overlapping commands from one pass", func() {
		applyNodes(4, "1")
		candidates := candidatesFor(singleNode)
		Expect(candidates).To(HaveLen(4))

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(3))

		// The pass queued its own commands, so the controller must not start them again.
		Expect(queue.GetCommands()).To(HaveLen(3))
		claimed := sets.New[string]()
		for _, cmd := range cmds {
			Expect(cmd.Admitted).To(BeTrue())
			Expect(cmd.ID).ToNot(BeZero())
			for _, c := range cmd.Candidates {
				Expect(claimed.Has(c.ProviderID())).To(BeFalse())
				claimed.Insert(c.ProviderID())
			}
		}
		// Only the first admission waits out the settling period; the rest inherit it.
		Expect(validator.periods).To(Equal([]time.Duration{15 * time.Second, 0, 0}))
	})

	It("keeps the one-command-per-pass behavior when the cap is 1", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{MaxConsolidationCommandsPerPass: lo.ToPtr(1)}))
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		// The unbatched path leaves starting the command to the controller.
		Expect(cmds[0].Admitted).To(BeFalse())
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("does not admit more commands than the NodePool's remaining budget", func() {
		applyNodes(4, "1")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 2}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("admits the remaining commands when one proposal is rejected at admission", func() {
		applyNodes(4, "1")
		validator.errs = []error{disruption.NewSchedulingValidationError(errors.New("stale plan"))}
		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		// The rejected proposal no longer abandons the whole pass.
		Expect(cmds).To(HaveLen(2))
		Expect(queue.GetCommands()).To(HaveLen(2))
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected + 1))
	})

	It("returns no commands when every proposal is rejected at admission", func() {
		applyNodes(4, "1")
		validator.errs = []error{
			disruption.NewSchedulingValidationError(errors.New("stale plan")),
			disruption.NewBudgetValidationError(errors.New("budget")),
			disruption.NewChurnValidationError(errors.New("churn")),
		}

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())
		Expect(queue.GetCommands()).To(BeEmpty())
		// Candidates are still actionable, so the pass must not claim the fleet is consolidated.
		Expect(singleNode.IsConsolidated()).To(BeFalse())
	})

	It("surfaces a start failure without losing the commands already admitted", func() {
		applyNodes(4, "1")
		blocker := &blockingValidator{blockCall: 1}
		method := newSingleNodeConsolidation(blocker)
		blocker.method = method

		cmds, err := method.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(method)...)
		Expect(err).To(HaveOccurred())
		// The command admitted before the failure keeps running, next to the rival command.
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Admitted).To(BeTrue())
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("stops admitting once the pass runs out of its admission reserve", func() {
		disruption.SingleNodeConsolidationTimeoutDuration = 5 * time.Second
		applyNodes(4, "1")
		skipped := admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		// The first proposal is always attempted; the rest fall outside the reserve.
		Expect(cmds).To(HaveLen(1))
		Expect(queue.GetCommands()).To(HaveLen(1))
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")).To(Equal(skipped + 2))
	})

	It("spends its admission reserve on attempts, not on admissions", func() {
		disruption.SingleNodeConsolidationTimeoutDuration = 5 * time.Second
		applyNodes(4, "1")
		validator.errs = []error{disruption.NewSchedulingValidationError(errors.New("stale plan"))}
		skipped := admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")

		cmds, err := singleNode.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidatesFor(singleNode)...)
		Expect(err).To(Succeed())
		Expect(cmds).To(BeEmpty())
		// A rejected attempt costs a re-simulation too, so it consumes the pass's one attempt
		// past the deadline rather than letting every remaining proposal validate for free.
		Expect(validator.calls).To(Equal(1))
		Expect(admissionFailures(disruption.AdmissionStageDeadline, "admission_reserve")).To(Equal(skipped + 2))
	})

	It("rejects a proposal whose headroom an earlier command in the same pass consumed", func() {
		// Two loaded nodes and one node with room for exactly one of their pods. Consolidating
		// either is valid on its own; consolidating both is not, and the second proposal is
		// validated only after the first command has marked its candidate for deletion.
		applyNodes(2, "20")
		spareClaim, spareNode := nodeClaims[2], nodes[2]
		spareClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		ExpectApplied(ctx, env.Client, spareClaim, spareNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{spareNode}, []*v1.NodeClaim{spareClaim})

		real := newSingleNodeConsolidation(immediateValidator{
			inner: disruption.NewSingleConsolidationValidator(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)),
		})
		candidates := lo.Filter(candidatesFor(real), func(c *disruption.Candidate, _ int) bool {
			return c.ProviderID() != spareClaim.Status.ProviderID
		})
		Expect(candidates).To(HaveLen(2))

		rejected := admissionFailures(disruption.AdmissionStageValidation, "scheduling")
		cmds, err := real.ComputeCommands(ctx, map[string]int{nodePool.Name: 100}, candidates...)
		Expect(err).To(Succeed())
		Expect(cmds).To(HaveLen(1))
		Expect(queue.GetCommands()).To(HaveLen(1))
		// The second proposal was held, then rejected against the state the first command created.
		Expect(admissionFailures(disruption.AdmissionStageValidation, "scheduling")).To(Equal(rejected + 1))
	})

	It("starts every command exactly once when the controller runs the pass", func() {
		applyNodes(4, "1")
		controller := disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(newSingleNodeConsolidation(&scriptedValidator{})))

		ExpectSingletonReconciled(ctx, controller)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(3))
		providerIDs := sets.New[string]()
		for _, cmd := range cmds {
			for _, c := range cmd.Candidates {
				Expect(providerIDs.Has(c.ProviderID())).To(BeFalse(), fmt.Sprintf("%s was claimed twice", c.ProviderID()))
				providerIDs.Insert(c.ProviderID())
			}
		}
	})
})
