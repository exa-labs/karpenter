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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var _ = Describe("Rolling Restart Consolidation", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var receiverNodeClaim *v1.NodeClaim
	var receiverNode *corev1.Node

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})

		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

		receiverNodeClaim, receiverNode = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					v1.DoNotDisruptAnnotationKey: "true",
				},
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
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
		receiverNodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	})

	setupDeploymentWithPDB := func(podLabels map[string]string, podCount int) (*appsv1.Deployment, *appsv1.ReplicaSet, []*corev1.Pod, *policyv1.PodDisruptionBudget) {
		deploy := test.Deployment(test.DeploymentOptions{
			Replicas: int32(podCount),
			Labels:   podLabels,
		})
		deploy.Annotations = map[string]string{
			v1.AllowRollingRestartAnnotationKey: "true",
		}
		ExpectApplied(ctx, env.Client, deploy)

		rs := test.ReplicaSet()
		rs.OwnerReferences = []metav1.OwnerReference{{
			APIVersion:         "apps/v1",
			Kind:               "Deployment",
			Name:               deploy.Name,
			UID:                deploy.UID,
			Controller:         lo.ToPtr(true),
			BlockOwnerDeletion: lo.ToPtr(true),
		}}
		ExpectApplied(ctx, env.Client, rs)

		pods := test.Pods(podCount, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
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
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"),
				},
			},
		})

		pdb := test.PodDisruptionBudget(test.PDBOptions{
			Labels:         podLabels,
			MaxUnavailable: fromInt(0),
			Status: &policyv1.PodDisruptionBudgetStatus{
				ObservedGeneration: 1,
				DisruptionsAllowed: 0,
				CurrentHealthy:     int32(podCount),
				DesiredHealthy:     int32(podCount),
				ExpectedPods:       int32(podCount),
			},
		})

		return deploy, rs, pods, pdb
	}

	setupStatefulSetWithPDB := func(podLabels map[string]string) (*appsv1.StatefulSet, *corev1.Pod, *policyv1.PodDisruptionBudget) {
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-sts",
				Namespace: "default",
				Annotations: map[string]string{
					v1.AllowRollingRestartAnnotationKey: "true",
				},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: lo.ToPtr(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: podLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "test",
							Image: "public.ecr.aws/eks-distro/kubernetes/pause:3.2",
						}},
					},
				},
			},
		}
		ExpectApplied(ctx, env.Client, sts)

		pod := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "StatefulSet",
					Name:               sts.Name,
					UID:                sts.UID,
					Controller:         lo.ToPtr(true),
					BlockOwnerDeletion: lo.ToPtr(true),
				}},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"),
				},
			},
		})

		pdb := test.PodDisruptionBudget(test.PDBOptions{
			Labels:         podLabels,
			MaxUnavailable: fromInt(0),
			Status: &policyv1.PodDisruptionBudgetStatus{
				ObservedGeneration: 1,
				DisruptionsAllowed: 0,
				CurrentHealthy:     1,
				DesiredHealthy:     1,
				ExpectedPods:       1,
			},
		})

		return sts, pod, pdb
	}

	Context("Candidate Creation", func() {
		It("should create candidates for PDB-blocked nodes instead of returning an error", func() {
			podLabels := map[string]string{"pdb-test": "blocked"}
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
			})
			pdbObj := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         podLabels,
				MaxUnavailable: fromInt(0),
			})
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, pod, pdbObj)
			ExpectManualBinding(ctx, env.Client, pod, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			nodePoolMap, nodePoolToInstanceTypesMap, err := disruption.BuildNodePoolMap(ctx, env.Client, cloudProvider)
			Expect(err).To(Succeed())

			pdbs, err := pdb.NewLimits(ctx, env.Client)
			Expect(err).To(Succeed())

			Expect(cluster.Nodes()).To(HaveLen(1))
			stateNode := ExpectStateNodeExists(cluster, node)
			candidate, err := disruption.NewCandidate(ctx, env.Client, recorder, fakeClock,
				stateNode, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue, disruption.GracefulDisruptionClass)
			Expect(err).ToNot(HaveOccurred())
			Expect(candidate).ToNot(BeNil())
			Expect(candidate.NodeClaim).ToNot(BeNil())
			Expect(candidate.Node).ToNot(BeNil())
			Expect(candidate.PDBBlocked()).To(BeTrue())
		})
	})

	Context("Consolidation ShouldDisrupt", func() {
		It("should include PDB-blocked candidates for consolidation", func() {
			podLabels := map[string]string{"pdb-test": "consolidation"}
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
			})
			pdbObj := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         podLabels,
				MaxUnavailable: fromInt(0),
			})
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, pod, pdbObj)
			ExpectManualBinding(ctx, env.Client, pod, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			snc := disruption.NewSingleNodeConsolidation(c)

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, snc.ShouldDisrupt, snc.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(1))
		})
	})

	Context("Single-Node Consolidation", func() {
		It("should consolidate a PDB-blocked node via rolling restart", func() {
			podLabels := map[string]string{"pdb-test": "single-rolling"}
			deploy, _, pods, pdbObj := setupDeploymentWithPDB(podLabels, 1)
			_ = deploy

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, pods[0], pdbObj)
			ExpectManualBinding(ctx, env.Client, pods[0], node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			fakeClock.Step(10 * time.Minute)

			ExpectSingletonReconciled(ctx, disruptionController)

			// Node should be cordoned (tainted with disrupted-no-schedule).
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		})

		It("should not consolidate a PDB-blocked node without allow-rolling-restart annotation", func() {
			podLabels := map[string]string{"pdb-test": "no-annotation"}
			deploy := test.Deployment(test.DeploymentOptions{
				Replicas: int32(1),
				Labels:   podLabels,
			})
			ExpectApplied(ctx, env.Client, deploy)

			rs := test.ReplicaSet()
			rs.OwnerReferences = []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "Deployment",
				Name:               deploy.Name,
				UID:                deploy.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}}
			ExpectApplied(ctx, env.Client, rs)

			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
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
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})

			pdbObj := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         podLabels,
				MaxUnavailable: fromInt(0),
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, pod, pdbObj)
			ExpectManualBinding(ctx, env.Client, pod, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			snc := disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{}))

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, snc.Reason())
			Expect(err).To(Succeed())

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, snc.ShouldDisrupt, snc.Class(), queue)
			Expect(err).To(Succeed())

			cmds, err := snc.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(cmds).To(HaveLen(0))
		})

		It("should consolidate a StatefulSet-backed PDB-blocked node via rolling restart", func() {
			podLabels := map[string]string{"pdb-test": "sts-single"}
			_, stsPod, pdbObj := setupStatefulSetWithPDB(podLabels)

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, stsPod, pdbObj)
			ExpectManualBinding(ctx, env.Client, stsPod, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			fakeClock.Step(10 * time.Minute)

			ExpectSingletonReconciled(ctx, disruptionController)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		})

		It("should skip PDB-blocked candidate with do-not-disrupt annotation", func() {
			podLabels := map[string]string{"pdb-test": "do-not-disrupt"}
			deploy, _, pods, pdbObj := setupDeploymentWithPDB(podLabels, 1)
			_ = deploy

			// Add do-not-disrupt to both the node and nodeClaim so that
			// StateNode.Annotations() (which returns Node annotations when
			// registered) sees the annotation.
			nodeClaim.Annotations = map[string]string{
				v1.DoNotDisruptAnnotationKey: "true",
			}
			node.Annotations = map[string]string{
				v1.DoNotDisruptAnnotationKey: "true",
			}

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, pods[0], pdbObj)
			ExpectManualBinding(ctx, env.Client, pods[0], node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			snc := disruption.NewSingleNodeConsolidation(c)

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, snc.ShouldDisrupt, snc.Class(), queue)
			Expect(err).To(Succeed())
			// do-not-disrupt should prevent the candidate from being eligible
			Expect(candidates).To(HaveLen(0))
		})

		It("should deduplicate rolling restarts within the 10-minute window", func() {
			podLabels := map[string]string{"pdb-test": "dedup"}
			deploy, _, pods, pdbObj := setupDeploymentWithPDB(podLabels, 1)

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, pods[0], pdbObj)
			ExpectManualBinding(ctx, env.Client, pods[0], node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			if deploy.Spec.Template.Annotations == nil {
				deploy.Spec.Template.Annotations = map[string]string{}
			}
			deploy.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = fakeClock.Now().Format(time.RFC3339)
			ExpectApplied(ctx, env.Client, deploy)

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			snc := disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{}))

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, snc.Reason())
			Expect(err).To(Succeed())

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, snc.ShouldDisrupt, snc.Class(), queue)
			Expect(err).To(Succeed())

			cmds, err := snc.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(cmds).To(HaveLen(0))
		})
	})

	Context("Multi-Node Consolidation", func() {
		It("should include PDB-blocked candidates in multi-node consolidation", func() {
			podLabels := map[string]string{"pdb-test": "multi-node"}

			nodeClaim2, node2 := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})
			nodeClaim2.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

			deploy := test.Deployment(test.DeploymentOptions{
				Replicas: int32(1),
				Labels:   podLabels,
			})
			deploy.Annotations = map[string]string{
				v1.AllowRollingRestartAnnotationKey: "true",
			}
			ExpectApplied(ctx, env.Client, deploy)

			rs := test.ReplicaSet()
			rs.OwnerReferences = []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "Deployment",
				Name:               deploy.Name,
				UID:                deploy.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}}
			ExpectApplied(ctx, env.Client, rs)

			// PDB-blocked pod on node 1
			pdbPod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
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
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})

			// Regular pod on node 2 (no PDB)
			regularPod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
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
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})

			pdbObj := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         podLabels,
				MaxUnavailable: fromInt(0),
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, nodeClaim2, node2,
				receiverNodeClaim, receiverNode, pdbPod, regularPod, pdbObj)
			ExpectManualBinding(ctx, env.Client, pdbPod, node)
			ExpectManualBinding(ctx, env.Client, regularPod, node2)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, node2, receiverNode},
				[]*v1.NodeClaim{nodeClaim, nodeClaim2, receiverNodeClaim})

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			mnc := disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{}))

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, mnc.ShouldDisrupt, mnc.Class(), queue)
			Expect(err).To(Succeed())

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, mnc.Reason())
			Expect(err).To(Succeed())

			cmds, err := mnc.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(cmds).To(HaveLen(1))

			// Multi-node should include BOTH candidates.
			Expect(cmds[0].Candidates).To(HaveLen(2))
			candidateNames := lo.Map(cmds[0].Candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })
			Expect(candidateNames).To(ContainElement(node.Name))
			Expect(candidateNames).To(ContainElement(node2.Name))

			// Command should have rolling restart targets for the PDB-blocked candidate.
			Expect(cmds[0].Restarts).To(HaveLen(1))
			Expect(cmds[0].Restarts[0].Kind).To(Equal("Deployment"))
			Expect(cmds[0].Restarts[0].Name).To(Equal(deploy.Name))
		})

		It("should consolidate all-PDB multi-node command with rolling restarts", func() {
			podLabels1 := map[string]string{"pdb-test": "all-pdb-1"}
			podLabels2 := map[string]string{"pdb-test": "all-pdb-2"}

			nodeClaim2, node2 := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})
			nodeClaim2.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

			// Deployment 1 → RS1 → pod on node 1
			deploy1 := test.Deployment(test.DeploymentOptions{Replicas: int32(1), Labels: podLabels1})
			deploy1.Annotations = map[string]string{
				v1.AllowRollingRestartAnnotationKey: "true",
			}
			ExpectApplied(ctx, env.Client, deploy1)
			rs1 := test.ReplicaSet()
			rs1.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment",
				Name: deploy1.Name, UID: deploy1.UID,
				Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
			}}
			ExpectApplied(ctx, env.Client, rs1)
			pod1 := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels1,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet",
						Name: rs1.Name, UID: rs1.UID,
						Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
					}},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})
			pdb1 := test.PodDisruptionBudget(test.PDBOptions{
				Labels: podLabels1, MaxUnavailable: fromInt(0),
			})

			// Deployment 2 → RS2 → pod on node 2
			deploy2 := test.Deployment(test.DeploymentOptions{Replicas: int32(1), Labels: podLabels2})
			deploy2.Annotations = map[string]string{
				v1.AllowRollingRestartAnnotationKey: "true",
			}
			ExpectApplied(ctx, env.Client, deploy2)
			rs2 := test.ReplicaSet()
			rs2.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment",
				Name: deploy2.Name, UID: deploy2.UID,
				Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
			}}
			ExpectApplied(ctx, env.Client, rs2)
			pod2 := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels2,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet",
						Name: rs2.Name, UID: rs2.UID,
						Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
					}},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})
			pdb2 := test.PodDisruptionBudget(test.PDBOptions{
				Labels: podLabels2, MaxUnavailable: fromInt(0),
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, nodeClaim2, node2,
				receiverNodeClaim, receiverNode, pod1, pod2, pdb1, pdb2)
			ExpectManualBinding(ctx, env.Client, pod1, node)
			ExpectManualBinding(ctx, env.Client, pod2, node2)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, node2, receiverNode},
				[]*v1.NodeClaim{nodeClaim, nodeClaim2, receiverNodeClaim})

			fakeClock.Step(10 * time.Minute)

			ExpectSingletonReconciled(ctx, disruptionController)

			// Both nodes should be cordoned.
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
			node2 = ExpectNodeExists(ctx, env.Client, node2.Name)
			Expect(node2.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		})
	})

	Context("Validation", func() {
		It("should reject command when PDB-blocked status changes", func() {
			podLabels := map[string]string{"pdb-test": "validation-change"}
			deploy, _, pods, pdbObj := setupDeploymentWithPDB(podLabels, 1)
			_ = deploy

			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node,
				receiverNodeClaim, receiverNode, pods[0], pdbObj)
			ExpectManualBinding(ctx, env.Client, pods[0], node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client,
				nodeStateController, nodeClaimStateController,
				[]*corev1.Node{node, receiverNode},
				[]*v1.NodeClaim{nodeClaim, receiverNodeClaim})

			c := disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			snc := disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{}))

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, snc.Reason())
			Expect(err).To(Succeed())

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder,
				fakeClock, cloudProvider, snc.ShouldDisrupt, snc.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(1))
			Expect(candidates[0].PDBBlocked()).To(BeTrue())

			ExpectDeleted(ctx, env.Client, pdbObj)

			fakeClock.Step(20 * time.Second)
			cmds, err := snc.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(cmds).To(HaveLen(0))
		})
	})
})
