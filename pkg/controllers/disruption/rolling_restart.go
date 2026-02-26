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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// rollingRestartDeduplicationWindow is the minimum time between successive restarts
// of the same workload. If a Deployment or StatefulSet was already restarted within
// this window (by a previous reconcile targeting a different node), the restart is
// skipped to avoid cascading rollouts.
const rollingRestartDeduplicationWindow = 10 * time.Minute

// RollingRestart represents a workload whose pod template will be patched to trigger
// a rolling update, moving pods off a cordoned node without using the eviction API.
type RollingRestart struct {
	Namespace string
	Name      string
	Kind      string // "Deployment" or "StatefulSet"
}

// collectRestartTargets walks the owner chain of each pod to find the owning Deployment
// or StatefulSet, deduplicating across pods that share the same owner.
func collectRestartTargets(ctx context.Context, kubeClient client.Client, clk clock.Clock, pods []*corev1.Pod) ([]RollingRestart, error) {
	seen := map[types.NamespacedName]bool{}
	var restarts []RollingRestart

	for _, po := range pods {
		if !podutil.IsEvictable(po) {
			continue
		}
		owner, err := resolveRestartTarget(ctx, kubeClient, po)
		if err != nil {
			return nil, fmt.Errorf("resolving restart target for pod %s/%s: %w", po.Namespace, po.Name, err)
		}
		key := types.NamespacedName{Namespace: owner.Namespace, Name: owner.Name}
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := checkAllowRollingRestart(ctx, kubeClient, owner); err != nil {
			return nil, err
		}
		if wasRecentlyRestarted(ctx, kubeClient, clk, owner) {
			continue
		}
		restarts = append(restarts, owner)
	}
	return restarts, nil
}

// resolveRestartTarget walks the owner chain from a pod to find the owning Deployment
// or StatefulSet. For pods owned by a ReplicaSet, it checks the ReplicaSet's owner.
func resolveRestartTarget(ctx context.Context, kubeClient client.Client, po *corev1.Pod) (RollingRestart, error) {
	for _, ref := range po.OwnerReferences {
		if ref.Kind == "StatefulSet" && ref.APIVersion == "apps/v1" {
			return RollingRestart{
				Namespace: po.Namespace,
				Name:      ref.Name,
				Kind:      "StatefulSet",
			}, nil
		}
		if ref.Kind == "ReplicaSet" && ref.APIVersion == "apps/v1" {
			rs := &appsv1.ReplicaSet{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: po.Namespace}, rs); err != nil {
				return RollingRestart{}, fmt.Errorf("getting ReplicaSet %s/%s: %w", po.Namespace, ref.Name, err)
			}
			for _, rsRef := range rs.OwnerReferences {
				if rsRef.Kind == "Deployment" && rsRef.APIVersion == "apps/v1" {
					return RollingRestart{
						Namespace: po.Namespace,
						Name:      rsRef.Name,
						Kind:      "Deployment",
					}, nil
				}
			}
			return RollingRestart{}, fmt.Errorf("ReplicaSet %s/%s is not owned by a Deployment", po.Namespace, ref.Name)
		}
	}
	return RollingRestart{}, fmt.Errorf("pod %s/%s has no restartable owner", po.Namespace, po.Name)
}

// checkAllowRollingRestart verifies that the owning workload has opted in to
// karpenter-triggered rolling restarts via the AllowRollingRestartAnnotationKey
// annotation. Returns an error if the annotation is missing or not "true".
func checkAllowRollingRestart(ctx context.Context, kubeClient client.Client, r RollingRestart) error {
	nn := types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
	switch r.Kind {
	case "Deployment":
		deploy := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, nn, deploy); err != nil {
			return fmt.Errorf("getting Deployment %s: %w", nn, err)
		}
		if deploy.Annotations[v1.AllowRollingRestartAnnotationKey] != "true" {
			return fmt.Errorf("Deployment %s missing %s annotation", nn, v1.AllowRollingRestartAnnotationKey)
		}
	case "StatefulSet":
		sts := &appsv1.StatefulSet{}
		if err := kubeClient.Get(ctx, nn, sts); err != nil {
			return fmt.Errorf("getting StatefulSet %s: %w", nn, err)
		}
		if sts.Annotations[v1.AllowRollingRestartAnnotationKey] != "true" {
			return fmt.Errorf("StatefulSet %s missing %s annotation", nn, v1.AllowRollingRestartAnnotationKey)
		}
	default:
		return fmt.Errorf("unsupported workload kind %q for rolling restart annotation check", r.Kind)
	}
	return nil
}

// triggerRollingRestarts patches each target workload's pod template annotation to
// trigger a rolling update. This is the same mechanism as `kubectl rollout restart`.
// Workloads that were already restarted within rollingRestartDeduplicationWindow are
// skipped to prevent duplicate rollouts when multiple nodes share the same owner.
func triggerRollingRestarts(ctx context.Context, kubeClient client.Client, clk clock.Clock, restarts []RollingRestart) error {
	now := clk.Now().Format(time.RFC3339)
	for _, r := range restarts {
		nn := types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
		switch r.Kind {
		case "Deployment":
			if err := restartDeployment(ctx, kubeClient, clk, nn, now); err != nil {
				return err
			}
		case "StatefulSet":
			if err := restartStatefulSet(ctx, kubeClient, clk, nn, now); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported workload kind %q for rolling restart", r.Kind)
		}
	}
	return nil
}

func restartDeployment(ctx context.Context, kubeClient client.Client, clk clock.Clock, nn types.NamespacedName, timestamp string) error {
	deploy := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, nn, deploy); err != nil {
		return fmt.Errorf("getting Deployment %s: %w", nn, err)
	}
	if recentlyRestarted(clk, deploy.Spec.Template.Annotations) {
		log.FromContext(ctx).Info("skipping rolling restart for Deployment: already restarted recently", "deployment", nn)
		RollingRestartSkippedDeduplicatedCounter.Inc(map[string]string{workloadKindLabel: "Deployment"})
		return nil
	}
	stored := deploy.DeepCopy()
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = map[string]string{}
	}
	deploy.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = timestamp
	if err := kubeClient.Patch(ctx, deploy, client.MergeFrom(stored)); err != nil {
		return err
	}
	log.FromContext(ctx).Info("triggered rolling restart for Deployment", "deployment", nn)
	return nil
}

func restartStatefulSet(ctx context.Context, kubeClient client.Client, clk clock.Clock, nn types.NamespacedName, timestamp string) error {
	sts := &appsv1.StatefulSet{}
	if err := kubeClient.Get(ctx, nn, sts); err != nil {
		return fmt.Errorf("getting StatefulSet %s: %w", nn, err)
	}
	if recentlyRestarted(clk, sts.Spec.Template.Annotations) {
		log.FromContext(ctx).Info("skipping rolling restart for StatefulSet: already restarted recently", "statefulset", nn)
		RollingRestartSkippedDeduplicatedCounter.Inc(map[string]string{workloadKindLabel: "StatefulSet"})
		return nil
	}
	stored := sts.DeepCopy()
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = map[string]string{}
	}
	sts.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = timestamp
	if err := kubeClient.Patch(ctx, sts, client.MergeFrom(stored)); err != nil {
		return err
	}
	log.FromContext(ctx).Info("triggered rolling restart for StatefulSet", "statefulset", nn)
	return nil
}

// wasRecentlyRestarted fetches the current workload from the API server and checks
// whether it was already restarted within the deduplication window.
func wasRecentlyRestarted(ctx context.Context, kubeClient client.Client, clk clock.Clock, r RollingRestart) bool {
	nn := types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
	switch r.Kind {
	case "Deployment":
		deploy := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, nn, deploy); err != nil {
			return false
		}
		return recentlyRestarted(clk, deploy.Spec.Template.Annotations)
	case "StatefulSet":
		sts := &appsv1.StatefulSet{}
		if err := kubeClient.Get(ctx, nn, sts); err != nil {
			return false
		}
		return recentlyRestarted(clk, sts.Spec.Template.Annotations)
	}
	return false
}

// recentlyRestarted checks if the pod template annotations contain a restartedAt
// timestamp within the deduplication window.
func recentlyRestarted(clk clock.Clock, annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	restartedAt, ok := annotations["kubectl.kubernetes.io/restartedAt"]
	if !ok {
		return false
	}
	t, err := time.Parse(time.RFC3339, restartedAt)
	if err != nil {
		return false
	}
	return clk.Since(t) < rollingRestartDeduplicationWindow
}
