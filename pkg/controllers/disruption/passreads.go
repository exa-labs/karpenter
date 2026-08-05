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
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// PassReads memoizes the cluster-wide reads a consolidation simulation makes that do not vary
// with the candidate under test: the pending pod backlog and the PodDisruptionBudget limits.
//
// Only answers are memoized, never failures. A failed read belongs to the candidate that happened
// to reach it first - an expired candidate deadline, a momentary apiserver error - and caching it
// would hand that candidate's bad luck to every later candidate in the pass. The next caller reads
// again, exactly as every candidate did before the reads were shared.
//
// A pass evaluates hundreds of candidates and asks each one the same question about the rest of
// the cluster. Re-reading the backlog per candidate multiplies a pass-level constant by the
// traversal depth, which is invisible while the backlog is empty and dominant while it is not:
// during a fleet doubling the backlog reached ~3,400 pods and the simulation stage went from
// cheaper than construction to 16x its steady-state cost.
//
// Memoizing also makes a pass internally consistent. Candidates are selected and sorted from one
// snapshot; reading a newer backlog for each of them means later candidates are judged against a
// cluster the ordering never saw. Live state remains authoritative where it matters: a validator
// that waits out a settling window installs its own PassReads, so the re-simulation that admits a
// command reads the backlog and the budgets as they are after the wait, not as discovery saw them.
type PassReads struct {
	mu sync.Mutex

	backlog    []*corev1.Pod
	backlogSet bool

	pdbs    pdb.Limits
	pdbsSet bool
}

type passReadsContextKey struct{}

func NewPassReads() *PassReads {
	return &PassReads{}
}

func WithPassReads(ctx context.Context, reads *PassReads) context.Context {
	return context.WithValue(ctx, passReadsContextKey{}, reads)
}

func PassReadsFromContext(ctx context.Context) *PassReads {
	reads, _ := ctx.Value(passReadsContextKey{}).(*PassReads)
	return reads
}

// pendingPods reads the backlog once and returns a fresh slice each time. The copy matters:
// SimulateScheduling appends the candidate's pods to what it gets back, which would otherwise
// write into the memoized backing array and leak one candidate's pods into the next. The pods
// themselves are shared, which is safe because scheduling reads them without mutating them.
func (r *PassReads) pendingPods(read func() ([]*corev1.Pod, error)) ([]*corev1.Pod, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.backlogSet {
		backlog, err := read()
		if err != nil {
			return nil, err
		}
		r.backlog, r.backlogSet = backlog, true
	}
	out := make([]*corev1.Pod, len(r.backlog))
	copy(out, r.backlog)
	return out, nil
}

// pdbLimits reads the PodDisruptionBudget limits once.
func (r *PassReads) pdbLimits(read func() (pdb.Limits, error)) (pdb.Limits, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.pdbsSet {
		pdbs, err := read()
		if err != nil {
			return nil, err
		}
		r.pdbs, r.pdbsSet = pdbs, true
	}
	return r.pdbs, nil
}

// pendingPodsForPass returns the pass's pending pod backlog, reading it once per pass. A caller
// outside a pass (no PassReads in context) reads through, preserving today's behavior.
func pendingPodsForPass(ctx context.Context, provisioner *provisioning.Provisioner) ([]*corev1.Pod, error) {
	read := func() ([]*corev1.Pod, error) { return provisioner.GetPendingPods(ctx) }
	reads := PassReadsFromContext(ctx)
	if reads == nil {
		return read()
	}
	return reads.pendingPods(read)
}

// pdbLimitsForPass returns the pass's PodDisruptionBudget limits, reading them once per pass.
func pdbLimitsForPass(ctx context.Context, kubeClient client.Client) (pdb.Limits, error) {
	read := func() (pdb.Limits, error) { return pdb.NewLimits(ctx, kubeClient) }
	reads := PassReadsFromContext(ctx)
	if reads == nil {
		return read()
	}
	return reads.pdbLimits(read)
}
