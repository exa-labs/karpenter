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
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// PassReads memoizes the cluster-wide reads a consolidation simulation makes that do not vary
// with the candidate under test: the pending pod backlog and the PodDisruptionBudget limits.
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
	backlogErr error
	backlogSet bool

	pdbs    pdb.Limits
	pdbsErr error
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
		if readAbandoned(err) {
			return nil, err
		}
		r.backlog, r.backlogErr, r.backlogSet = backlog, err, true
	}
	if r.backlogErr != nil {
		return nil, r.backlogErr
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
		if readAbandoned(err) {
			return nil, err
		}
		r.pdbs, r.pdbsErr, r.pdbsSet = pdbs, err, true
	}
	return r.pdbs, r.pdbsErr
}

// readAbandoned reports whether a read ended with its caller rather than with a failure of the
// read itself. Whichever candidate reaches a read first performs it under that candidate's
// deadline, so memoizing this class of error would hand one candidate's timeout to every later
// candidate in the pass and cost the pass every command it had left to find.
func readAbandoned(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
