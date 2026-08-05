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
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

func testPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestPassReadsPendingPodsReadsOnce(t *testing.T) {
	reads := NewPassReads()
	calls := 0
	read := func() ([]*corev1.Pod, error) {
		calls++
		return []*corev1.Pod{testPod("a"), testPod("b")}, nil
	}
	for range 5 {
		pods, err := reads.pendingPods(read)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pods) != 2 {
			t.Fatalf("expected 2 pods, got %d", len(pods))
		}
	}
	if calls != 1 {
		t.Fatalf("expected the backlog to be read once, got %d reads", calls)
	}
}

// A candidate appends its own pods to the backlog it is handed. Two candidates must not see each
// other's, which is only true if each gets its own slice.
func TestPassReadsPendingPodsAppendDoesNotLeak(t *testing.T) {
	reads := NewPassReads()
	read := func() ([]*corev1.Pod, error) {
		// Capacity beyond length is what makes append write into a shared backing array.
		backlog := make([]*corev1.Pod, 1, 8)
		backlog[0] = testPod("backlog")
		return backlog, nil
	}

	first, err := reads.pendingPods(read)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	first = append(first, testPod("first-candidate"))

	second, err := reads.pendingPods(read)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("expected the second candidate to see only the backlog, got %d pods", len(second))
	}
	second = append(second, testPod("second-candidate"))

	if first[1].Name != "first-candidate" {
		t.Fatalf("the second candidate's pod overwrote the first's: %s", first[1].Name)
	}
	if second[1].Name != "second-candidate" {
		t.Fatalf("unexpected pod on the second candidate: %s", second[1].Name)
	}
}

// A failed read belongs to the candidate that hit it, not to the pass. Before the reads were
// shared every candidate listed for itself, so a momentary apiserver error cost one candidate;
// caching the failure would cost every candidate left in the walk.
func TestPassReadsDoesNotMemoizeAFailedRead(t *testing.T) {
	reads := NewPassReads()
	calls := 0
	read := func() ([]*corev1.Pod, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("listing pods")
		}
		return []*corev1.Pod{testPod("a")}, nil
	}
	if _, err := reads.pendingPods(read); err == nil {
		t.Fatal("expected the read error to be returned")
	}
	pods, err := reads.pendingPods(read)
	if err != nil {
		t.Fatalf("expected the next candidate to retry the read, got %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("expected the retried read's backlog, got %d pods", len(pods))
	}
}

// The candidate that happens to reach the read first performs it under its own deadline. Caching
// that candidate's expiry would answer every later candidate with it, so the pass would lose the
// commands the per-candidate bound exists to preserve.
func TestPassReadsDoesNotMemoizeAnAbandonedRead(t *testing.T) {
	reads := NewPassReads()
	calls := 0
	read := func() ([]*corev1.Pod, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("determining pending pods: %w", context.DeadlineExceeded)
		}
		return []*corev1.Pod{testPod("a")}, nil
	}

	if _, err := reads.pendingPods(read); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the first candidate to see its own deadline, got %v", err)
	}
	pods, err := reads.pendingPods(read)
	if err != nil {
		t.Fatalf("expected the next candidate to retry the read, got %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("expected the retried read's backlog, got %d pods", len(pods))
	}
}

func TestPassReadsDoesNotMemoizeAnAbandonedPDBRead(t *testing.T) {
	reads := NewPassReads()
	calls := 0
	read := func() (pdb.Limits, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("tracking pod disruption budgets: %w", context.Canceled)
		}
		return pdb.Limits{}, nil
	}

	if _, err := reads.pdbLimits(read); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the first candidate to see its own cancellation, got %v", err)
	}
	if _, err := reads.pdbLimits(read); err != nil {
		t.Fatalf("expected the next candidate to retry the read, got %v", err)
	}
}

func TestPassReadsConcurrentReadsShareOneResult(t *testing.T) {
	reads := NewPassReads()
	var mu sync.Mutex
	calls := 0
	read := func() ([]*corev1.Pod, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return []*corev1.Pod{testPod("a")}, nil
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//nolint:errcheck
			reads.pendingPods(read)
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("expected one read across concurrent callers, got %d", calls)
	}
}

// Callers outside a pass keep reading through, so nothing that reuses SimulateScheduling picks up
// a stale backlog by accident.
func TestPassReadsAbsentFromContext(t *testing.T) {
	if PassReadsFromContext(context.Background()) != nil {
		t.Fatal("expected no PassReads on a bare context")
	}
	ctx := WithPassReads(context.Background(), NewPassReads())
	if PassReadsFromContext(ctx) == nil {
		t.Fatal("expected PassReads to round-trip through the context")
	}
}
