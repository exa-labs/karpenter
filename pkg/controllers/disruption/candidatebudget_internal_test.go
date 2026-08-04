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
	"testing"
	"time"
)

func TestCandidateBudgetExhausted(t *testing.T) {
	for _, tc := range []struct {
		name         string
		candidateErr error
		parentErr    error
		want         bool
	}{
		{
			name: "simulation finished inside its budget",
			want: false,
		},
		{
			// A cancelled solve returns partial results, so an exhausted budget discards the
			// verdict whether or not the simulation reported an error.
			name:         "candidate ran out of budget",
			candidateErr: context.DeadlineExceeded,
			want:         true,
		},
		{
			// Shutdown cancels the parent, which cancels the candidate with it. That is not a
			// candidate the walk should record as too slow.
			name:         "pass is shutting down",
			candidateErr: context.Canceled,
			parentErr:    context.Canceled,
			want:         false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := candidateBudgetExhausted(tc.candidateErr, tc.parentErr); got != tc.want {
				t.Fatalf("candidateBudgetExhausted(%v, %v) = %v, want %v", tc.candidateErr, tc.parentErr, got, tc.want)
			}
		})
	}
}

// The pass-scoped caches hang off the context, so a per-candidate deadline must not detach them:
// a candidate that derives its own context still has to hit the pass's memoized reads.
func TestCandidateContextKeepsPassScopedValues(t *testing.T) {
	reads := NewPassReads()
	ctx := WithPassReads(context.Background(), reads)
	candidateCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if PassReadsFromContext(candidateCtx) != reads {
		t.Fatal("expected the candidate context to inherit the pass's reads")
	}
}
