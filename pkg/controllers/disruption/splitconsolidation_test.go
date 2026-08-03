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
	"testing"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

func TestSplitAttemptBudgetBoundsAPass(t *testing.T) {
	budget := disruption.NewSplitAttemptBudget(2)
	for i := range 2 {
		if !budget.TryAcquire() {
			t.Fatalf("expected attempt %d to be granted", i)
		}
	}
	if budget.TryAcquire() {
		t.Fatal("expected the budget to be exhausted")
	}
	if budget.Remaining() != 0 {
		t.Fatalf("expected no attempts remaining, got %d", budget.Remaining())
	}
}

func TestSplitAttemptBudgetDisabled(t *testing.T) {
	for _, attempts := range []int{0, -1} {
		if disruption.NewSplitAttemptBudget(attempts).TryAcquire() {
			t.Fatalf("expected a budget of %d to grant nothing", attempts)
		}
	}
}

func TestSplitAttemptBudgetFromContext(t *testing.T) {
	if disruption.SplitAttemptBudgetFromContext(context.Background()) != nil {
		t.Fatal("expected no budget outside a consolidation pass")
	}
	budget := disruption.NewSplitAttemptBudget(1)
	if disruption.SplitAttemptBudgetFromContext(disruption.WithSplitAttemptBudget(context.Background(), budget)) != budget {
		t.Fatal("expected the pass budget back from the context")
	}
}
