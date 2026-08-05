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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func podWithUID(uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: uid, UID: types.UID(uid)}}
}

func claimWith(pods ...*corev1.Pod) *scheduling.NodeClaim {
	return &scheduling.NodeClaim{Pods: pods}
}

func TestReplacementsAttributableToDisruption(t *testing.T) {
	candidatePod, otherCandidatePod, backlogPod := podWithUID("candidate"), podWithUID("candidate-2"), podWithUID("backlog")
	onDeletingNodePod := podWithUID("on-deleting-node")
	disrupted := sets.New[types.UID]("candidate", "candidate-2")

	forCandidate := claimWith(candidatePod)
	forBacklog := claimWith(backlogPod)
	mixed := claimWith(backlogPod, otherCandidatePod)
	forDeletingNode := claimWith(onDeletingNodePod)

	for _, tc := range []struct {
		name      string
		claims    []*scheduling.NodeClaim
		disrupted sets.Set[types.UID]
		want      []*scheduling.NodeClaim
	}{
		{
			// The shape that turns a delete into a replacement during a burst: the candidate's pods
			// all landed on existing nodes and every new claim belongs to the backlog.
			name:      "backlog-only claims drop out",
			claims:    []*scheduling.NodeClaim{forBacklog},
			disrupted: disrupted,
			want:      nil,
		},
		{
			name:      "the candidate's own replacement is kept",
			claims:    []*scheduling.NodeClaim{forCandidate, forBacklog},
			disrupted: disrupted,
			want:      []*scheduling.NodeClaim{forCandidate},
		},
		{
			// The disruption needs this claim, so it is kept whole rather than repriced for the
			// share of it the candidate uses.
			name:      "a claim shared with the backlog is kept",
			claims:    []*scheduling.NodeClaim{mixed},
			disrupted: disrupted,
			want:      []*scheduling.NodeClaim{mixed},
		},
		{
			// Pods on already-deleting nodes are in the disrupted set, so a claim opened for them
			// is kept. They look like backlog but nothing else owns them yet, and a pass admitting
			// several commands re-simulates each proposal against the nodes the earlier ones just
			// marked for deletion: dropping their claim lets a later proposal spend capacity an
			// earlier command already took.
			name:      "a claim for pods on an already-deleting node is kept",
			claims:    []*scheduling.NodeClaim{forDeletingNode},
			disrupted: sets.New[types.UID]("candidate", "candidate-2", "on-deleting-node"),
			want:      []*scheduling.NodeClaim{forDeletingNode},
		},
		{
			// Every pod was withheld from the simulation, so nothing is attributable and the
			// unattributed behavior stands rather than becoming a delete.
			name:      "no disrupted pods leaves the result alone",
			claims:    []*scheduling.NodeClaim{forBacklog},
			disrupted: sets.New[types.UID](),
			want:      []*scheduling.NodeClaim{forBacklog},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := replacementsAttributableToDisruption(tc.claims, tc.disrupted)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d claims, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("claim %d: got %v, want %v", i, got[i].Pods, tc.want[i].Pods)
				}
			}
		})
	}
}
