package report

import (
	"reflect"
	"testing"
)

func TestLayeredPointsEveryEdgeDown(t *testing.T) {
	// A diamond (0 → 1, 2 → 3), a tail (3 → 4) and a long edge (0 → 4).
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {0, 4}}
	lay := layered(5, edges)
	if len(lay.Edges) != len(edges) {
		t.Fatalf("kept %d of %d edges", len(lay.Edges), len(edges))
	}
	for _, e := range lay.Edges {
		if lay.Pos[e[0]][1] >= lay.Pos[e[1]][1] {
			t.Errorf("edge %v does not point down: %v → %v", e, lay.Pos[e[0]], lay.Pos[e[1]])
		}
	}
	if lay.Pos[1][1] != lay.Pos[2][1] || lay.Pos[1][0] == lay.Pos[2][0] {
		t.Errorf("siblings 1 and 2 should share a layer side by side: %v %v", lay.Pos[1], lay.Pos[2])
	}
	if lay.Pos[4][1] <= lay.Pos[3][1] {
		t.Error("the long edge must not pull node 4 above node 3")
	}
	for i, p := range lay.Pos {
		if p[0] < 0 || p[1] < 0 || p[0]+nodeW > lay.W || p[1]+nodeH > lay.H {
			t.Errorf("node %d at %v is outside %dx%d", i, p, lay.W, lay.H)
		}
		for j := range i {
			q := lay.Pos[j]
			if p[1] == q[1] && p[0] < q[0]+nodeW && q[0] < p[0]+nodeW {
				t.Errorf("nodes %d and %d overlap", i, j)
			}
		}
	}
	if !reflect.DeepEqual(lay, layered(5, edges)) {
		t.Error("layout is not deterministic")
	}
}

func TestLayeredSurvivesBadInput(t *testing.T) {
	lay := layered(3, [][2]int{{0, 1}, {1, 2}, {2, 0}, {1, 1}, {0, 7}, {0, 1}})
	if len(lay.Pos) != 3 {
		t.Fatalf("%d positions", len(lay.Pos))
	}
	for _, e := range lay.Edges {
		if lay.Pos[e[0]][1] >= lay.Pos[e[1]][1] {
			t.Errorf("edge %v kept pointing up", e)
		}
	}
	if empty := layered(0, nil); empty.W != 2*padding || len(empty.Edges) != 0 {
		t.Errorf("empty graph: %+v", empty)
	}
}
