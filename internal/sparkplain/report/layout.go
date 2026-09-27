package report

import "sort"

// Graph layout for the explorer's stage DAGs and SQL plan graphs (SPEC §6:
// the layout is done here, in Go, and the page's script draws it). It is a small layered layout: each node goes
// in the layer after its deepest predecessor, nodes within a layer are
// ordered by the average position of their neighbours to cut crossings, and
// every edge points down.

// Node box size and gaps, in SVG units.
const (
	nodeW   = 200
	nodeH   = 48
	gapX    = 28
	gapY    = 44
	padding = 8
)

// xLayout is a laid-out graph: Pos[i] is node i's top-left corner, and
// every edge in Edges goes from a node in a higher layer to a lower one.
type xLayout struct {
	W     int      `json:"w"`
	H     int      `json:"h"`
	Pos   [][2]int `json:"pos"`
	Edges [][2]int `json:"edges"` // from, to (data flows from → to)
}

// layered lays out n nodes joined by edges (from → to). Edges that would
// close a cycle are dropped; Spark's graphs have none.
func layered(n int, edges [][2]int) xLayout {
	out := make([][]int, n)
	in := make([][]int, n)
	seen := map[[2]int]bool{}
	var kept [][2]int
	for _, e := range edges {
		if e[0] == e[1] || e[0] < 0 || e[1] < 0 || e[0] >= n || e[1] >= n || seen[e] {
			continue
		}
		seen[e] = true
		out[e[0]] = append(out[e[0]], e[1])
		in[e[1]] = append(in[e[1]], e[0])
		kept = append(kept, e)
	}

	// Layers by longest path from the sources (Kahn's algorithm).
	layer := make([]int, n)
	deg := make([]int, n)
	for i := range n {
		deg[i] = len(in[i])
	}
	var queue, order []int
	for i := range n {
		if deg[i] == 0 {
			queue = append(queue, i)
		}
	}
	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]
		order = append(order, v)
		for _, w := range out[v] {
			layer[w] = max(layer[w], layer[v]+1)
			if deg[w]--; deg[w] == 0 {
				queue = append(queue, w)
			}
		}
	}
	placed := make([]bool, n)
	for _, v := range order {
		placed[v] = true
	}
	top := 0
	for _, v := range order {
		top = max(top, layer[v])
	}
	for i := range n { // nodes on a cycle: put them below everything else
		if !placed[i] {
			top++
			layer[i] = top
		}
	}
	var dag [][2]int
	for _, e := range kept {
		if layer[e[0]] < layer[e[1]] {
			dag = append(dag, e)
		}
	}

	// Order within layers: start in index order, then sweep down and up
	// placing each node at the average position of its neighbours.
	nl := 0
	for i := range n {
		nl = max(nl, layer[i]+1)
	}
	rows := make([][]int, nl)
	for i := range n {
		rows[layer[i]] = append(rows[layer[i]], i)
	}
	pos := make([]float64, n)
	renumber := func(r []int) {
		for k, v := range r {
			pos[v] = float64(k)
		}
	}
	for _, r := range rows {
		renumber(r)
	}
	bary := func(r []int, nbr [][]int) {
		key := make(map[int]float64, len(r))
		for _, v := range r {
			if len(nbr[v]) == 0 {
				key[v] = pos[v]
				continue
			}
			s := 0.0
			for _, u := range nbr[v] {
				s += pos[u]
			}
			key[v] = s / float64(len(nbr[v]))
		}
		sort.SliceStable(r, func(a, b int) bool { return key[r[a]] < key[r[b]] })
		renumber(r)
	}
	for range 4 {
		for l := 1; l < nl; l++ {
			bary(rows[l], in)
		}
		for l := nl - 2; l >= 0; l-- {
			bary(rows[l], out)
		}
	}

	widest := 0
	for _, r := range rows {
		widest = max(widest, len(r))
	}
	lay := xLayout{Pos: make([][2]int, n), Edges: dag}
	lay.W = padding*2 + widest*nodeW + max(0, widest-1)*gapX
	lay.H = padding*2 + nl*nodeH + max(0, nl-1)*gapY
	for l, r := range rows {
		rowW := len(r)*nodeW + max(0, len(r)-1)*gapX
		x0 := padding + (lay.W-2*padding-rowW)/2
		for k, v := range r {
			lay.Pos[v] = [2]int{x0 + k*(nodeW+gapX), padding + l*(nodeH+gapY)}
		}
	}
	if lay.Edges == nil {
		lay.Edges = [][2]int{}
	}
	return lay
}
