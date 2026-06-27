// Package dagsvg renders a jobflow configuration's dependency DAG as a
// standalone SVG image. With Options.Steps it expands each job into its
// internal step DAG. It has no third-party dependencies and produces an SVG
// that renders in browsers and on GitHub.
package dagsvg

import (
	"fmt"
	"strings"

	"github.com/fermat-tech/jobflow/engine"
)

// Options controls rendering.
type Options struct {
	// Steps expands each job into a cluster box showing its step DAG, rather
	// than drawing jobs as single nodes.
	Steps bool
}

// Render returns an SVG document for the given jobs' dependency graph.
func Render(jobs []*engine.Job, opts Options) ([]byte, error) {
	if len(jobs) == 0 {
		return nil, fmt.Errorf("dagsvg: no jobs to render")
	}
	if opts.Steps {
		return renderWithSteps(jobs), nil
	}
	return renderJobs(jobs), nil
}

// Layout/style constants (px).
const (
	lineH      = 17.0
	charW      = 7.0
	padX       = 12.0
	padY       = 8.0
	minNodeW   = 96.0
	colGap     = 66.0
	rowGap     = 26.0
	margin     = 24.0
	clusterPad = 14.0
	titleH     = 26.0
	maxLabel   = 36
)

// vnode is a positioned node in a layered layout. Coordinates are relative to
// the layout origin (0,0); callers add a margin or cluster offset.
type vnode struct {
	id    string
	lines []string
	w, h  float64
	x, y  float64
}

// place assigns each node a column (by longest-path rank over deps) and a row
// within that column, returning the bounding width and height. ids fixes the
// order; deps maps a node to the nodes it depends on.
func place(ids []string, deps map[string][]string, nodes map[string]*vnode) (float64, float64) {
	rank := make(map[string]int, len(ids))
	inProgress := make(map[string]bool, len(ids))
	var rankOf func(string) int
	rankOf = func(id string) int {
		if r, ok := rank[id]; ok {
			return r
		}
		if inProgress[id] {
			return 0 // defensive: ignore back-edges of an (invalid) cycle
		}
		inProgress[id] = true
		r := 0
		for _, d := range deps[id] {
			if _, ok := nodes[d]; !ok {
				continue
			}
			if dr := rankOf(d) + 1; dr > r {
				r = dr
			}
		}
		inProgress[id] = false
		rank[id] = r
		return r
	}

	maxRank := 0
	for _, id := range ids {
		if r := rankOf(id); r > maxRank {
			maxRank = r
		}
	}
	byRank := make([][]string, maxRank+1)
	for _, id := range ids {
		byRank[rank[id]] = append(byRank[rank[id]], id)
	}

	x, maxBottom := 0.0, 0.0
	for r := 0; r <= maxRank; r++ {
		colW := 0.0
		for _, id := range byRank[r] {
			if nodes[id].w > colW {
				colW = nodes[id].w
			}
		}
		y := 0.0
		for _, id := range byRank[r] {
			n := nodes[id]
			n.x, n.y = x, y
			y += n.h + rowGap
			if n.y+n.h > maxBottom {
				maxBottom = n.y + n.h
			}
		}
		x += colW + colGap
	}
	width := x - colGap
	if width < 0 {
		width = 0
	}
	return width, maxBottom
}

func renderJobs(jobs []*engine.Job) []byte {
	nodes := make(map[string]*vnode, len(jobs))
	ids := make([]string, 0, len(jobs))
	deps := make(map[string][]string, len(jobs))
	for _, j := range jobs {
		n := &vnode{id: j.Name, lines: jobLabel(j)}
		n.w, n.h = sizeNode(n.lines)
		nodes[j.Name] = n
		ids = append(ids, j.Name)
		deps[j.Name] = j.DependsOn
	}
	w, h := place(ids, deps, nodes)

	var b strings.Builder
	svgHeader(&b, w+2*margin, h+2*margin)
	for _, j := range jobs { // edges under nodes
		to := nodes[j.Name]
		for _, dep := range j.DependsOn {
			if from := nodes[dep]; from != nil {
				edge(&b, margin+from.x+from.w, margin+from.y+from.h/2, margin+to.x, margin+to.y+to.h/2)
			}
		}
	}
	for _, id := range ids {
		n := nodes[id]
		drawNode(&b, margin+n.x, margin+n.y, n.w, n.h, n.lines, "job")
	}
	svgFooter(&b)
	return []byte(b.String())
}

func renderWithSteps(jobs []*engine.Job) []byte {
	type cluster struct {
		job   *engine.Job
		steps map[string]*vnode
		box   *vnode
	}
	clusters := make(map[string]*cluster, len(jobs))
	jobIDs := make([]string, 0, len(jobs))
	jobDeps := make(map[string][]string, len(jobs))
	jobNodes := make(map[string]*vnode, len(jobs))

	for _, j := range jobs {
		sdeps := engine.EffectiveStepDeps(j)
		snodes := make(map[string]*vnode, len(j.Steps))
		sids := make([]string, 0, len(j.Steps))
		for _, s := range j.Steps {
			n := &vnode{id: s.Name, lines: stepLabel(s)}
			n.w, n.h = sizeNode(n.lines)
			snodes[s.Name] = n
			sids = append(sids, s.Name)
		}
		iw, ih := place(sids, sdeps, snodes)

		boxW := iw + 2*clusterPad
		boxH := ih + titleH + clusterPad
		if titleW := float64(len(j.Name))*charW + 2*clusterPad; titleW > boxW {
			boxW = titleW
		}
		jn := &vnode{id: j.Name, w: boxW, h: boxH}
		jobNodes[j.Name] = jn
		clusters[j.Name] = &cluster{job: j, steps: snodes, box: jn}
		jobIDs = append(jobIDs, j.Name)
		jobDeps[j.Name] = j.DependsOn
	}
	w, h := place(jobIDs, jobDeps, jobNodes)

	var b strings.Builder
	svgHeader(&b, w+2*margin, h+2*margin)
	for _, j := range jobs { // job-dependency edges between boxes
		to := jobNodes[j.Name]
		for _, dep := range j.DependsOn {
			if from := jobNodes[dep]; from != nil {
				edge(&b, margin+from.x+from.w, margin+from.y+from.h/2, margin+to.x, margin+to.y+to.h/2)
			}
		}
	}
	for _, id := range jobIDs {
		c := clusters[id]
		bx, by := margin+c.box.x, margin+c.box.y
		fmt.Fprintf(&b, `<rect class="cluster" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, bx, by, c.box.w, c.box.h)
		fmt.Fprintf(&b, `<text class="title" x="%.1f" y="%.1f">%s</text>`, bx+clusterPad, by+17, xmlEscape(c.job.Name))

		ox, oy := bx+clusterPad, by+titleH
		sdeps := engine.EffectiveStepDeps(c.job)
		for _, s := range c.job.Steps {
			tn := c.steps[s.Name]
			for _, dep := range sdeps[s.Name] {
				if fn := c.steps[dep]; fn != nil {
					edge(&b, ox+fn.x+fn.w, oy+fn.y+fn.h/2, ox+tn.x, oy+tn.y+tn.h/2)
				}
			}
		}
		for _, s := range c.job.Steps {
			n := c.steps[s.Name]
			drawNode(&b, ox+n.x, oy+n.y, n.w, n.h, n.lines, "step")
		}
	}
	svgFooter(&b)
	return []byte(b.String())
}

func jobLabel(j *engine.Job) []string {
	lines := []string{j.Name}
	if j.Description != "" {
		lines = append(lines, truncate(j.Description, maxLabel))
	}
	if j.Schedule != "" {
		lines = append(lines, truncate("⏱ "+j.Schedule, maxLabel))
	}
	return lines
}

func stepLabel(s engine.Step) []string {
	lines := []string{s.Name}
	switch {
	case s.Command != "":
		lines = append(lines, truncate("$ "+s.Command, maxLabel))
	case s.Handler != "":
		lines = append(lines, truncate("handler: "+s.Handler, maxLabel))
	}
	return lines
}

func sizeNode(lines []string) (float64, float64) {
	maxLen := 0
	for _, ln := range lines {
		if n := len([]rune(ln)); n > maxLen {
			maxLen = n
		}
	}
	w := float64(maxLen)*charW + 2*padX
	if w < minNodeW {
		w = minNodeW
	}
	return w, float64(len(lines))*lineH + 2*padY
}

func drawNode(b *strings.Builder, x, y, w, h float64, lines []string, class string) {
	fmt.Fprintf(b, `<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="6"/>`, class, x, y, w, h)
	ty := y + padY + 12
	for i, ln := range lines {
		cls := "sub"
		if i == 0 {
			cls = "name"
		}
		fmt.Fprintf(b, `<text class="%s" x="%.1f" y="%.1f">%s</text>`, cls, x+padX, ty, xmlEscape(ln))
		ty += lineH
	}
}

func edge(b *strings.Builder, x1, y1, x2, y2 float64) {
	dx := (x2 - x1) * 0.5
	fmt.Fprintf(b, `<path class="edge" d="M%.1f,%.1f C%.1f,%.1f %.1f,%.1f %.1f,%.1f" marker-end="url(#arrow)"/>`,
		x1, y1, x1+dx, y1, x2-dx, y2, x2, y2)
}

func svgHeader(b *strings.Builder, w, h float64) {
	fmt.Fprintf(b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" width="%.0f" height="%.0f">`, w, h, w, h)
	b.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>`)
	b.WriteString(`<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="7" refY="3" orient="auto">` +
		`<path d="M0,0 L7,3 L0,6 Z" fill="#475569"/></marker></defs>`)
	b.WriteString(`<style>` +
		`text{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;fill:#0f172a}` +
		`.name{font-weight:600;font-size:13px}.sub{font-size:11px;fill:#475569}` +
		`.title{font-weight:600;font-size:12px;fill:#334155}` +
		`rect.job{fill:#eef2ff;stroke:#6366f1;stroke-width:1.5}` +
		`rect.step{fill:#ecfdf5;stroke:#10b981;stroke-width:1.5}` +
		`rect.cluster{fill:#f8fafc;stroke:#94a3b8;stroke-width:1.2}` +
		`.edge{fill:none;stroke:#475569;stroke-width:1.4}` +
		`</style>`)
}

func svgFooter(b *strings.Builder) { b.WriteString(`</svg>`) }

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")
	return r.Replace(s)
}
