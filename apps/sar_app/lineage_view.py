"""Renders the bronze -> silver -> gold lineage map as a self-contained
HTML/CSS/JS component (via ``streamlit.components.v1.html``).

Graphviz's default renderer (used by ``st.graphviz_chart``) draws on a white
background with plain boxes and no control over badges/pills/typography, and
its automatic layout doesn't guarantee clean routing for a variable number of
nodes. This module instead lays nodes out with plain CSS (so it always
matches the app's dark theme) and draws connector lines with a small
JavaScript pass that measures each node's actual on-screen position via
``getBoundingClientRect()`` after layout — so the lines always line up with
the boxes, regardless of how many tables are shown or how they wrap.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

LAYER_ORDER = ["bronze", "silver", "gold"]

STATUS_STYLE = {
    "direct":     {"badge": "DIRECT MATCH",    "color": "#5b8def", "bg": "rgba(91,141,239,0.16)"},
    "upstream":   {"badge": "UPSTREAM SOURCE", "color": "#9b6bff", "bg": "rgba(155,107,255,0.18)"},
    "downstream": {"badge": "DOWNSTREAM COPY", "color": "#2dd4bf", "bg": "rgba(45,212,191,0.16)"},
    "traversed":  {"badge": None,              "color": "#7d8698", "bg": "rgba(125,134,152,0.06)"},
}


@dataclass
class LineageNode:
    full_name: str
    layer: str  # bronze | silver | gold
    status: str  # direct | upstream | downstream | traversed
    row_count: int | None = None
    caption: str | None = None


@dataclass
class LineageEdge:
    source: str
    target: str
    color: str
    label: str | None = None


def _node_id(full_name: str) -> str:
    return "n_" + "".join(c if c.isalnum() else "_" for c in full_name)


def render(nodes: list[LineageNode], edges: list[LineageEdge]) -> tuple[str, int]:
    """Return ``(html_document, suggested_height_px)``."""
    columns = {layer: [n for n in nodes if n.layer == layer] for layer in LAYER_ORDER}
    max_col_len = max((len(v) for v in columns.values()), default=1)
    height = max(220, 90 + max_col_len * 100)

    def node_html(n: LineageNode) -> str:
        style = STATUS_STYLE[n.status]
        border_style = "dashed" if n.status == "traversed" else "solid"
        badge_html = (
            f'<span class="badge" style="color:{style["color"]};background:{style["bg"]}">'
            f'<span class="dot" style="background:{style["color"]}"></span>{style["badge"]}</span>'
            if style["badge"] else ""
        )
        caption = n.caption or (f"{n.row_count} row(s)" if n.row_count is not None else "")
        return f'''<div class="node" id="{_node_id(n.full_name)}" style="border-style:{border_style};border-color:{style["color"]}">
  <div class="tname">{n.full_name}</div>
  {badge_html}
  <div class="meta">{caption}</div>
</div>'''

    columns_html = ""
    for layer in LAYER_ORDER:
        col_nodes = columns[layer]
        if not col_nodes:
            continue
        columns_html += f'''<div class="col">
  <div class="col-label">{layer.upper()}</div>
  {"".join(node_html(n) for n in col_nodes)}
</div>
'''

    edges_json = json.dumps([
        {"source": _node_id(e.source), "target": _node_id(e.target), "label": e.label, "color": e.color}
        for e in edges
    ])

    html = f'''<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  html, body {{ margin: 0; padding: 0; background: #0a0d13; font-family: -apple-system, "Segoe UI", sans-serif; }}
  .wrap {{ position: relative; padding: 20px 24px; box-sizing: border-box; }}
  .grid {{ display: flex; gap: 48px; align-items: flex-start; position: relative; z-index: 2; }}
  .col {{ flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 14px; }}
  .col-label {{
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;
    color: #7d8698; margin-bottom: 2px;
  }}
  .node {{
    background: #141a24; border: 1.5px solid #333c4d; border-radius: 6px;
    padding: 10px 13px; font-size: 13px; color: #eef0f5; box-sizing: border-box;
  }}
  .tname {{ font-family: ui-monospace, "SF Mono", Consolas, monospace; font-size: 13px; margin-bottom: 6px; word-break: break-all; }}
  .meta {{ color: #7d8698; font-size: 11px; margin-top: 6px; }}
  .badge {{
    display: inline-flex; align-items: center; gap: 5px; font-size: 10px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.03em; padding: 3px 8px; border-radius: 999px;
  }}
  .badge .dot {{ width: 5px; height: 5px; border-radius: 50%; }}
  svg {{ position: absolute; top: 0; left: 0; z-index: 1; pointer-events: none; }}
  .edge-label {{ font-family: ui-monospace, "SF Mono", Consolas, monospace; }}
</style>
</head>
<body>
<div class="wrap" id="wrap">
  <svg id="svg"></svg>
  <div class="grid">
{columns_html}  </div>
</div>
<script>
  const edges = {edges_json};

  function draw() {{
    const wrap = document.getElementById('wrap');
    const svg = document.getElementById('svg');
    const wrapRect = wrap.getBoundingClientRect();
    svg.setAttribute('width', wrap.scrollWidth);
    svg.setAttribute('height', wrap.scrollHeight);
    svg.innerHTML = '';

    edges.forEach(e => {{
      const srcEl = document.getElementById(e.source);
      const tgtEl = document.getElementById(e.target);
      if (!srcEl || !tgtEl) return;
      const s = srcEl.getBoundingClientRect();
      const t = tgtEl.getBoundingClientRect();

      const x1 = s.right - wrapRect.left;
      const y1 = s.top + s.height / 2 - wrapRect.top;
      const x2 = t.left - wrapRect.left;
      const y2 = t.top + t.height / 2 - wrapRect.top;
      const midX = (x1 + x2) / 2;

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', `M ${{x1}} ${{y1}} C ${{midX}} ${{y1}}, ${{midX}} ${{y2}}, ${{x2}} ${{y2}}`);
      path.setAttribute('stroke', e.color);
      path.setAttribute('stroke-width', '2');
      path.setAttribute('fill', 'none');
      path.setAttribute('opacity', '0.85');
      svg.appendChild(path);

      if (e.label) {{
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', midX);
        text.setAttribute('y', (y1 + y2) / 2 - 8);
        text.setAttribute('fill', '#9aa3b5');
        text.setAttribute('font-size', '10');
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('class', 'edge-label');
        text.textContent = e.label;
        svg.appendChild(text);
      }}
    }});
  }}

  window.addEventListener('load', draw);
  new ResizeObserver(draw).observe(document.getElementById('wrap'));
</script>
</body>
</html>'''

    return html, height
