/**
 * ECL Charts — diagram framework for the ECL Patterns book.
 * Vanilla JS, generates inline SVG. No dependencies.
 *
 * Diagram types:
 *   ECL.segment()    — horizontal colored bars with dimension-line annotations
 *   ECL.timeline()   — time axis with extraction windows, cursors, markers
 *   ECL.tableState() — side-by-side tables with color-coded rows
 *   ECL.tradeoff()   — 2D scatter plot with labeled points
 */

const ECL = {
  theme: {
    bg: "#181825",
    text: "#cdd6f4",
    textDim: "#a6adc8",
    textMuted: "#6c7086",
    line: "#585b70",
    lineFaint: "#313244",
    font: "'Segoe UI', system-ui, sans-serif",
    palette: {
      muted_gray: "#585b70",
      steel_blue: "#6c8ebf",
      olive: "#b8a44e",
      forest: "#5aab7b",
      coral: "#d46a6a",
      purple: "#8878b8",
      amber: "#d4a040",
      slate: "#7888a0",
      teal: "#58b0a0",
      rose: "#c07088",
    },
  },

  color(c) {
    if (!c) return "none";
    return c.startsWith("#") ? c : this.theme.palette[c] || c;
  },

  svg(tag, attrs = {}, parent = null) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v !== undefined && v !== null) el.setAttribute(k, String(v));
    }
    if (parent) parent.appendChild(el);
    return el;
  },

  /** Multi-line SVG text helper. Returns y after last line. */
  _text(svgEl, x, y, text, attrs = {}) {
    const lines = String(text).split("\n");
    const lineH = parseFloat(attrs["font-size"] || 12) + 3;
    lines.forEach((line, i) => {
      this.svg("text", { x, y: y + i * lineH, ...attrs }, svgEl).textContent = line;
    });
    return y + (lines.length - 1) * lineH;
  },

  saveSVG(container, filename) {
    const svgEl = container.querySelector("svg");
    if (!svgEl) return;
    const clone = svgEl.cloneNode(true);
    clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    const blob = new Blob(
      ['<?xml version="1.0" encoding="utf-8"?>\n' + clone.outerHTML],
      { type: "image/svg+xml" }
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename.endsWith(".svg") ? filename : filename + ".svg";
    a.click();
    URL.revokeObjectURL(a.href);
  },

  /** Dimension-line: horizontal line + ticks + centered label. */
  _dimLine(svgEl, x1, x2, y, text, { above = true, T, tickH = 6 } = {}) {
    const textY = above ? y - 8 : y + 14;
    this.svg("line", { x1, y1: y, x2, y2: y, stroke: T.line, "stroke-width": 1 }, svgEl);
    this.svg("line", { x1, y1: y - tickH / 2, x2: x1, y2: y + tickH / 2, stroke: T.line, "stroke-width": 1 }, svgEl);
    this.svg("line", { x1: x2, y1: y - tickH / 2, x2, y2: y + tickH / 2, stroke: T.line, "stroke-width": 1 }, svgEl);
    this.svg("text", {
      x: (x1 + x2) / 2, y: textY,
      "text-anchor": "middle", fill: T.textDim,
      "font-family": T.font, "font-size": 11,
    }, svgEl).textContent = text;
  },


  // ════════════════════════════════════════════════════════════════
  //  SEGMENT DIAGRAM
  // ════════════════════════════════════════════════════════════════

  segment(container, config) {
    const {
      title = null, width = 800, barHeight = 56,
      segments = [], bracesTop = [], bracesBottom = [],
    } = config;

    const T = this.theme;
    const padX = 40, segGap = 2;
    const annRowH = 28, annGap = 8;

    const sortBySpan = (a, b) => (a[1] - a[0]) - (b[1] - b[0]);
    const topSorted = [...bracesTop].sort(sortBySpan);
    const botSorted = [...bracesBottom].sort(sortBySpan);

    const titleH = title ? 32 : 0;
    const topZone = topSorted.length ? topSorted.length * annRowH + annGap : 6;
    const botZone = botSorted.length ? botSorted.length * annRowH + annGap : 6;
    const barY = titleH + topZone;
    const totalH = barY + barHeight + botZone + 4;

    const svgEl = this.svg("svg", { width, height: totalH, viewBox: `0 0 ${width} ${totalH}` });
    this.svg("rect", { x: 0, y: 0, width, height: totalH, fill: T.bg, rx: 6 }, svgEl);

    if (title) {
      this.svg("text", {
        x: width / 2, y: 20, "text-anchor": "middle", fill: T.textDim,
        "font-family": T.font, "font-size": 13, "letter-spacing": "0.3",
      }, svgEl).textContent = title;
    }

    const totalW = segments.reduce((s, seg) => s + seg.width, 0);
    const totalGaps = Math.max(0, segments.length - 1) * segGap;
    const usable = width - 2 * padX - totalGaps;
    const pos = [];
    let cx = padX;
    segments.forEach((seg) => {
      const pw = (seg.width / totalW) * usable;
      pos.push({ x: cx, w: pw });
      cx += pw + segGap;
    });

    segments.forEach((seg, i) => {
      const p = pos[i];
      this.svg("rect", {
        x: p.x, y: barY, width: p.w, height: barHeight,
        fill: this.color(seg.color), rx: 3,
      }, svgEl);
      const lines = seg.label.split("\n");
      const lineH = 15;
      const baseY = barY + barHeight / 2 - ((lines.length - 1) * lineH) / 2;
      lines.forEach((line, j) => {
        this.svg("text", {
          x: p.x + p.w / 2, y: baseY + j * lineH,
          "text-anchor": "middle", "dominant-baseline": "central",
          fill: T.text, "font-family": T.font, "font-size": 12, "font-weight": "600",
        }, svgEl).textContent = line;
      });
    });

    topSorted.forEach(([si, ei, text], row) => {
      const x1 = pos[si].x, x2 = pos[ei].x + pos[ei].w;
      this._dimLine(svgEl, x1, x2, barY - annGap - row * annRowH, text, { above: true, T });
    });

    botSorted.forEach(([si, ei, text], row) => {
      const x1 = pos[si].x, x2 = pos[ei].x + pos[ei].w;
      this._dimLine(svgEl, x1, x2, barY + barHeight + annGap + row * annRowH, text, { above: false, T });
    });

    container.appendChild(svgEl);
    return svgEl;
  },


  // ════════════════════════════════════════════════════════════════
  //  TIMELINE DIAGRAM
  // ════════════════════════════════════════════════════════════════

  timeline(container, config) {
    const {
      title = null, width = 800,
      axis = [],
      spans = [],       // { start, end, label, color, row }
      cursors = [],     // { pos, label, color }
      markers = [],     // { pos, label, color, labelBelow }
    } = config;

    const T = this.theme;
    const padX = 50, padTop = title ? 36 : 10;
    const cursorLabelH = cursors.length ? 20 : 0;
    const axisY = padTop + cursorLabelH + 10;
    const rowH = 34;
    const maxRow = spans.length ? Math.max(...spans.map(s => s.row || 0)) : 0;
    const spanBaseY = axisY + 10;
    const spanZoneBottom = spanBaseY + (maxRow + 1) * rowH;
    const markerZoneH = markers.length ? 30 : 0;
    const axisLabelY = spanZoneBottom + markerZoneH + 14;
    const totalH = axisLabelY + 14;

    const svgEl = this.svg("svg", { width, height: totalH, viewBox: `0 0 ${width} ${totalH}` });
    this.svg("rect", { x: 0, y: 0, width, height: totalH, fill: T.bg, rx: 6 }, svgEl);

    if (title) {
      this.svg("text", {
        x: width / 2, y: 22, "text-anchor": "middle", fill: T.textDim,
        "font-family": T.font, "font-size": 13, "letter-spacing": "0.3",
      }, svgEl).textContent = title;
    }

    const axisLen = width - 2 * padX;
    const maxVal = axis.length - 1 || 1;
    const toX = (val) => padX + (val / maxVal) * axisLen;

    // Axis line
    this.svg("line", {
      x1: padX, y1: axisY, x2: padX + axisLen, y2: axisY,
      stroke: T.line, "stroke-width": 1,
    }, svgEl);

    // Axis ticks + labels
    axis.forEach((label, i) => {
      const x = toX(i);
      this.svg("line", { x1: x, y1: axisY - 3, x2: x, y2: axisY + 3, stroke: T.line, "stroke-width": 1 }, svgEl);
      this.svg("text", {
        x, y: axisLabelY, "text-anchor": "middle", fill: T.textMuted,
        "font-family": T.font, "font-size": 10,
      }, svgEl).textContent = label;
    });

    // Spans
    spans.forEach((span) => {
      const x1 = toX(span.start), x2 = toX(span.end);
      const row = span.row || 0;
      const sy = spanBaseY + row * rowH;
      const sh = rowH - 6;
      this.svg("rect", {
        x: x1, y: sy, width: x2 - x1, height: sh,
        fill: this.color(span.color), rx: 3, opacity: 0.85,
      }, svgEl);
      if (span.label) {
        // Clip text to span width
        const g = this.svg("g", {}, svgEl);
        const clipId = `clip-${Math.random().toString(36).slice(2, 8)}`;
        const defs = this.svg("defs", {}, g);
        const clipPath = this.svg("clipPath", { id: clipId }, defs);
        this.svg("rect", { x: x1 + 4, y: sy, width: x2 - x1 - 8, height: sh }, clipPath);
        this.svg("text", {
          x: (x1 + x2) / 2, y: sy + sh / 2,
          "text-anchor": "middle", "dominant-baseline": "central",
          fill: T.text, "font-family": T.font, "font-size": 10.5, "font-weight": "600",
          "clip-path": `url(#${clipId})`,
        }, g).textContent = span.label;
      }
    });

    // Cursors (vertical dashed lines)
    cursors.forEach((cur, ci) => {
      const x = toX(cur.pos);
      this.svg("line", {
        x1: x, y1: axisY - 4, x2: x, y2: spanZoneBottom - 4,
        stroke: this.color(cur.color || "amber"), "stroke-width": 1.5,
        "stroke-dasharray": "4,3",
      }, svgEl);
      if (cur.label) {
        this.svg("text", {
          x: x, y: axisY - 8, "text-anchor": "middle",
          fill: this.color(cur.color || "amber"),
          "font-family": T.font, "font-size": 10, "font-weight": "600",
        }, svgEl).textContent = cur.label;
      }
    });

    // Markers (dots with label below the spans)
    markers.forEach((m) => {
      const x = toX(m.pos);
      const my = spanZoneBottom + 6;
      this.svg("circle", { cx: x, cy: my, r: 5, fill: this.color(m.color || "coral") }, svgEl);
      // Thin line from marker up to axis
      this.svg("line", {
        x1: x, y1: axisY, x2: x, y2: my - 5,
        stroke: this.color(m.color || "coral"), "stroke-width": 1, "stroke-dasharray": "2,2",
      }, svgEl);
      if (m.label) {
        this.svg("text", {
          x: x + 8, y: my + 4, "text-anchor": "start",
          fill: this.color(m.color || "coral"),
          "font-family": T.font, "font-size": 9, "font-weight": "600",
        }, svgEl).textContent = m.label;
      }
    });

    container.appendChild(svgEl);
    return svgEl;
  },


  // ════════════════════════════════════════════════════════════════
  //  TABLE STATE DIAGRAM
  // ════════════════════════════════════════════════════════════════

  tableState(container, config) {
    const {
      title = null,
      tables = [],   // { label, columns, rows: [{ data, color, annotation }] }
      arrow = true,
    } = config;

    const T = this.theme;
    const cellH = 30, headerH = 32;
    const fontSize = 11;

    // Collect all annotations into a legend
    const legend = [];
    tables.forEach((table) => {
      table.rows.forEach((row) => {
        if (row.annotation && row.color) {
          const existing = legend.find(l => l.text === row.annotation);
          if (!existing) legend.push({ text: row.annotation, color: row.color });
        }
      });
    });

    // Auto-size: compute width from content
    const nTables = tables.length;
    const maxCols = Math.max(...tables.map(t => t.columns.length));
    const colW = 90;
    const tableW = maxCols * colW;
    const tableGap = arrow ? 50 : 24;
    const padX = 30;
    const width = padX * 2 + nTables * tableW + (nTables - 1) * tableGap;
    const padY = title ? 44 : 18;

    const maxRows = Math.max(...tables.map(t => t.rows.length));
    const tableH = headerH + maxRows * cellH;
    const legendH = legend.length ? 34 : 0;
    const totalH = padY + tableH + legendH + 16;

    const svgEl = this.svg("svg", { width, height: totalH, viewBox: `0 0 ${width} ${totalH}` });
    this.svg("rect", { x: 0, y: 0, width, height: totalH, fill: T.bg, rx: 6 }, svgEl);

    if (title) {
      this.svg("text", {
        x: width / 2, y: 26, "text-anchor": "middle", fill: T.textDim,
        "font-family": T.font, "font-size": 13, "letter-spacing": "0.3",
      }, svgEl).textContent = title;
    }

    tables.forEach((table, ti) => {
      const tx = padX + ti * (tableW + tableGap);
      const ty = padY;
      const cw = tableW / table.columns.length;

      // Table background
      this.svg("rect", {
        x: tx, y: ty, width: tableW, height: tableH,
        fill: "#1e1e34", stroke: T.line, "stroke-width": 1, rx: 4,
      }, svgEl);

      // Table label
      this.svg("text", {
        x: tx + tableW / 2, y: ty - 8, "text-anchor": "middle",
        fill: T.textDim, "font-family": T.font, "font-size": 11, "font-weight": "600",
      }, svgEl).textContent = table.label;

      // Header
      this.svg("rect", { x: tx, y: ty, width: tableW, height: headerH, fill: "#252540", rx: 4 }, svgEl);
      this.svg("rect", { x: tx, y: ty + headerH - 6, width: tableW, height: 6, fill: "#252540" }, svgEl);

      table.columns.forEach((col, ci) => {
        this.svg("text", {
          x: tx + ci * cw + cw / 2, y: ty + headerH / 2,
          "text-anchor": "middle", "dominant-baseline": "central",
          fill: T.textDim, "font-family": T.font, "font-size": 10, "font-weight": "600",
        }, svgEl).textContent = col;
      });

      this.svg("line", {
        x1: tx, y1: ty + headerH, x2: tx + tableW, y2: ty + headerH,
        stroke: T.line, "stroke-width": 1,
      }, svgEl);

      // Data rows
      table.rows.forEach((row, ri) => {
        const ry = ty + headerH + ri * cellH;

        // Row background tint for colored rows
        if (row.color) {
          this.svg("rect", {
            x: tx + 1, y: ry + 1, width: tableW - 2, height: cellH - 2,
            fill: this.color(row.color), opacity: 0.12, rx: 2,
          }, svgEl);
          // Left accent strip
          this.svg("rect", {
            x: tx + 1, y: ry + 2, width: 3, height: cellH - 4,
            fill: this.color(row.color), rx: 1,
          }, svgEl);
        }

        if (ri > 0) {
          this.svg("line", {
            x1: tx + 8, y1: ry, x2: tx + tableW - 8, y2: ry,
            stroke: T.lineFaint, "stroke-width": 0.5,
          }, svgEl);
        }

        row.data.forEach((val, ci) => {
          const displayVal = val === null ? "—" : String(val);
          this.svg("text", {
            x: tx + ci * cw + cw / 2, y: ry + cellH / 2,
            "text-anchor": "middle", "dominant-baseline": "central",
            fill: val === null ? T.textMuted : T.text,
            "font-family": T.font, "font-size": fontSize,
          }, svgEl).textContent = displayVal;
        });
      });
    });

    // Arrow between tables
    if (arrow && nTables >= 2) {
      const ax = padX + tableW + tableGap / 2;
      const ay = padY + tableH / 2;
      this.svg("line", {
        x1: ax - 14, y1: ay, x2: ax + 10, y2: ay,
        stroke: T.textMuted, "stroke-width": 1.5,
      }, svgEl);
      this.svg("polygon", {
        points: `${ax + 10},${ay - 4} ${ax + 18},${ay} ${ax + 10},${ay + 4}`,
        fill: T.textMuted,
      }, svgEl);
    }

    // Legend below tables
    if (legend.length) {
      const legendY = padY + tableH + 16;
      let lx = padX;
      legend.forEach((item) => {
        this.svg("rect", { x: lx, y: legendY - 4, width: 10, height: 10, fill: this.color(item.color), rx: 2 }, svgEl);
        const txt = this.svg("text", {
          x: lx + 14, y: legendY + 3, fill: T.textDim,
          "font-family": T.font, "font-size": 10,
        }, svgEl);
        txt.textContent = item.text;
        lx += item.text.length * 6.5 + 30;
      });
    }

    container.appendChild(svgEl);
    return svgEl;
  },


  // ════════════════════════════════════════════════════════════════
  //  TRADEOFF CHART (2D scatter with labeled points)
  // ════════════════════════════════════════════════════════════════

  tradeoff(container, config) {
    const {
      title = null, width = 700, height = 440,
      xLabel = "", yLabel = "",
      xTicks = [], yTicks = [],
      points = [],
      regions = [],
    } = config;

    const T = this.theme;
    const padL = 70, padR = 40, padT = title ? 44 : 24, padB = 52;
    const plotW = width - padL - padR;
    const plotH = height - padT - padB;

    const toX = (v) => padL + v * plotW;
    const toY = (v) => padT + (1 - v) * plotH;

    const svgEl = this.svg("svg", { width, height, viewBox: `0 0 ${width} ${height}` });
    this.svg("rect", { x: 0, y: 0, width, height, fill: T.bg, rx: 6 }, svgEl);

    if (title) {
      this.svg("text", {
        x: width / 2, y: 26, "text-anchor": "middle", fill: T.textDim,
        "font-family": T.font, "font-size": 14, "letter-spacing": "0.3",
      }, svgEl).textContent = title;
    }

    // Background regions
    regions.forEach((r) => {
      this.svg("rect", {
        x: toX(r.x), y: toY(r.y + r.h), width: r.w * plotW, height: r.h * plotH,
        fill: this.color(r.color), opacity: 0.1, rx: 6,
      }, svgEl);
      if (r.label) {
        this.svg("text", {
          x: toX(r.x + r.w / 2), y: toY(r.y + r.h) + 16,
          "text-anchor": "middle",
          fill: T.textMuted, "font-family": T.font, "font-size": 10, "font-style": "italic",
        }, svgEl).textContent = r.label;
      }
    });

    // Grid
    for (let i = 0; i <= 4; i++) {
      const v = i / 4;
      this.svg("line", { x1: toX(0), y1: toY(v), x2: toX(1), y2: toY(v), stroke: T.lineFaint, "stroke-width": 0.5 }, svgEl);
      this.svg("line", { x1: toX(v), y1: toY(0), x2: toX(v), y2: toY(1), stroke: T.lineFaint, "stroke-width": 0.5 }, svgEl);
    }

    // Axes
    this.svg("line", { x1: toX(0), y1: toY(0), x2: toX(1), y2: toY(0), stroke: T.line, "stroke-width": 1 }, svgEl);
    this.svg("line", { x1: toX(0), y1: toY(0), x2: toX(0), y2: toY(1), stroke: T.line, "stroke-width": 1 }, svgEl);

    // X label
    this.svg("text", {
      x: toX(0.5), y: height - 8, "text-anchor": "middle",
      fill: T.textDim, "font-family": T.font, "font-size": 12,
    }, svgEl).textContent = xLabel;

    // Y label (rotated via writing-mode for better SVG compat)
    this.svg("text", {
      x: 18, y: padT + plotH / 2, "text-anchor": "middle",
      fill: T.textDim, "font-family": T.font, "font-size": 12,
      transform: `rotate(-90, 18, ${padT + plotH / 2})`,
    }, svgEl).textContent = yLabel;

    // Axis ticks
    if (xTicks.length) {
      xTicks.forEach((label, i) => {
        const v = i / (xTicks.length - 1);
        this.svg("text", {
          x: toX(v), y: toY(0) + 18, "text-anchor": "middle",
          fill: T.textMuted, "font-family": T.font, "font-size": 10,
        }, svgEl).textContent = label;
      });
    }
    if (yTicks.length) {
      yTicks.forEach((label, i) => {
        const v = i / (yTicks.length - 1);
        this.svg("text", {
          x: toX(0) - 10, y: toY(v) + 4, "text-anchor": "end",
          fill: T.textMuted, "font-family": T.font, "font-size": 10,
        }, svgEl).textContent = label;
      });
    }

    // Points
    points.forEach((p) => {
      const px = toX(p.x), py = toY(p.y);
      const r = p.size || 7;
      const c = this.color(p.color || "steel_blue");

      this.svg("circle", { cx: px, cy: py, r: r + 4, fill: c, opacity: 0.12 }, svgEl);
      this.svg("circle", { cx: px, cy: py, r, fill: c }, svgEl);

      if (p.label) {
        // Default label position: right of dot. Allow overrides.
        const lx = p.labelX !== undefined ? p.labelX : px + r + 8;
        const ly = p.labelY !== undefined ? p.labelY : py + 4;
        const anchor = p.labelAnchor || "start";
        this.svg("text", {
          x: lx, y: ly, "text-anchor": anchor,
          fill: c, "font-family": T.font, "font-size": 11, "font-weight": "600",
        }, svgEl).textContent = p.label;
      }
    });

    container.appendChild(svgEl);
    return svgEl;
  },
};
