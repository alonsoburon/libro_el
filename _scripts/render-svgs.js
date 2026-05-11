#!/usr/bin/env node
/**
 * Render _charts/diagrams/*.html to typst/diagrams/*.svg using jsdom.
 * Usage: node _scripts/render-svgs.js [glob]
 *   node _scripts/render-svgs.js              # render all
 *   node _scripts/render-svgs.js 0201         # render matching
 */
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

const chartsDir = path.join(__dirname, "..", "_charts", "diagrams");
const outDir = path.join(__dirname, "..", "typst", "diagrams");
const jsFile = path.join(__dirname, "..", "_charts", "ecl-charts.js");

const filter = process.argv[2] || "";

const chartJS = fs.readFileSync(jsFile, "utf-8");

const htmlFiles = fs.readdirSync(chartsDir)
  .filter(f => f.endsWith(".html") && f.includes(filter))
  .sort();

let rendered = 0;
for (const file of htmlFiles) {
  const html = fs.readFileSync(path.join(chartsDir, file), "utf-8");
  const svgName = file.replace(".html", ".svg");

  const dom = new JSDOM(html, { runScripts: "dangerously", resources: "usable" });
  const { document, window } = dom.window;

  // Inject ecl-charts.js since jsdom can't load relative <script src>
  const scriptEl = document.createElement("script");
  scriptEl.textContent = chartJS;
  document.head.appendChild(scriptEl);

  // Re-run the inline <script> that calls ECL.*
  const inlineScripts = document.querySelectorAll("script:not([src])");
  for (const s of inlineScripts) {
    try { window.eval(s.textContent); } catch (e) { /* skip button onclick etc */ }
  }

  const svgEl = document.querySelector("svg");
  if (!svgEl) {
    console.log(`  SKIP ${file} (no SVG generated)`);
    continue;
  }

  svgEl.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  const svgStr = '<?xml version="1.0" encoding="utf-8"?>\n' + svgEl.outerHTML;
  fs.writeFileSync(path.join(outDir, svgName), svgStr);
  console.log(`  ${file} → ${svgName}`);
  rendered++;
  dom.window.close();
}
console.log(`Done: ${rendered} SVGs rendered.`);
