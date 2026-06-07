# Editorial Handoff -- Battle-Tested Data Pipelines

A technical book on ECL (Extract, Conform, Load) patterns for batch data pipelines. The manuscript is author-written and author-reviewed; what I need help with is **design: diagrams and the cover.**

## Where things are

| What | Where |
|---|---|
| Full manuscript (single source of truth) | `typst/book.typ` |
| Compiled book | `typst/book.pdf` (283 pp, A4) |
| Diagrams (SVG, 63 files) | `typst/diagrams/` |
| Diagram index + style spec | `typst/diagrams/INDEX.md` |
| Cover (DRAFT placeholder) | `typst/diagrams/cover-art.svg` |
| Layout / theme / palette | `typst/theme.typ`, `typst/palette.typ` |
| Book outline | `INDEX.md` (root) |

The `00-`..`08-` Markdown folders are a **legacy** drafting vault, not the source of truth. Ignore them; everything live is in `typst/`.

## Status

- **Text**: written and reviewed by the author. Not seeking a content rewrite -- copyedit and proofing welcome, but the voice and stances are deliberate (see `CLAUDE.md` "Voice & Tone").
- **Diagrams**: all 63 exist and render, in a consistent gruvbox-light style. Open to a designer's polish and to redrawing any that read weakly. The style spec to match is at the top of `typst/diagrams/INDEX.md`.
- **Cover**: a deliberate DRAFT placeholder. Design from scratch -- title is "Battle-Tested Data Pipelines", subtitle "The step ELT forgot".

## Build

```bash
typst compile typst/book.typ typst/book.pdf   # compile
typst watch   typst/book.typ typst/book.pdf   # live preview while editing
```

Diagrams are hand-authored SVGs embedded with `#figure(image("diagrams/XXYY-name.svg", width: 95%))`. To preview one: `rsvg-convert -w 1080 typst/diagrams/NAME.svg -o /tmp/x.png`. Validate before committing: `xmllint --noout typst/diagrams/NAME.svg`.

## What I'm asking for

1. **Cover design** -- the current file is a wireframe placeholder.
2. **Diagram review/polish** -- consistency, legibility, and redraws where a figure underperforms. `INDEX.md` lists every figure, where it appears, and the style rules.

Conventions for any new or revised diagram are documented in `typst/diagrams/INDEX.md` (canvas, palette, fonts, title) and in full in `CLAUDE.md` under "SVG Diagram Conventions".
