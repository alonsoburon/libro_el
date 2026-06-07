# Diagram Review Report -- 2026-06-07
Full visual + style review of all 70 SVGs in `typst/diagrams/`. Method: mechanical lint (XML, palette, fonts, viewBox, em-dash) + 70 visual review agents over 1440px renders + adversarial verification of every medium/high finding (14 of 35 refuted as false positives) + cross-file consistency analysis.

Stats: 70 reviewed | 21 visual issues confirmed | 14 refuted | 28 low-severity nitpicks (unverified)

## Confirmed visual defects (fix queue)

### HIGH
- [ ] **0306-hard-delete.svg** [legibility] -- Data row text (101, Alice, active, Bob, Carol, closed) uses fill #3c3836 (near-black) against a dark navy #1e1e34 background. The contrast ratio is very low, making the cell values appear dim and hard to read in the render.
  - where: Both tables, all data rows (y approx 91, 121, 151)
- [ ] **0307-open-closed.svg** [clipping] -- The 'RECENTLY CLOSED' box is too narrow for its subtitle text '(cursor catches transition)' -- the text is clipped at the right edge of the box. In the render the subtitle reads '(cursor catches transiti' and is cut off. The box width (~110px) is far too short for the 12px bold text it contains.
  - where: Middle segment box, 'RECENTLY CLOSED', approximately x=207-317 in the SVG
- [ ] **0307-open-closed.svg** [clipping] -- The 'OPEN DOCUMENTS' box title line overlaps and visually collides with the 'RECENTLY CLOSED' box. Because both boxes are adjacent and the 'RECENTLY CLOSED' box starts immediately after (x=207), the subtitle '(mutable, re-extract all)' text appears to run into the neighbor box in the render.
  - where: Border between OPEN DOCUMENTS and RECENTLY CLOSED boxes, approximately x=205-215
- [ ] **0404-compaction.svg** [clipping] -- The italic footer note 'Run periodically. Frequency = storage cost vs history needs.' starts at x=500 with no text-anchor (defaults to left-align) and extends past the 720px canvas boundary. In the render the text is cut off after 'histor', losing the words 'y needs.'
  - where: bottom-right, y=201, x=500 to beyond right edge
- [ ] **0404-log-anatomy.svg** [clipping] -- The italic legend text 'Compaction collapses log to view state periodically.' starts at x=555 (left-anchored) with font-size 9. At that size the string extends roughly to x=840, well past the 720-wide canvas. In the render the text is cut off after 'state' -- 'periodically.' is not visible.
  - where: bottom-right legend, y=239, x=555 to canvas right edge
- [ ] **0405-hybrid-append-merge.svg** [overlap] -- The bold 'run 3: 1001 shipped, 1002 confirmed, 1003 pending' text (x=350, y=82, font-size 7.5) extends approximately 40px past x=520 where the right-column consumer label 'low frequency, high volume' begins at the same y=82. The two strings visually collide -- 'pending' renders on top of 'frequency, high volume' making both unreadable.
  - where: BigQuery: orders_log box, third data row (y~82), right half of the box
- [ ] **0502-synthetic-key.svg** [clipping] -- The italic 'corrected' annotation is placed at x=710 in a 720-wide viewBox. It is clipped by the canvas right edge -- only 'cor' is visible in the render.
  - where: right edge, row 2 of the table, y~98 in SVG coordinates
- [ ] **0604-sla-duration-creep.svg** [clipping] -- The italic legend caption 'The signal was in the health table for weeks.' starting at x=575 extends beyond the 720px canvas boundary and is visibly cut off in the render. The text reads '...health table fo' before the canvas edge clips it.
  - where: bottom-right legend area, y=185, x=575 to past x=720
- [ ] **0611-backfill-types.svg** [overlap] -- The '2025-Feb' label overflows its 20px-wide bounding box on both sides. The text is rendered at font-size 7 but the box (x=540, width=20) is far too narrow to contain '2025-Feb', so the label visibly extends outside the yellow stroke border in both directions.
  - where: Row 2 (Targeted partitions), rightmost partition box at approximately x=540-560 in SVG coordinates
- [ ] **domain-model-er-mermaid.svg** [clipping] -- The 'sessions' table is cut off at the right canvas edge. The table header shows 'sessions' but the right side of the table body (columns and their values) is clipped -- 'started_' is visible but truncated, and the full column content is missing.
  - where: top-right corner, sessions table
- [ ] **domain-model-er-mermaid.svg** [clipping] -- The 'products' table is clipped at the right canvas edge. The header shows only 'produc' and the column text 'nam' is truncated mid-word. The entire right portion of this table is cut off.
  - where: right side, middle area, products table
- [ ] **ecl-corridors.svg** [clipping] -- Both column boxes are declared height=280 starting at y=70, placing their bottoms at y=350, but the viewBox height is only 310. The bottom 40px of each box -- including the rounded bottom corners (rx=6) -- is clipped by the canvas edge. In the render, both panels end with a hard straight edge at the canvas bottom rather than a rounded corner.
  - where: bottom edge of both left and right panels, y~310 (canvas boundary)

### MEDIUM
- [ ] **0205-scoped-replace.svg** [legibility] -- The MANAGED ZONE box uses fill #6c8ebf (flagged as off-palette), which renders as a solid medium-blue. The dark text (#3c3836) on this blue fill has adequate contrast, but the blue color is visually heavier than the tan FROZEN ZONE box, making the two zones feel unbalanced -- the managed zone visually dominates. This is a rendering-visible consequence of the off-palette color.
  - where: right half, MANAGED ZONE box
- [ ] **0209-partial-columns.svg** [overlap] -- In the source table, the 'email' column data values (centered at x=132) and 'national_id' column data values (centered at x=182) visibly run into each other. 'national_id' values like '12.345.678-9' are ~72px wide and their left edge reaches back to roughly x=146, while 'email' values like 'a@co.cl' extend rightward to ~x=157. In the render, email text is visibly cut short and bleeds directly into the national_id numbers across all three data rows.
  - where: Source table, email and national_id data columns, rows 1-3 (y~87, y~117, y~147 in SVG coords)
- [ ] **0303-stateless-window.svg** [alignment] -- The 'now (N+1)' dashed vertical line is placed at x=655, which falls between the Mar 2 tick (x=620) and Mar 3 tick (x=690) on the timeline axis -- roughly mid-day Mar 2. The Run N+1 row label reads '06:00 Mar 2', so 'now' should coincide with the Mar 2 tick (x=620). A reader following the timeline axis will see the dashed line sitting noticeably to the right of the Mar 2 label, suggesting the run fires around noon rather than at 06:00.
  - where: right third of diagram, 'now (N+1)' dashed line between Mar 2 and Mar 3 ticks
- [ ] **0403-merge-upsert.svg** [spacing] -- The staging (left) table box has the same height as the destination table but contains only 2 data rows, leaving roughly half the box as empty dark space. The destination table fills its 4-row height naturally, making the left table look unfinished and the two panels visually unbalanced.
  - where: left table (Staging), lower half, approximately y=135 to y=196
- [ ] **0507-nested-json.svg** [clipping] -- The 'downstream' arrow label (font-size 6.5, x=498) starts 7px before the right panel box edge (x=505). The text overruns the box border and is clipped in the render -- only 'do' is legible, the rest is hidden behind the box fill.
  - where: center-right, between destination table and VIEW panel, approximately y=72, x=498-540
- [ ] **0606-scheduling-dependencies.svg** [arrow] -- The dashed path from customers to executive_kpi (line 111 in SVG) has no arrowhead polygon. Every other dependency arrow in the diagram has an arrowhead; this one terminates at the executive_kpi box left edge (~x=535, y=164) with no marker, making the direction ambiguous.
  - where: center-right, dashed arc terminating at executive_kpi box left edge, y~164
- [ ] **0611-backfill-types.svg** [overlap] -- The 'state-reset point' label (y=151, orange) overlaps with the italic 'use when known windows were bad' annotation (y=147, gray) from the row above. Both lines of text occupy nearly the same vertical band, making them readable only with effort.
  - where: Center of diagram between Row 2 and Row 3, approximately y=147-151 in SVG coordinates around x=380-460
- [ ] **cover-art.svg** [spacing] -- Author name at y=620 leaves roughly 220px of blank space between it and the bottom accent line (y=837). The content cluster occupies the top ~60% of the canvas; the bottom ~40% is almost entirely empty, creating a pronounced void that a reader would notice on the cover.
  - where: lower third of the cover, below the three ECL blocks and above the bottom accent bar
- [ ] **domain-model-er-mermaid.svg** [spacing] -- The lower half of the canvas (roughly y > 400 in the 792-height viewBox) is entirely empty white space. The diagram content occupies only the top ~40% of the canvas, leaving a disproportionately large blank region.
  - where: bottom half of canvas

## Low-severity nitpicks (unverified, fix opportunistically)
- 0104-lies-sources-tell.svg [spacing]: Only 3px of bottom padding between the last row rect (y=235, height=22, so bottom edge y=257) and the canvas bottom (y=260). Not clipping in the render but leaves almost no breathing room.
- 0105-hard-vs-soft-rules.svg [spacing]: The divider label 'what the database enforces / what humans say' sits at y=91 with its baseline only 3px above the dashed dividing line at y=94, making the label and line appear cramped; the label reads as sitting on top of the line rather than above it.
- 0105-hard-vs-soft-rules.svg [spacing]: The '...and many more' italic tag at y=198 and the italic sub-label 'until one disputed line breaks ranks' at y=203 in the right column are only 5px apart (baseline to baseline), which is tighter than every other label/sub-label pair (baseline gap of 10px) and makes those two lines visually crowd each other.
- 0108-purity-freshness.svg [arrow]: Connector lines have no arrowheads. The horizontal lines from diamond-shaped question nodes to terminal boxes (and the left-branching lines to 'Scoped replace + periodic full' and 'Append + materialize') carry no arrowhead markers, so direction is implied only by layout. For a decision tree this is a minor readability gap.
- 0108-purity-freshness.svg [spacing]: The 'writes >> reads' and 'reads >> writes' edge labels (y=346) sit very close to the horizontal connector lines (y=352), leaving only 6px of vertical clearance. They are legible but cramped.
- 0109-stateless-idempotent.svg [spacing]: The rotated 'STATEFUL' axis label sits at x=656 in a 680-wide canvas, leaving only ~12px to the right edge. In the render it reads correctly but is noticeably tight against the canvas margin.
- 0207-activity-driven.svg [spacing]: The ':last_run' label (x=225, y=105) and the 'DISTINCT' arrow label (x=256, y=112) are stacked in the narrow 48px gap between the left table and the middle table. They are legible and do not pixel-overlap, but the vertical gap between them is only ~7px, making the area feel cramped on close inspection.
- 0207-activity-driven.svg [spacing]: Bottom caption baseline sits at y=212 inside a 220px-tall canvas, leaving only 8px below for descenders. In the current render all glyphs are visible, but letters with descenders (g, p, y) reach very close to the canvas edge and could clip in renderers with tight bounding-box handling.
- 0208-hash-detection.svg [clipping]: The _hash column in the rightmost table (Destination after) has its header and values centered at x=775 inside a table that ends at x=810, leaving only 35px to the right column edge and 10px to the canvas edge at x=820. The text is not clipped but is noticeably close to the right canvas boundary.
- 0209-partial-columns.svg [overlap]: The left dashed red exclusion boundary line at x=157 passes directly through the right portion of the 'email' column header text (centered at x=132). The line visually bisects the word 'email' in the header, which is slightly confusing -- the dashed boundary appears to cut the email column rather than cleanly separate email from national_id.
- 0302-cursor-extraction.svg [spacing]: Bottom axis labels ("Day 1" through "Day 7") are placed at y=124 in a 138px-tall canvas, leaving only ~14px of margin below the text baseline. Descenders in some fonts would nearly touch the canvas edge.
- 0303-stateless-window.svg [spacing]: The Run N+1 box right edge is at x=658 while the 'now (N+1)' marker is at x=655 -- the now-line is 3px inside the box rather than flush with its right edge. This causes the box to extend 3px past 'now', implying the window extends slightly beyond the current time. Barely visible but technically inconsistent.
- 0304-cursor-from-header.svg [legibility]: Extraction arrow labels ('SELECT ol.*' and 'WHERE o.updated_at >=') are rendered at font-size 7.5px, noticeably smaller than all other body text (8-9px). At print scale these two lines will be the hardest text to read in the diagram.
- 0307-open-closed-split.svg [spacing]: The 're-extract all ↑' label (y=132) and '↓  cursor only' label (y=146) are both crammed into a narrow band around the open/closed divider line, alongside the highlighted INV-398 row background starting at y=138. The four elements (divider line, two zone labels, and the highlighted row) occupy only 14px of vertical space, making the area slightly dense, though all text remains legible.
- 0403-merge-mechanics.svg [spacing]: Destination row 6 (order_id 1006) has a background rect height of 14px while all other data rows use 18px, making the last row visibly shorter than the rest.
- 0403-merge-upsert.svg [alignment]: Legend swatch boxes sit at y=208 (10px tall, center ~y=213) but legend text baseline is at y=215, placing the text slightly below the vertical center of its color swatch.
- 0406-checkpoint-placement.svg [arrow]: The 'vulnerability window' italic label (centered at x=520, y=93, font-size 7.5) is visually bisected by the dashed vertical line for checkpoint C at x=548, which drops through the label's right half.
- 0406-checkpoint-placement.svg [legibility]: The 'vulnerability window' label uses font-size 7.5 italic, which is the smallest text in the diagram. At true SVG dimensions (720px canvas) this renders at roughly 7-8px -- marginal legibility, especially in print.
- 0503-type-casting.svg [legibility]: Italic caption at font-size 7.5 ('x 1M rows = aggregate diverges from source. Accounting will find it.') is the smallest text in the diagram. At Typst embed width (~95% of page) this line will be noticeably small and may strain readability, especially in print.
- 0504-null-handling.svg [spacing]: The 'COALESCE(email, \'\')' arrow label (y=158) and the 'NULL and \'\'  merged' sub-label (y=176) are placed very close together vertically relative to the narrow gap between the two destination boxes (green box bottom y=98, red box top y=132). The labels are legible but the vertical corridor between the two destination boxes is congested.
- 0601-four-layers-observability.svg [legibility]: Legend color swatches are only 3px wide -- they render as very thin slivers that are barely distinguishable from each other at normal reading size.
- 0605-alerting-severity.svg [alignment]: The Y-axis row label 'Multiple tables' sits at y=118, but the visual midpoint of that row's cells (y=104 to y=154) is y=129. It reads slightly high relative to its row, compared to how 'Pipeline-wide' (y=68) and 'Single table' (y=168) align to their rows.
- 0606-scheduling-dependencies.svg [clipping]: Legend text sits at y=237 (font-size 8) in a 240-high canvas. Descenders on letters such as 'y' in 'dependency' and 'p' in 'path' land at approximately y=239, within 1px of the canvas bottom. The render shows the text is just clear, but it is extremely tight and any slight rendering difference could clip descenders.
- 0610-extraction-gates.svg [spacing]: The extraction_failure terminal box bottom edge is at y=230 in a 240-height canvas, leaving only 10px of bottom margin. In the render the box sits very close to the canvas boundary, giving a cramped feel at the bottom of the diagram.
- 0613-duplicate-sources.svg [spacing]: All four arrow arrowheads converge within a 12px vertical band (y=107 to y=119) on the left edge of the destination table. They are individually distinguishable but visually merge into one compound shape, making it hard to trace which color belongs to which arrowhead.
- 0703-view-hierarchy.svg [spacing]: The two-line QUALIFY SQL block in the orders (blue) row sits at y=134 and y=144 inside a 34px-tall box (y=120 to y=154). The second line baseline is only 10px from the bottom border, giving noticeably less breathing room than all other rows. The text is legible but cramped relative to the surrounding rows.
- 0704-analyst-queries.svg [spacing]: The italic 'or simply: SELECT * FROM orders WHERE...' line (y=124) sits only 6px above the top border of the result table rect below it (y=130). At the rendered size it just barely clears, but the gap is tight enough to read as nearly touching.
- domain-model-er.svg [spacing]: The 1:N label text (y=54) sits directly on the connector line (y1=60 on the line), with only 6px vertical separation. The label overlaps the line stroke slightly and could be mistaken for a field row in the adjacent boxes.

## Refuted findings (checked, not real -- do not fix)
- 0105-hard-vs-soft-rules.svg [clipping]: The italic sub-labels on the third row of soft rules ('until a return shows -1' and 'until one disputed line breaks ranks') have their text ... -> The zoomed render shows both italic sub-labels sitting visibly inside the orange box with clear space above the bottom border stroke -- no clipping, poking, or flush-against-the-edge defect is observable despite the 1px baseline overshoot in the SVG coordinates.
- 0108-purity-freshness.svg [spacing]: The legend row at the bottom has two separate swatch groups (Low complexity vs Medium/High) but the Medium label covers only one of two swat... -> In the render, the two medium swatches (olive + tan) sit adjacently before the "Medium" label and the two high swatches (orange + red) sit adjacently before "High," forming readable pairs where the label trails the group -- the unlabeled first swatch in each pair is visually grouped with its labeled partner, and while the gap before the olive swatch is noticeable, no reader is likely to misattribute swatches to the wrong tier.
- 0205-scoped-replace.svg [spacing]: The MANAGED ZONE bracket (line x1=349.7, x2=760) ends at x=760, but the MANAGED ZONE box extends to x=760 (349.7+410.3). The right side of t... -> The label at y=52 and bracket at y=60 produce an 8px gap identical to the lower label (y=80) above its bracket (y=88), and the rendered PNG shows both labels sitting at the same consistent distance above their respective bracket lines -- no uneven spacing is visible.
- 0208-hash-detection.svg [spacing]: The 'compare / by id' arrow label (centered at x=270 in SVG coords) and the arrow itself (arrowhead tip at x=286) leave only 4px of clearanc... -> In the render, the "compare / by id" label and arrowhead sit clearly within the inter-table gap -- the text does not visually touch the Destination (before) table's left border, and the spacing reads as adequate white space at book page scale.
- 0208-hash-detection.svg [spacing]: The 'load / diff' arrow label (centered at x=550) and arrowhead (tip at x=566) sit 4px from the Destination (after) table border at x=570. S... -> The arrowhead tip at x=566 sits 4px from the Destination (after) border at x=570, matching the first arrow's identical 4px clearance, but in the rendered PNG both arrows are fully legible with no clipping or overlap -- the spacing is tight but not a functional defect a book reader would notice.
- 0301-cursor-blind-spots.svg [clipping]: The three right-side annotation boxes ('no trigger on INS', 'bypassed trigger', 'set to past date') start at x=610 and extend to x=710 in a ... -> The annotation boxes end at x=710 (10px from the canvas edge), but in the render all three labels are fully visible, cleanly bounded, and not clipped or uncomfortably crowded -- a reader of the book would see nothing defective here.
- 0307-open-closed.svg [overlap]: The bracket label 'SELECT * WHERE status = \'open\'' sits at y=80 with tick marks at y=85-91 and the box top at y=96. The gap is tight (abou... -> In the rendered PNG, the label text and bracket ticks are visibly separated with a clear gap -- the text sits above the bracket line with enough white space that a reader would not perceive any touching or overlap; the spacing is compact but entirely readable.
- 0406-checkpoint-placement.svg [clipping]: The bold heading 'After load, before confirm: reprocessing' (font-size 9, starting at x=265) is too wide for its container box (width 218, e... -> In the rendered PNG the heading "After load, before confirm: reprocessing" fits entirely within the gold panel (x=251 to x=469) with visible margin before the green panel; no clipping or overrun is observable.
- 0503-type-gap.svg [arrow]: The DATE/DATETIME2(7)/TIME row arrow (horizontal at y=183) and the TIMESTAMP WITH TIME ZONE row arrow (diagonal from y=205 left to y=186 rig... -> The two lines do not cross: the DATE/DATETIME2(7)/TIME arrow is horizontal at y=183 while the TIMESTAMP WITH TIME ZONE arrow runs diagonally from y=205 down to y=186, so at every x-coordinate between 204 and 516 the diagonal line is strictly below the horizontal one -- they form a clean fan-in to the TIMESTAMP box with no X-crossing in the render.
- 0504-null-handling.svg [clipping]: The italic text 'Irreversible. No consumer can recover the distinction.' (font-size 7, y=200) sits only 4px above the bottom edge of its con... -> In the render the italic text at y=200 is visibly tight near the bottom of the red box (bottom at y=204) but remains fully legible with no clipping -- all letterforms are intact and a small gap is visible between the descenders and the box stroke, so a book reader would not see a defect.
- 0701-detail-fanout.svg [clipping]: The text 'GROUP BY warehouse, week' in the v_shipments _by_location box (font-size 10, centered at x=442.5) is the widest body line in any o... -> In the rendered PNG, "GROUP BY warehouse, week" is visibly contained within the box with a small but clear gap before the right border -- the text does not touch or overflow the edge in the actual render.
- domain-model-er.svg [clipping]: The invoice_lines box (x=690, width=180) extends to x=870, but the canvas is 900px wide, leaving only 30px of right margin. At the rendered ... -> In the rendered PNG, the invoice_lines box and all its text ("Independent hard deletes", "invoice_id FK", "status") are fully visible with clear white space to the right of the box edge -- no clipping or crowding is apparent to a book reader.
- domain-model-er.svg [clipping]: The inventory_movements box (x=465, width=280) extends to x=745, but the canvas is 900px wide so there is adequate right space. However the ... -> The rendered PNG shows the products subtitle "Schema mutates (new columns appear)" fully contained within its box with no visible overflow -- the text sits cleanly inside the right edge of the box at font-size 9 with a 10px left padding from x=480.
- domain-model-er.svg [arrow]: The dashed FK path from products (row 2, x=520-570 range) down to inventory and inventory_movements passes through the empty vertical corrid... -> The two paths share y=248 but do not cross: path 1 runs horizontally from x=540 leftward to x=295 and path 2 runs from x=570 rightward to x=605, so they occupy non-overlapping x-ranges with a gap between x=540 and x=570 -- no diagonal crossing exists in the render.

## Mechanical lint results

```
0104-lies-sources-tell.svg
  - non-standard fonts: ['monospace']
0108-purity-freshness.svg
  - viewBox not 0 0 720: 0 0 760 440
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#282828', '#427b58', '#689d6a', '#98971a', '#af3a03', '#b57614', '#bdae93', '#cc241d', '#f9f5d7']
0109-stateless-idempotent.svg
  - em-dash found (2x)
  - viewBox not 0 0 720: 0 0 680 380
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#282828', '#427b58', '#689d6a', '#af3a03', '#b57614', '#bdae93', '#cc241d']
0202-partition-swap.svg
  - off-palette colors: ['#f9f5d7']
0203-staging-swap.svg
  - off-palette colors: ['#f9f5d7']
0205-scoped-replace.svg
  - viewBox not 0 0 720: 0 0 800 192
  - off-palette colors: ['#6c8ebf']
0206-rolling-window.svg
  - em-dash found (1x)
  - viewBox not 0 0 720: 0 0 800 192
  - off-palette colors: ['#b8a44e']
0207-activity-driven.svg
  - off-palette colors: ['#5aab7b', '#bdae93', '#f9f5d7']
0207-sparse-table.svg
  - em-dash found (1x)
  - viewBox not 0 0 720: 0 0 800 164
  - off-palette colors: ['#5aab7b']
0208-hash-detection.svg
  - em-dash found (1x)
  - viewBox not 0 0 720: 0 0 820 276
  - off-palette colors: ['#b57614', '#f9f5d7']
0209-partial-columns.svg
  - em-dash found (3x)
  - off-palette colors: ['#f9f5d7']
0301-cursor-blind-spots.svg
  - em-dash found (5x)
  - off-palette colors: ['#f9f5d7']
0302-cursor-extraction.svg
  - em-dash found (1x)
  - viewBox not 0 0 720: 0 0 800 138
  - off-palette colors: ['#5aab7b', '#6c8ebf', '#8878b8', '#d4a040']
0302-cursor-mechanics.svg
  - em-dash found (2x)
  - off-palette colors: ['#b57614']
0303-stateless-window.svg
  - em-dash found (3x)
  - off-palette colors: ['#b57614']
0304-cursor-from-header.svg
  - em-dash found (6x)
  - off-palette colors: ['#f9f5d7']
0305-sequential-id-cursor.svg
  - em-dash found (1x)
0306-full-id-comparison.svg
  - em-dash found (1x)
  - off-palette colors: ['#f9f5d7']
0306-hard-delete.svg
  - em-dash found (4x)
  - viewBox not 0 0 720: 0 0 650 216
  - off-palette colors: ['#1e1e34', '#252540', '#d46a6a']
0307-open-closed-split.svg
  - em-dash found (4x)
  - off-palette colors: ['#f9f5d7']
0307-open-closed.svg
  - viewBox not 0 0 720: 0 0 800 192
  - off-palette colors: ['#b8a44e', '#d4a040']
0308-detail-without-timestamp.svg
  - off-palette colors: ['#bdae93', '#f9f5d7']
0309-late-arriving.svg
  - em-dash found (3x)
  - viewBox not 0 0 720: 0 0 800 168
  - off-palette colors: ['#5aab7b', '#6c8ebf', '#d46a6a', '#d4a040']
0310-create-vs-update.svg
  - non-standard fonts: ['monospace']
  - off-palette colors: ['#f9f5d7']
0401-full-replace-load.svg
  - off-palette colors: ['#f9f5d7']
0402-append-only-load.svg
  - off-palette colors: ['#f9f5d7']
0403-merge-cost.svg
  - off-palette colors: ['#f9f5d7']
0403-merge-mechanics.svg
  - off-palette colors: ['#f9f5d7']
0403-merge-upsert.svg
  - em-dash found (1x)
  - viewBox not 0 0 720: 0 0 650 246
  - off-palette colors: ['#1e1e34', '#252540', '#5aab7b', '#b8a44e']
0404-compaction.svg
  - off-palette colors: ['#f9f5d7']
0404-log-anatomy.svg
  - off-palette colors: ['#f9f5d7']
0405-hybrid-append-merge.svg
  - off-palette colors: ['#f9f5d7']
0406-checkpoint-placement.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0406-partial-load-recovery.svg
  - viewBox not 0 0 720: 0 0 760 220
  - off-palette colors: ['#689d6a', '#cc241d']
0501-metadata-injection.svg
  - viewBox not 0 0 720: 0 0 820 210
  - off-palette colors: ['#f9f5d7']
0502-synthetic-key.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0503-type-casting.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0503-type-gap.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0504-null-handling.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0505-timezone-conforming.svg
  - off-palette colors: ['#689d6a', '#cc241d', '#f9f5d7']
0506-charset-encoding.svg
  - off-palette colors: ['#f9f5d7']
0507-nested-json.svg
  - off-palette colors: ['#f9f5d7']
0601-four-layers-observability.svg
  - off-palette colors: ['#b57614']
0602-health-table.svg
  - off-palette colors: ['#b57614', '#f9f5d7']
0604-sla-duration-creep.svg
  - off-palette colors: ['#b57614']
0605-alerting-severity.svg
  - off-palette colors: ['#b57614']
0606-scheduling-dependencies.svg
  - off-palette colors: ['#cc241d', '#f9f5d7']
0609-schema-evolution-policies.svg
  - off-palette colors: ['#689d6a', '#b57614']
0610-extraction-gates.svg
  - non-standard fonts: ['monospace']
  - off-palette colors: ['#f9f5d7']
0612-partial-failure-recovery.svg
  - off-palette colors: ['#689d6a', '#f9f5d7']
0613-duplicate-sources.svg
  - off-palette colors: ['#076678', '#af3a03', '#b57614', '#cc241d', '#f9f5d7']
0615-corruption-recovery.svg
  - off-palette colors: ['#689d6a', '#f9f5d7']
0701-detail-fanout.svg
  - em-dash found (1x)
  - off-palette colors: ['#181825', '#1e1e2e', '#585b70', '#58b0a0', '#5aab7b', '#6c7086', '#6c8ebf', '#a6adc8', '#d46a6a', '#d4a040']
0702-partition-pruning.svg
  - off-palette colors: ['#f9f5d7']
0703-view-hierarchy.svg
  - off-palette colors: ['#b57614']
0704-analyst-queries.svg
  - off-palette colors: ['#f9f5d7']
0706-point-in-time.svg
  - off-palette colors: ['#79740f']
cover-art.svg
  - viewBox not 0 0 720: 0 0 595 842
  - off-palette colors: ['#076678', '#282828', '#504945']
domain-model-er-mermaid.svg
  - viewBox not 0 0 720: 0 0 612 792
  - off-palette colors: ['#060606', '#282828', '#2c2d2d', '#504945', '#665c54', '#787878']
domain-model-er.svg
  - viewBox not 0 0 720: 0 0 900 500
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#689d6a', '#b16286', '#bdae93', '#f9f5d7']
ecl-conforming-vs-transforming.svg
  - viewBox not 0 0 720: 0 0 680 360
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#282828', '#427b58', '#689d6a', '#af3a03', '#f9f5d7']
ecl-corridors.svg
  - viewBox not 0 0 720: 0 0 680 310
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#282828', '#689d6a', '#b16286', '#cc241d', '#f9f5d7']
ecl-load-strategy-spectrum.svg
  - viewBox not 0 0 720: 0 0 820 360
  - non-standard fonts: ['system-ui,sans-serif']
  - off-palette colors: ['#282828', '#689d6a', '#98971a', '#cc241d', '#f9f5d7']
schema-drift-detection.svg
  - viewBox not 0 0 720: 0 0 612 792
  - off-palette colors: ['#333333', '#504945', '#689d6a', '#b57614', '#f9f5d7']
```

---

# SVG Diagram Style Consistency Analysis

70 diagrams reviewed against documented conventions (viewBox `0 0 720 H`, gruvbox-light palette, Segoe UI/system-ui font, centered title y=18 size=13 fill=#7c6f64, no em-dashes).

## 1. De-facto palette extensions (adopt, don't fix)

These colors are gruvbox-light variants used consistently across many files for a stable structural role. They are not design errors -- they are the working palette as actually practiced and should be added to the documented palette.

| Color | Files | Role | Recommendation |
|---|---|---|---|
| `#f9f5d7` | **41** | gruvbox light0_hard -- the standard fill for emphasis/staging/neutral boxes (e.g. `fill="#f9f5d7" stroke="#d5c4a1"` in 0506, 0503). The single most common "off-palette" color in the book. | **Add as `bg-soft #f9f5d7`.** Fixing 41 files is absurd; this is the de-facto box fill. |
| `#689d6a` | **17** | gruvbox green (bright/aqua) -- "correct/pass" accents in the 0502-0505 conforming set and recovery terminals (0406, 0612, 0615). Used alongside documented green `#79740e` as a brighter pass color. | **Add as `green-bright #689d6a`** (pairs with `red-bright` below). |
| `#b57614` | **13** | gruvbox yellow/orange (faded) -- "buffer/build-first/source-health" amber labels (0601, 0602, 0604, 0605, 0703). Sits between documented orange `#d65d0e` and yellow `#d79921`. | **Add as `amber #b57614`.** Consistent role across the Part VI operational set. |
| `#cc241d` | **13** | gruvbox red (neutral) -- "wrong/fail" accent in the same conforming set (0502-0505) and 0406/0606. Brighter sibling of documented red `#9d0006`. | **Add as `red-bright #cc241d`.** Pairs with `#689d6a` as the correct/wrong duo across 0502-0505. |
| `#bdae93` | **5** | gruvbox gray/light4 -- dimmed/muted table rows and grid lines (20 uses in 0207-activity, 8 in 0308). | **Add as `dim-warm #bdae93`.** Legitimate neutral. |
| `#af3a03` | 4 | gruvbox dark-orange. Appears in the early-design cluster (see §2) plus a couple others. Borderline -- lower priority. | Consider adding as `orange-dark`, or fold into `#d65d0e` during rework. |

**Net:** documenting `#f9f5d7`, `#689d6a`, `#b57614`, `#cc241d`, `#bdae93` (and optionally `#af3a03`) clears the lint noise on ~50+ files without touching a single SVG. These are gruvbox-family colors; they belong in the palette.

## 2. True outliers (need rework -- foreign design systems)

Two distinct foreign design sessions, plus expected exemptions.

### Cluster A -- Catppuccin darks (3 files, two sub-sessions)
These use a dark catppuccin/near-black palette that has nothing to do with gruvbox-light. Clearly produced in separate design sessions from the rest of the book.

- **`0701-detail-fanout.svg`** -- full Catppuccin Mocha: `#1e1e2e`, `#181825`, `#a6adc8`, `#585b70`, `#6c7086`, `#58b0a0`, plus foreign blue `#6c8ebf`, green `#5aab7b`, gold `#d4a040`, red `#d46a6a`. Title fill is `#a6adc8` at y=24 (wrong color, wrong y). This is the worst offender -- a completely different theme. **Full redraw required.** (Note: an untracked `_charts/diagrams/0701-detail-fanout.html` exists in git status -- likely the source of this rogue style.)
- **`0306-hard-delete.svg`** and **`0403-merge-upsert.svg`** -- paired sub-session: both use `#1e1e34` + `#252540` (dark navy fills) with foreign accents (`#d46a6a` red, `#b8a44e`/`#5aab7b`). Both also have non-standard viewBox (650x216, 650x246), em-dashes, and y=26 titles. Same hand, same session. **Both need redraw** to the gruvbox-light system. Note each has a clean sibling that already conforms (`0306-full-id-comparison`, `0403-merge-mechanics`/`merge-cost`) -- use those as the template.

### Cluster B -- Foreign blues/purples (Mermaid/draw.io leakage, scattered)
`#6c8ebf` (draw.io blue, 4 files: 0205, 0302-cursor-extraction, 0309-late-arriving, 0701) and `#8878b8`/`#d4a040`/`#5aab7b` (0302-cursor-extraction) and `#b8a44e`/`#d4a040` (0206, 0307-open-closed). These look like draw.io/Mermaid default-theme leakage. **Recolor to palette** (mechanical per-color swap, not full redraw) -- the layouts are fine, only the accent colors are foreign.

### Cluster C -- Early-design session: oversized titles + system-ui font (5 files)
`0108-purity-freshness`, `0109-stateless-idempotent`, `ecl-conforming-vs-transforming`, `ecl-corridors`, `ecl-load-strategy-spectrum`. Shared fingerprint: `font="system-ui,sans-serif"` (not the convention's `'Segoe UI', system-ui`), title at **font-size 16, fill #282828** (near-black) with a separate subtitle line, large non-720 viewBoxes (760x440, 680x380, 680x360, 680x310, 820x360), and the gruvbox `#282828`/`#427b58`/`#98971a`/`#b16286` darker variants. These were clearly made together before the title convention was fixed. **Per-file rework:** retitle to convention (centered y=18, size 13, fill #7c6f64), swap font string, recolor `#282828` titles. Layouts are sound; this is medium-effort cleanup, not redraw.

### Exempt (do not touch)
- **`cover-art.svg`** -- explicitly style-exempt (Libertinus Serif, A4 portrait).
- **`domain-model-er-mermaid.svg`** -- generated Mermaid export, dark theme, 612x792. Style-exempt as a generated artifact (consider deleting in favor of the hand-built `domain-model-er.svg`).
- **`domain-model-er.svg`** and **`schema-drift-detection.svg`** -- schema/flowchart diagrams with no title and portrait/large viewBox. ER diagram is intentionally title-less; schema-drift is a 612x792 portrait flowchart (draw.io-style `use` glyphs). These are structurally different from the inline figure set; decide whether to bring them into the 720-wide convention or formally exempt them.

## 3. Title convention adherence

**Adheres** (centered, y=18, size 13, fill #7c6f64): the large majority -- all of 0104, 0105, 0201, 0202, 0203, 0207-activity, 0307-open-closed-split, 0308, 0309-late-arriving-data, 0401-0405, 0404-*, 0501-0507, 0601, 0602, 0604, 0605, 0606, 0608, 0609, 0610, 0611, 0612, 0613, 0614, 0615, 0702, 0703, 0704, 0706.

**Minor y-offset only** (right color/size, y=20-26 instead of 18) -- cosmetic, fix in the em-dash pass:
- y=20: 0205, 0206, 0207-sparse, 0302-cursor-mechanics, 0303, 0307-open-closed
- y=22: 0208, 0209, 0301, 0302-cursor-extraction, 0304(y=18 ok), 0309-late-arriving, 0703
- y=26: 0306-hard-delete, 0403-merge-upsert (these are also Cluster A redraws)

**Wrong title style** (size 16 + fill #282828, or foreign) -- fix during rework:
- Cluster C: 0108, 0109, ecl-conforming-vs-transforming, ecl-corridors, ecl-load-strategy-spectrum
- Cluster A: 0701 (fill #a6adc8)

**No title element** (by design / generated): domain-model-er, domain-model-er-mermaid, schema-drift-detection, cover-art.

**Off-center x note:** several use x=400 (for 800-wide canvases) or x=410/380 -- these are correctly centered *for their viewBox*, so they're fine if the wider viewBox is accepted; otherwise they normalize when the canvas is brought to 720.

## 4. Recommended fix order

**Step 0 -- Documentation change (zero file edits, clears ~50 files of lint noise).**
Add to the CLAUDE.md palette: `#f9f5d7` (bg-soft), `#689d6a` (green-bright), `#cc241d` (red-bright), `#b57614` (amber), `#bdae93` (dim-warm). This is the single highest-leverage action -- it reclassifies the bulk of "off-palette" hits as intended.

**Step 1 -- Mechanical em-dash replace (17 files, find-replace `—` -> `--`).**
0109, 0206, 0207-sparse, 0208, 0209, 0301, 0302-cursor-extraction, 0302-cursor-mechanics, 0303, 0304, 0305, 0306-full-id-comparison, 0306-hard-delete, 0307-open-closed-split, 0309-late-arriving, 0403-merge-upsert, 0701. 40 occurrences total; pure text substitution. Bundle the minor title y-offset normalization (y=20/22 -> y=18) into the same pass for files already being opened.

**Step 2 -- Mechanical color swaps (foreign blues/purples, layouts intact).**
Cluster B: 0205, 0206, 0302-cursor-extraction, 0307-open-closed. Swap `#6c8ebf`->`#458588`, `#8878b8`/`#b16286`-> a palette accent, `#b8a44e`/`#d4a040`->`#d79921`, `#d46a6a`->`#9d0006`. Per-color sed, then re-render and eyeball.

**Step 3 -- Per-file rework, no redraw (Cluster C, 5 files).**
0108, 0109, ecl-conforming-vs-transforming, ecl-corridors, ecl-load-strategy-spectrum. Retitle to convention, fix font string `system-ui,sans-serif` -> `'Segoe UI', system-ui, sans-serif`, recolor `#282828` title text to `#7c6f64`, and (optionally) normalize viewBox toward 720-wide. Medium effort each; geometry is reusable.

**Step 4 -- Full redraws (3 files, foreign theme, no salvage).**
- `0701-detail-fanout.svg` -- worst case, full Catppuccin Mocha. Redraw from scratch in gruvbox-light using the multi-table-fanout idiom (template: 0613-duplicate-sources). Delete/ignore the stray `_charts/diagrams/0701-detail-fanout.html`.
- `0306-hard-delete.svg` -- redraw side-by-side compare; template `0306-full-id-comparison.svg`.
- `0403-merge-upsert.svg` -- redraw; template `0403-merge-mechanics.svg` (already conformant sibling).

**Step 5 -- Decide policy on the four exempt/structural files.**
cover-art (keep exempt), domain-model-er-mermaid (consider deleting -- redundant generated dark export), domain-model-er and schema-drift-detection (formally exempt as schema/portrait diagrams, or schedule a 720-wide redraw -- low priority, they render acceptably).

**Effort summary:** Step 0 is the big win (1 doc edit). Steps 1-2 are ~21 files of pure find-replace. Step 3 is 5 medium reworks. Step 4 is 3 genuine redraws. Only **3 files truly need full redraws**; everything else is documentation or mechanical substitution.

Relevant absolute paths for the redraw queue:
- `/home/alonso/code/libro_el/typst/diagrams/0701-detail-fanout.svg` (full redraw)
- `/home/alonso/code/libro_el/typst/diagrams/0306-hard-delete.svg` (full redraw)
- `/home/alonso/code/libro_el/typst/diagrams/0403-merge-upsert.svg` (full redraw)
- Templates to reuse: `/home/alonso/code/libro_el/typst/diagrams/0613-duplicate-sources.svg`, `/home/alonso/code/libro_el/typst/diagrams/0306-full-id-comparison.svg`, `/home/alonso/code/libro_el/typst/diagrams/0403-merge-mechanics.svg`
- Stray source to clean up: `/home/alonso/code/libro_el/_charts/diagrams/0701-detail-fanout.html`