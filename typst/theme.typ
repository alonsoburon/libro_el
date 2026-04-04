// ECL Patterns book theme
// Imports semantic palette -- change theme here to switch the entire book
#import "palette.typ": ecl-palette

#let p = ecl-palette(theme: "light")

// Re-export palette for chapter files that need accent colors directly
#let palette = p

// Semantic callout blocks -- colored left border, bold title + body
// Usage: #ecl-warning("Title here")[Body text with `code` and *markup*.]
#let ecl-callout(title, color: p.blue-accent, body) = block(
  fill: p.surface, inset: 12pt, radius: 4pt,
  stroke: (left: 3pt + color),
)[
  #text(fill: color, weight: "bold", size: 10pt, title)
  #v(4pt)
  #text(fill: p.fg-subtle, body)
]
#let ecl-tip(title, body) = ecl-callout(title, body, color: p.green-accent)
#let ecl-warning(title, body) = ecl-callout(title, body, color: p.yellow-accent)
#let ecl-danger(title, body) = ecl-callout(title, body, color: p.red-accent)
#let ecl-info(title, body) = ecl-callout(title, body, color: p.blue-accent)

// Part numbering state -- stepped by ecl-part-page, used by heading numbering
#let ecl-part = state("ecl-part", 0)

// Part title page -- the heading creates the PDF outline entry,
// and the show rule (level 1) renders it as a centered title page.
#let ecl-part-page(part-num, num, title) = {
  ecl-part.update(n => n + 1)
  counter(heading).update(part-num)  // set level-1 counter to the Part number
  pagebreak()
  heading(level: 1, numbering: none, outlined: true, bookmarked: true)[Part #num: #title]
}

// Apply book-wide styling
#let ecl-theme(body) = {
  set page(
    paper: "a4",
    margin: (x: 2cm, y: 2.5cm),
    fill: p.bg,
    // Running header: current pattern name (level 2 after offset)
    header: context {
      let elems = query(heading.where(level: 2).before(here()))
      if elems.len() > 0 {
        let current = elems.last()
        let here-page = counter(page).at(here()).first()
        let heading-page = counter(page).at(current.location()).first()
        if heading-page != here-page {
          text(fill: p.fg-dim, size: 9pt, current.body)
          v(-4pt)
          line(length: 100%, stroke: 0.5pt + p.border)
        }
      }
    },
    footer: context {
      align(center, text(fill: p.fg-dim, size: 9pt, counter(page).display()))
    },
  )
  set text(font: "Libertinus Serif", size: 11pt, fill: p.fg)

  // Heading numbering: Level 1 = Parts (no number shown).
  // Level 2+ = patterns/sections, numbered as Part.Pattern.Section (e.g. 1.2.3).
  // The heading counter's level-1 value IS the Part number (from the Part heading).
  // Front matter headings (level-1 counter = 0) get no numbers.
  set heading(numbering: (..nums) => {
    let n = nums.pos()
    // Level 1 (Parts): no visible number
    if n.len() <= 1 { return }
    // Level 2+: only if Part > 0 (skip front matter)
    if n.first() > 0 {
      numbering("1.1", ..n)
    }
  })

  // Level 1: Part headings -- rendered as centered title page
  show heading.where(level: 1): it => {
    set page(header: none, footer: none)
    v(1fr)
    align(center)[
      #text(fill: p.fg-dim, size: 14pt, tracking: 2pt, upper(it.body))
      #v(12pt)
    ]
    v(1fr)
  }

  // Level 2: pattern titles (= in source, offset to level 2) -- rule below
  show heading.where(level: 2): it => {
    set text(fill: p.fg-bright, size: 22pt)
    it
    v(2pt)
    line(length: 100%, stroke: 0.75pt + p.border)
    v(6pt)
  }
  show heading.where(level: 3): set text(fill: p.fg-bright, size: 16pt)
  show heading.where(level: 4): set text(fill: p.fg-subtle, size: 13pt)

  // Prevent orphaned headings -- always keep with the next block
  show heading: set block(below: 1em, above: 1.4em, sticky: true)

  // Cross-references: show "1.2 -- Name" instead of "Section 1.2"
  // Front matter headings (level-1 counter = 0) show just the title.
  show ref: it => {
    let el = it.element
    if el != none and el.func() == heading {
      link(el.location())[#context {
        let nums = counter(heading).at(el.location())
        if nums.len() >= 2 and nums.first() > 0 {
          [#numbering("1.1", ..nums) -- #el.body]
        } else {
          el.body
        }
      }]
    } else {
      it
    }
  }

  show raw.where(block: true): it => block(
    fill: p.code-bg, inset: 12pt, radius: 4pt, width: 100%,
    text(fill: p.code-fg, size: 9pt, it)
  )
  show raw.where(block: false): set text(fill: p.inline-code, size: 1em)
  show link: set text(fill: p.link)
  set table(
    stroke: 0.5pt + p.border,
    fill: (_, y) => if y == 0 { p.table-header-bg } else if calc.odd(y) { p.table-even-bg } else { p.table-odd-bg },
  )
  show table.cell.where(y: 0): set text(fill: p.fg-bright, weight: "bold", size: 10pt)
  show table.cell: set text(fill: p.fg-subtle, size: 10pt)
  show quote.where(block: true): it => block(
    inset: (left: 12pt, rest: 8pt),
    stroke: (left: 3pt + p.blue-accent),
    text(fill: p.fg-quote, it.body)
  )
  show strong: set text(fill: p.fg-bright)
  show emph: set text(fill: p.fg-quote)

  // Highlight the core concept -- subtle orange tint, not a shout
  let c-tint = color.mix((p.orange, 30%), (p.fg, 70%))
  show "ECL": [E#text(fill: c-tint)[C]L]
  show "Conforming": text(fill: c-tint, "Conforming")
  show "conforming": text(fill: c-tint, "conforming")

  body
}
