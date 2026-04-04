// Gruvbox Dark palette for ECL Patterns book
#let gruvbox = (
  bg0_h:  rgb("#1d2021"),  // darkest background
  bg0:    rgb("#282828"),  // card/surface
  bg1:    rgb("#3c3836"),  // borders, separators
  bg2:    rgb("#504945"),
  bg3:    rgb("#665c54"),
  bg4:    rgb("#7c6f64"),
  fg0:    rgb("#fbf1c7"),  // brightest text (titles)
  fg1:    rgb("#ebdbb2"),  // body text
  fg2:    rgb("#d5c4a1"),
  fg3:    rgb("#bdae93"),
  fg4:    rgb("#a89984"),  // dim text
  gray:   rgb("#928374"),  // muted
  red:    rgb("#fb4934"),
  green:  rgb("#b8bb26"),
  yellow: rgb("#fabd2f"),
  blue:   rgb("#83a598"),
  purple: rgb("#d3869b"),
  aqua:   rgb("#8ec07c"),
  orange: rgb("#fe8019"),
  // darker accent variants
  red_d:    rgb("#cc241d"),
  green_d:  rgb("#98971a"),
  yellow_d: rgb("#d79921"),
  blue_d:   rgb("#458588"),
  purple_d: rgb("#b16286"),
  aqua_d:   rgb("#689d6a"),
  orange_d: rgb("#d65d0e"),
)

// Semantic callout blocks -- colored left border, bold title + body
// Usage: #ecl-warning("Title here")[Body text with `code` and *markup*.]
#let ecl-callout(title, color: gruvbox.blue_d, body) = block(
  fill: gruvbox.bg0, inset: 12pt, radius: 4pt,
  stroke: (left: 3pt + color),
)[
  #text(fill: color, weight: "bold", size: 10pt, title)
  #v(4pt)
  #text(fill: gruvbox.fg2, body)
]
#let ecl-tip(title, body) = ecl-callout(title, body, color: gruvbox.green_d)
#let ecl-warning(title, body) = ecl-callout(title, body, color: gruvbox.yellow_d)
#let ecl-danger(title, body) = ecl-callout(title, body, color: gruvbox.red_d)
#let ecl-info(title, body) = ecl-callout(title, body, color: gruvbox.blue_d)

// Part numbering state -- stepped by ecl-part-page, used by heading numbering
#let ecl-part = state("ecl-part", 0)

// Part title page -- O'Reilly style, centered
#let ecl-part-page(num, title) = {
  ecl-part.update(n => n + 1)
  counter(heading).update(0)
  pagebreak()
  page(header: none, footer: none)[
    #v(1fr)
    #align(center)[
      #text(fill: gruvbox.fg4, size: 14pt, tracking: 2pt, upper[Part #num])
      #v(12pt)
      #text(fill: gruvbox.fg0, size: 28pt, weight: "bold", title)
    ]
    #v(1fr)
  ]
}

// Apply book-wide styling
#let ecl-theme(body) = {
  set page(
    paper: "a4",
    margin: (x: 2cm, y: 2.5cm),
    fill: gruvbox.bg0_h,
    // Running header: current pattern name, suppressed on the heading's own page
    header: context {
      let elems = query(heading.where(level: 1).before(here()))
      if elems.len() > 0 {
        let current = elems.last()
        let here-page = counter(page).at(here()).first()
        let heading-page = counter(page).at(current.location()).first()
        if heading-page != here-page {
          text(fill: gruvbox.fg4, size: 9pt, current.body)
          v(-4pt)
          line(length: 100%, stroke: 0.5pt + gruvbox.bg2)
        }
      }
    },
    footer: context {
      align(center, text(fill: gruvbox.fg4, size: 9pt, counter(page).display()))
    },
  )
  set text(font: "Libertinus Serif", size: 11pt, fill: gruvbox.fg1)

  // Heading numbering: Part.Pattern.Section (e.g. 2.3.1)
  // Front matter (part = 0) gets no numbers.
  set heading(numbering: (..nums) => context {
    let part = ecl-part.get()
    if part > 0 {
      numbering("1.1", part, ..nums.pos())
    }
  })

  // Level 1: pattern titles -- number + rule below
  show heading.where(level: 1): it => {
    set text(fill: gruvbox.fg0, size: 22pt)
    it
    v(2pt)
    line(length: 100%, stroke: 0.75pt + gruvbox.bg3)
    v(6pt)
  }
  show heading.where(level: 2): set text(fill: gruvbox.fg0, size: 16pt)
  show heading.where(level: 3): set text(fill: gruvbox.fg2, size: 13pt)

  // Cross-references: show "1.2 — Name" instead of "Section 1.2"
  show ref: it => {
    let el = it.element
    if el != none and el.func() == heading {
      link(el.location())[#context {
        let nums = counter(heading).at(el.location())
        let part = ecl-part.at(el.location())
        if part > 0 {
          numbering("1.1", part, ..nums)
        }
      } -- #el.body]
    } else {
      it
    }
  }

  show raw.where(block: true): it => block(
    fill: gruvbox.bg0, inset: 12pt, radius: 4pt, width: 100%,
    text(fill: gruvbox.fg1, size: 9pt, it)
  )
  show raw.where(block: false): set text(fill: gruvbox.fg3, size: 1em)
  show link: set text(fill: gruvbox.blue)
  set table(
    stroke: 0.5pt + gruvbox.bg2,
    fill: (_, y) => if y == 0 { gruvbox.bg1 } else if calc.odd(y) { gruvbox.bg0 } else { gruvbox.bg0_h },
  )
  show table.cell.where(y: 0): set text(fill: gruvbox.fg0, weight: "bold", size: 10pt)
  show table.cell: set text(fill: gruvbox.fg2, size: 10pt)
  show quote.where(block: true): it => block(
    inset: (left: 12pt, rest: 8pt),
    stroke: (left: 3pt + gruvbox.blue_d),
    text(fill: gruvbox.fg3, it.body)
  )
  show strong: set text(fill: gruvbox.fg0)
  show emph: set text(fill: gruvbox.fg3)
  body
}
