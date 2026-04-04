// Semantic palette for ECL Patterns book
// Two themes: "dark" (PDF default, ePub dark mode) and "light" (ePub default)
// Gruvbox-based: dark uses gruvbox-dark, light uses gruvbox-light

#let ecl-palette(theme: "dark") = {
  let dark = theme == "dark"

  (
    // --- Surfaces ---
    bg:         if dark { rgb("#1d2021") } else { white },            // page background (white for print)
    surface:    if dark { rgb("#282828") } else { rgb("#f9f5d7") },  // cards, code blocks, callouts
    border:     if dark { rgb("#504945") } else { rgb("#d5c4a1") },  // borders, separators
    border-dim: if dark { rgb("#3c3836") } else { rgb("#ebdbb2") },  // subtle borders, table header bg

    // --- Text ---
    fg:         if dark { rgb("#ebdbb2") } else { rgb("#3c3836") },  // body text
    fg-bright:  if dark { rgb("#fbf1c7") } else { rgb("#282828") },  // titles, headings, bold
    fg-dim:     if dark { rgb("#a89984") } else { rgb("#7c6f64") },  // muted text, running header, legend
    fg-muted:   if dark { rgb("#928374") } else { rgb("#928374") },  // gray, same both themes
    fg-subtle:  if dark { rgb("#d5c4a1") } else { rgb("#504945") },  // callout body, table cell text
    fg-quote:   if dark { rgb("#bdae93") } else { rgb("#665c54") },  // quote text, emphasis

    // --- Accents (same hues, shifted for contrast) ---
    red:        if dark { rgb("#fb4934") } else { rgb("#cc241d") },
    green:      if dark { rgb("#b8bb26") } else { rgb("#98971a") },
    yellow:     if dark { rgb("#fabd2f") } else { rgb("#d79921") },
    blue:       if dark { rgb("#83a598") } else { rgb("#458588") },
    purple:     if dark { rgb("#d3869b") } else { rgb("#b16286") },
    aqua:       if dark { rgb("#8ec07c") } else { rgb("#689d6a") },
    orange:     if dark { rgb("#fe8019") } else { rgb("#d65d0e") },

    // --- Accent darks (for callout borders, accent text on surfaces) ---
    red-accent:    if dark { rgb("#cc241d") } else { rgb("#9d0006") },
    green-accent:  if dark { rgb("#98971a") } else { rgb("#79740e") },
    yellow-accent: if dark { rgb("#d79921") } else { rgb("#b57614") },
    blue-accent:   if dark { rgb("#458588") } else { rgb("#076678") },
    purple-accent: if dark { rgb("#b16286") } else { rgb("#8f3f71") },
    aqua-accent:   if dark { rgb("#689d6a") } else { rgb("#427b58") },
    orange-accent: if dark { rgb("#d65d0e") } else { rgb("#af3a03") },

    // --- Semantic roles ---
    link:       if dark { rgb("#83a598") } else { rgb("#458588") },  // = blue
    code-fg:    if dark { rgb("#ebdbb2") } else { rgb("#3c3836") },  // = fg
    code-bg:    if dark { rgb("#282828") } else { rgb("#f2e5bc") },  // = surface
    inline-code:if dark { rgb("#bdae93") } else { rgb("#665c54") },  // = fg-quote

    // --- Table ---
    table-header-bg: if dark { rgb("#3c3836") } else { rgb("#ebdbb2") },
    table-even-bg:   if dark { rgb("#282828") } else { rgb("#f9f5d7") },
    table-odd-bg:    if dark { rgb("#1d2021") } else { white },
  )
}

// CSS hex values for SVG embedding (both themes)
#let ecl-svg-colors(theme: "dark") = {
  let p = ecl-palette(theme: theme)
  // Returns a dictionary of CSS hex strings (without rgb() wrapper)
  let to-hex(c) = {
    // Typst colors can be converted via str()
    // For now, hardcode the hex mappings
    none
  }
  p
}
