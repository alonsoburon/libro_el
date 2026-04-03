#import "theme.typ": gruvbox, ecl-theme

#set document(title: "ECL Patterns", author: "Alonso Burón")

#show: ecl-theme

// Front Matter
#include "ch00-front-matter.typ"
#pagebreak()

// Part I -- Foundations & Source Archetypes
#include "ch01-foundations.typ"
#pagebreak()

// Part II -- Full Replace Patterns
#include "ch02-full-replace.typ"
#pagebreak()

// Part III -- Incremental Extraction Patterns
#include "ch03-incremental.typ"
#pagebreak()

// Part IV -- Load Strategies
#include "ch04-load-strategies.typ"
#pagebreak()

// Part V -- The Conforming Playbook
#include "ch05-conforming.typ"
#pagebreak()

// Part VI -- Operating the Pipeline
#include "ch06-operating.typ"
#pagebreak()

// Part VII -- Serving the Destination
#include "ch07-serving.typ"
#pagebreak()

// Appendix
#include "ch08-appendix.typ"
