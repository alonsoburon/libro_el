#import "theme.typ": *

#set document(title: "ECL Patterns", author: "Alonso Burón")

#show: ecl-theme

// === Front Matter (roman numerals) ===
#set page(numbering: "i")
#include "ch00-front-matter.typ"

// === Main Matter (arabic, starting at 1) ===
#set page(numbering: "1")
#counter(page).update(1)

#ecl-part-page("I", "Foundations & Source Archetypes")
#include "ch01-foundations.typ"

#ecl-part-page("II", "Full Replace Patterns")
#include "ch02-full-replace.typ"

#ecl-part-page("III", "Incremental Extraction Patterns")
#include "ch03-incremental.typ"

#ecl-part-page("IV", "Load Strategies")
#include "ch04-load-strategies.typ"

#ecl-part-page("V", "The Conforming Playbook")
#include "ch05-conforming.typ"

#ecl-part-page("VI", "Operating the Pipeline")
#include "ch06-operating.typ"

#ecl-part-page("VII", "Serving the Destination")
#include "ch07-serving.typ"

#ecl-part-page("VIII", "Appendix")
#include "ch08-appendix.typ"
