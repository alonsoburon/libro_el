#import "theme.typ": *

#set document(title: "ECL Patterns", author: "Alonso Burón")

#show: ecl-theme

// === Front Matter (roman numerals, offset so headings are level 2 like patterns) ===
#set page(numbering: "i")
#[#set heading(offset: 1)
#include "ch00-front-matter.typ"]

// === Main Matter (arabic, starting at 1) ===
#set page(numbering: "1")
#counter(page).update(1)

// Each part gets a level-1 heading (for PDF outline hierarchy).
// set heading(offset: 1) shifts chapter = to level 2, == to level 3, etc.
// This gives the PDF outline: Part > Pattern > Section > Subsection.

#ecl-part-page("I", "Foundations & Source Archetypes")
#[#set heading(offset: 1)
#include "ch01-foundations.typ"]

#ecl-part-page("II", "Full Replace Patterns")
#[#set heading(offset: 1)
#include "ch02-full-replace.typ"]

#ecl-part-page("III", "Incremental Extraction Patterns")
#[#set heading(offset: 1)
#include "ch03-incremental.typ"]

#ecl-part-page("IV", "Load Strategies")
#[#set heading(offset: 1)
#include "ch04-load-strategies.typ"]

#ecl-part-page("V", "The Conforming Playbook")
#[#set heading(offset: 1)
#include "ch05-conforming.typ"]

#ecl-part-page("VI", "Operating the Pipeline")
#[#set heading(offset: 1)
#include "ch06-operating.typ"]

#ecl-part-page("VII", "Serving the Destination")
#[#set heading(offset: 1)
#include "ch07-serving.typ"]

#ecl-part-page("VIII", "Appendix")
#[#set heading(offset: 1)
#include "ch08-appendix.typ"]
