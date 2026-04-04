#import "theme.typ": *

#set document(title: "Battle-Tested Data Pipelines", author: "Alonso Burón")

#show: ecl-theme

// === Front Matter (no running header) ===
#set page(numbering: "1")
#[#set heading(offset: 1)
#set page(header: none)
#include "ch00-front-matter.typ"]

// === Main Matter ===

// Each part gets a level-1 heading (for PDF outline hierarchy).
// set heading(offset: 1) shifts chapter = to level 2, == to level 3, etc.
// This gives the PDF outline: Part > Pattern > Section > Subsection.
// part-num sets the heading counter so patterns number as Part.N.

#ecl-part-page(1, "I", "Foundations & Source Archetypes")
#[#set heading(offset: 1)
#include "ch01-foundations.typ"]

#ecl-part-page(2, "II", "Full Replace Patterns")
#[#set heading(offset: 1)
#include "ch02-full-replace.typ"]

#ecl-part-page(3, "III", "Incremental Extraction Patterns")
#[#set heading(offset: 1)
#include "ch03-incremental.typ"]

#ecl-part-page(4, "IV", "Load Strategies")
#[#set heading(offset: 1)
#include "ch04-load-strategies.typ"]

#ecl-part-page(5, "V", "The Conforming Playbook")
#[#set heading(offset: 1)
#include "ch05-conforming.typ"]

#ecl-part-page(6, "VI", "Operating the Pipeline")
#[#set heading(offset: 1)
#include "ch06-operating.typ"]

#ecl-part-page(7, "VII", "Serving the Destination")
#[#set heading(offset: 1)
#include "ch07-serving.typ"]

#ecl-part-page(8, "VIII", "Appendix")
#[#set heading(offset: 1)
#include "ch08-appendix.typ"]
