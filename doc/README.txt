Hi,

Welcome to the beginnings of Tufte LaTeX package to help you
produce Tufte style handouts, reports, and notes.

== Quick Start

Try typesetting sample.tex with the following sequence
of commands,

 pdflatex sample-handout
 bibtex   sample-handout
 pdflatex sample-handout
 pdflatex sample-handout

The result should look like sample-handout.pdf.

== Troubleshooting

If you encounter errors of the form,

 ! LaTeX Error: File `paralist.sty' not found.

you will need to obtain missing packages from CTAN <http://ctan.org>.
For package installation instructions and answers to many other
questions, see the UK TeX FAQ,

 http://www.tex.ac.uk/cgi-bin/texfaq2html?introduction=yes

or search the news:comp.text.tex group via,

 http://groups.google.com/group/comp.text.tex

== Bugs/Features/Support

For kudos, feature requests, patches, or support requests that you
feel are /particular/ to this Tufte LaTeX package, i.e. not a general
LaTeX issue, please use this project's issue tracker available at

 http://tufte-latex.googlecode.com

== Contributing

Patches are most welcome via the issue tracker!  Submit a series of
high quality patches, and you'll find yourself a developer on this project.

== License

Copyright 2007-2008 Bil Kleb, Bill Wood, and Kevin Godby

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
