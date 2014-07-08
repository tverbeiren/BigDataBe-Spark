slides : slides.md
	pandoc -t revealjs slides.md -o slides.html --slide-level=1 --variable revealjs-url=reveal.js -s --variable theme=moon --variable transition=fade --highlight-style espresso --template reveal-template.html --no-highlight --variable hlss=zenburn
slides-doc : slides.md
	cat slides.md | grep -v -e '----' -e '\\ ' | pandoc -t html5 -o slides-doc.html  --highlight-style=tango --css doc-template.css --self-contained --toc
