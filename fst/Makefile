objects = \
	finnish2english.fst \
	err.fst \
	fixer.fst fixer.min.fst fixer.opt.fst

 #err.min.fst \
#chardist.fst \

.PHONY: all clean
all: $(objects)

clean:
	-rm $(objects)

#chardist.fst:
	#python3 charmetric.py

finnish2english.fst:
	python3 ./make_finnish2english.py

err.fst: finnish2english.fst
	cp finnish2english.fst err.fst

#err.min.fst: finnish2english.fst
	#hfst-minimise -i finnish2english.fst -o err.min.fst

fixer.fst: err.fst
	hfst-invert -i err.fst -o fixer.fst

fixer.min.fst: fixer.fst
	hfst-minimise -E -i fixer.fst -o fixer.min.fst

fixer.opt.fst: fixer.min.fst
	hfst-fst2fst -O -i fixer.min.fst -o fixer.opt.fst
