import sys
import hfst


def get_fst(fn):
    istr = hfst.HfstInputStream(fn)
    transducers = []
    while not (istr.is_eof()):
        transducers.append(istr.read())
    istr.close()
    assert len(transducers) == 1
    return transducers[0]


fst = get_fst(sys.argv[1])

while 1:
    first = input("1>> ")
    second = input("2>> ")

    inp = hfst.fst({first: second})

    test_fst = fst.copy()
    test_fst.intersect(inp)

    paths = list(test_fst.extract_paths().values())
    if len(paths) > 0:
        print("Match!")
        print(paths[0][0][1])
    else:
        print("None!")
