"""Produces a kind of a distance matrix between characters in an alphabet.

Copyright (C) 2016 Kimmo Koskenniemi
"""

import sys, io, hfst

vowels = {
    'i':('Close','Front','Unrounded'),
    'y':('Close','Front','Rounded'),
    'u':('Close','Back','Rounded'),
    'e':('Mid','Front','Unrounded'),
    'ö':('Mid','Front','Rounded'), # IPA ø
    'o':('Mid','Back','Rounded'),
    'õ':('Mid','Back','Unrounded'), # IPA ɤ
    'ä':('Open','Front','Unrounded'), # IPA æ
    'a':('Open','Back','Unrounded') # IPA ɑ
    }

cmo = {'Close':1, 'Mid':2, 'Open':3}
fb = {'Front':1, 'Back':2}
ur = {'Unrounded':1, 'Rounded':2}

consonants = {
    'm':('Bilab','Voiced','Nasal'),
    'p':('Bilab','Unvoiced','Stop'),
    'b':('Bilab','Voiced','Stop'),
    'v':('Labdent','Voiced','Fricative'),
    # 'w':('Labdent','Voiced','Fricative'), ##
    'w':('Labiovelar','Voiced','Approximant'), ##
    'f':('Labdent','Unvoiced','Fricative'),
    # 'ð':('Dental', 'Voiced', 'Fricative')
    # 'þ':('Dental', 'Unvoiced', 'Fricative')
    'n':('Alveolar','Voiced','Nasal'),
    't':('Alveolar','Unvoiced','Stop'),
    'z':('Alveolar','Unvoiced','Stop'), ##
    'd':('Alveolar','Voiced','Stop'),
    's':('Alveolar','Unvoiced','Sibilant'),
    'l':('Alveolar','Voiced','Lateral'),
    'r':('Alveolar','Voiced','Tremulant'),
    'j':('Velar','Voiced','Approximant'),
    'k':('Velar','Unvoiced','Stop'),
    'x':('Velar','Unvoiced','Stop'), ##
    'c':('Velar','Unvoiced','Stop'), ##
    'g':('Velar','Voiced','Stop'),
    'h':('Glottal','Unvoiced','Fricative')}

pos = {'Bilab':1, 'Labdent':1, 'Dental':2, 'Alveolar':2, 'Labiovelar':3, 'Velar':3, 'Glottal':4}
voic = {'Unvoiced':1, 'Voiced':2}

def cmodist(x1, x2):
    """Returns a distance of Close/Mid/Open values"""
    return abs(cmo[x2] - cmo[x1])

def posdist(x1, x2):
    """Returns a distance of articulation position values"""
    return abs(pos[x2] - pos[x1])

def adist(x1, x2):
    """Returns a default distance between any symbol values"""
    return (0 if x1 == x2 else 1)

def featmetr(lset1, lset2, f1, f2, f3):
    """Return a list of letter pairs and their distances.  Letter
    pairs and their three distinctive features are in lset1 and
    lset2. Functions f1, f2, f3 calculate the component distances for
    one feature each.  The returned list consist of items like
    't:d::2' suitable for weighterd HFST tools."""
    ll1 = sorted(lset1.keys())
    ll2 = sorted(lset2.keys())
    ml = []
    for l1 in ll1:
        (x1,y1,z1) = lset1[l1]
        for l2 in ll2:
            (x2,y2,z2) = lset2[l2]
            dist = f1(x1,x2) + f2(y1,y2) + f3(z1,z2)
            if dist <= 2:
                ml.append("{}:{}::{}".format(l1,l2,dist * 2))
    return (ml)

def printlset(lset):
    """Print the set of letters and their features for debugging"""
    ll = sorted(lset.keys());
    flist = []
    for l in ll:
        (x,y,z) = lset[l]
        flist.append("{} : {},{},{}".format(l, x, y, z))
    print('\n'.join(flist))


def get_fst():
    #printlset(consonants)

    # Distances between any two vowels:
    vvlist = featmetr(vowels, vowels, cmodist, adist, adist)
    # Distances between any two consonants:
    cclist = featmetr(consonants, consonants, posdist, adist, adist)

    vowl = sorted(vowels.keys())
    cons = sorted(consonants.keys())
    letters = sorted(vowl + cons)

    # Deletion of a letter possible at a fairly high cost:
    #dellist = ['{}:Ø::{}'.format(l,3) for l in letters]
    # Insertion of a letter possible at a fairly high cost:
    #epelist = ['Ø:{}::{}'.format(l,3) for l in letters]
    # Doubling only after the letter, not before:
    #dbllist = ['{} Ø:{}::{}'.format(l,l,2) for l in letters]
    # Shortening the second of two identical letters only:
    sholist = ['{} {}:0::{}'.format(l,l,1) for l in letters]
    # Individual treatment of some pairs or sequences:
    speclist = ['k:x s:0::1', 'f:p::1', '0:h::1']

    all = vvlist + cclist + sholist + speclist
    re = '[{}]'.format(' | '.join(all))
    return hfst.regex(re)


def main():
    algfst = get_fst()
    algfile = hfst.HfstOutputStream(filename="chardist.fst")
    algfile.write(algfst)
    algfile.flush()
    algfile.close()


if __name__ == '__main__':
    main()
