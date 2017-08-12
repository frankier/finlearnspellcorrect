# -*- coding: utf-8 -*-
import sys
import hfst
from itertools import zip_longest
import base64
import charmetric


finnish_alphabet = [
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
    "å",
    "ä",
    "ö",
]


# ## Utils
def normalise_ipa(dict):
    """
    Normalises the IPA parts in places
    """
    for k in dict:
        for i, val in enumerate(dict[k]):
            val = val.replace('ː', '')
            bits = val.rstrip(')').split('(')
            if len(bits) > 1:
                val = bits[0]
                dict[k].append(''.join(bits))
            dict[k][i] = val


def merge_dicts(d1, d2):
    new = {**d1}
    for k in d2:
        if k in new:
            new[k].extend(d2[k])
        new[k] = d2[k]
    return new


def save_fst(fst, fn):
    out = hfst.HfstOutputStream(filename=fn)
    out.write(finnish2other)
    out.flush()
    out.close()


# ## English phonology
# Based on...
# https://en.wikipedia.org/wiki/English_phonology
# https://en.wikipedia.org/wiki/Help:IPA_for_English
english_vowel_phonetics = {
    # trap, palm, face, comma
    'a': ['æ', 'ɑː', 'eɪ', 'ə'],
    # lot, cloth
    'o': ['ɒ', 'ɔː'],
    # kit, fleece, price
    'i': ['ɪ', 'aɪ'],
    # fleece
    'ee': ['iː'],
    # dress, dress
    'e': ['e', 'ɛ'],
    # strut
    'u': ['ʌ'],
    # foot, goose
    'oo': ['ʊ', 'uː'],
    # choice
    'oi': ['ɔɪ'],
    # goat, goat
    'oa': ['əʊ', 'oʊ'],
    # mouth
    'ou': ['aʊ'],
    # nurse
    'ur': ['ɜː', 'ɜːr'],
    # start
    'ar': ['ɑː', 'ɑːr'],
    # north, force
    'or': ['ɔː', 'ɔːr', 'ɔːr', 'oʊr'],
    # near
    'ear': ['ɪə(r)', 'ɪr'],
    # square
    'are': ['eə(r)', 'ɛr'],
    # cure
    'ure': ['ʊə(r)', 'ʊr'],
    # letter
    'er': ['ə(r)', 'ər'],
    # happy
    'y': ['i'],

    # higher
    'igh': ['aɪ'],
}

normalise_ipa(english_vowel_phonetics)

english_consonant_ident = [
    'b', 'd', 'k', 'p', 't', 'v', 'z',
    'm', 'n', 'w', 'r', 'l'
]
english_consonant_phonetics = {}

for c in english_consonant_ident:
    english_consonant_phonetics[c] = [c]

english_consonant_phonetics.update({
    'c': ['k'],
    'ch': ['ʧ', 'c', 'x'],
    'f': ['f'],
    'th': ['θ', 'ð'],
    'g': ['ɡ', 'dʒ'],
    'j': ['dʒ'],
    'y': ['j'],
    'gh': ['x'],
    's': ['s'],
    'ti': ['ʃ'],
    'h': ['h'],
    'z': ['z'],
    'si': ['ʒ'],
    'ng': ['ŋ'],
})

normalise_ipa(english_consonant_phonetics)

english_phonentics = merge_dicts(
        english_consonant_phonetics,
        english_vowel_phonetics)

# ## Finnish phonology

finnish_vowel_phonetics = {
    'a': ['ɑ'],
    'ä': ['æ'],
    'e': ['e'],
    'i': ['i'],
    'o': ['o'],
    'ö': ['ø'],
    'u': ['u'],
    'y': ['y'],
    'å': ['o'],
}

finnish_consonant_phonetics = {
    'b': ['b', 'p'],  # loan
    'c': ['k'],  # loan
    'd': ['d'],
    'f': ['f', 'v'],  # loan
    'g': ['k', 'ɡ'],  # loan
    'h': ['ç', 'h', 'ɦ', 'x'],
    'j': ['j'],
    'k': ['k'],
    'l': ['l'],
    'm': ['m'],
    'n': ['n', 'm'],
    'p': ['p'],
    'r': ['r'],
    's': ['s'],
    't': ['t'],
    'v': ['ʋ'],
    'w': ['w', 'v'],  # loan
    'x': ['ks', 'k', 'x'],  # loan
    'ng': ['ŋ'],
}

finnish_phonentics = merge_dicts(
        finnish_consonant_phonetics,
        finnish_vowel_phonetics)

# ## Make transducer

ipa_chars = []


def mk_replacer(replacements):
    replacements_re = '[{}]'.format(' | '.join(replacements))
    print(replacements_re)
    return hfst.regex(replacements_re)


def mk_lang_ipa_fst(phonentics, times, weighted=False):
    replacements = []
    # Step 1. Make Finnish -> IPA transducer
    for written, ipas in finnish_phonentics.items():
        for ipa in ipas:
            bits = []
            for upper, lower in zip_longest(written, ipa, fillvalue='0'):
                ipa_char = ('IPA' + lower)
                ipa_chars.append(ipa_char)
                bits.append('{}:"{}"{}'.format(upper, ipa_char, "::1" if weighted else "::0"))
            replacements.append(" ".join(bits))
    replacer = mk_replacer(replacements)
    replacer.repeat_n_to_k(1, times)
    return replacer


finnish2other = mk_lang_ipa_fst(finnish_phonentics, 3)

# Step 2. Make IPA -> English transducer
english2ipa = mk_lang_ipa_fst(english_phonentics, 2)
english2ipa.invert()
save_fst(finnish2other, "finnish2otherbasic.fst")
save_fst(english2ipa, "english2ipa.fst")
finnish2other.compose(english2ipa)

save_fst(finnish2other, "finnish2othernothingelse.fst")

chardist_fst = charmetric.get_fst()
save_fst(chardist_fst, "chardist_fst.fst")
finnish2other.disjunct(chardist_fst)
save_fst(finnish2other, "finnish2englishonce.fst")
finnish2other.repeat_star()

save_fst(finnish2other, "finnish2english.fst")
