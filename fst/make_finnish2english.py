# -*- coding: utf-8 -*-
import sys
import hfst
from itertools import zip_longest
import base64
import epitran
import panphon
from panphon.distance import Distance


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
    out.write(fst)
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

english_phonetics = merge_dicts(
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

finnish_phonetics = merge_dicts(
        finnish_consonant_phonetics,
        finnish_vowel_phonetics)

double_letters = {}

for letter, ipas in finnish_phonetics.items():
    if len(letter) != 1:
        continue
    double_letters[letter + letter] = [
        ipa + "ː" for ipa in ipas if len(ipa) == 1
    ]

finnish_phonetics = merge_dicts(finnish_phonetics, double_letters)

# ## Make transducer

def mk_replacer(replacements):
    replacements_re = '[{}]'.format(' | '.join(replacements))
    print(replacements_re)
    return hfst.regex(replacements_re)


def mk_lang_ipa_fst(phonetics, weighted=False):
    replacements = []
    for written, ipas in phonetics.items():
        for ipa in ipas:
            bits = []
            for upper, lower in zip_longest(written, ft.ipa_segs(ipa), fillvalue='0'):
                ipa_char = '"IPA{}"'.format(lower) if lower != '0' else '0'
                bits.append('{}:{}{}'.format(upper, ipa_char, "::1" if weighted else "::0"))
            replacements.append(" ".join(bits))
    replacer = mk_replacer(replacements)
    replacer.repeat_star()
    return replacer


ft = panphon.FeatureTable()


def to_segs(ipas):
    return {seg_ipa for ipa in ipas for seg_ipa in ft.ipa_segs(ipa)}


def get_ipa_alphabet():
    flite = epitran.flite.Flite()
    english_ipa_alpha = set(flite.arpa_map.values())
    finnish_ipa_alpha = {c for cc in finnish_phonetics.values() for c in cc}
    ipa_alpha = english_ipa_alpha | finnish_ipa_alpha
    ipa_alpha = to_segs(ipa_alpha)
    #ipa_vowels = {sym for sym in ipa_alpha if ft.seg_dict[sym]['syl'] == 1}
    #long_vowels = {v + 'ː' for v in ipa_vowels}
    return ipa_alpha # | long_vowels


distance = Distance()


def get_subst_distance(i1, i2):
    fti1 = ft.word_to_vector_list(i1, numeric=True)[0]
    fti2 = ft.word_to_vector_list(i2, numeric=True)[0]
    return distance.weighted_substitution_cost(fti1, fti2)


def mk_ipa_replacer():
    alphabet = get_ipa_alphabet()
    replacements = []
    for a1 in alphabet:
        for a2 in alphabet:
            ia1 = 'IPA' + a1
            ia2 = 'IPA' + a2
            #weight = distance.weighted_substitution_cost(ft1, ft2)
            dist = get_subst_distance(a1, a2)
            if dist < 1:
                replacements.append('"{}":"{}"::{}'.format(ia1, ia2, dist * 4))
    replacer = mk_replacer(replacements)
    replacer.repeat_star()
    return replacer


def main():
    finnish2other = mk_lang_ipa_fst(finnish_phonetics)

    # Step 2. Make IPA -> English transducer
    ipa2english = mk_lang_ipa_fst(english_phonetics)
    ipa2english.invert()
    ipa2finnish = finnish2other.copy()
    ipa2finnish.invert()
    ipa2english.disjunct(ipa2finnish)

    save_fst(finnish2other, "finnish2ipa.fst")
    save_fst(ipa2english, "ipa2english.fst")
    ipa_wrangle = mk_ipa_replacer()
    finnish2other.compose(ipa_wrangle)
    save_fst(finnish2other, "finnish2wrangledipa.fst")

    finnish2other.compose(ipa2english)
    save_fst(finnish2other, "finnish2englishonce.fst")
    finnish2other.repeat_star()

    save_fst(finnish2other, "finnish2english.fst")


if __name__ == '__main__':
    main()
