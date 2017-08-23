import struct
import sys
import lmdb
import re
import libhfst
import os
import random
from collections import defaultdict

syllable_re = re.compile(r'[kptgbdsfhvjlrmnczwxq][aeiouyäö]')


def preindex_it(fn):
    preindex = open(fn, 'rb')
    while 1:
        read = preindex.read(8)
        if len(read) == 0:
            break
        (token_len,) = struct.unpack('>Q', read)
        token = preindex.read(token_len)
        (doc_id, sent_id, word_id) = struct.unpack('>QQQ', preindex.read(24))
        yield (token, doc_id, sent_id, word_id)


POSSIBLE_DOCS = [
    103337,  # zombie
    311519,  # man without a past
    116752,  # drifting clouds
    54001,  # inspector palmu
    #1227536,  # rööperi
    83579,  # arvottomat
]
possible_tokens = set()
token_occurences = defaultdict(list)


def get_omorfi():
    from omorfi import omorfi
    omor = omorfi.Omorfi()
    omor.load_from_dir(labelsegment=True)
    return omor


def is_back(tok):
    return 'a' in tok or 'u' in tok or 'o' in tok


def is_front(tok):
    return not is_back(tok)


def get_pos(analysis):
    splat = re.split(r"[{}\[\]]", analysis)
    return splat
#    print(splat)
#    bits = []
#    for split in splat:
#        if split == 'MB':
#            pass
#        elif split == 'WB':
#            pass
#        elif split == 'wB':
#            pass
#        elif split == 'DB':
#            pass
#        elif split == 'XB':
#            pass
#        elif split == 'STUB':
#            pass
#        elif split == 'hyph?':
#            pass
#        else:
#            bits.append(split)
#    return bits


def long_consonant(tok):
    return 'kk' in tok or 'll' in tok or 'pp' in tok or 'tt' in tok or 'lt' in tok or 'lt' in tok or 'rt' in tok or 'nk' in tok or 'mp' in tok or 'nt' in tok or 'mm' in tok or 'nn' in tok


def get_attrs(omor, toks):
    by_attrs = defaultdict(set)
    for token in toks:
        for (analysis, weight) in omor.labelsegment(token):
            if weight > 1:
                continue
            pos = get_pos(analysis)
            for thing in ['PASV', 'POSSG1', 'HAN', 'COND',
                          'KA', 'PA', 'VERB', 'KIN', 'KO', 'INTERROGATIVE']:
                if thing in pos:
                    by_attrs[thing].add(token)
                    by_attrs[(thing, is_front(token))].add(token)
            by_attrs['front' if is_front(token) else 'back'].add(token)
            by_attrs['long' if long_consonant(token) else 'notlong'].add(token)
    return by_attrs


def count_by_attrs(by_attrs):
    for key in by_attrs:
        print(key, len(by_attrs[key]))


def main():
    global possible_tokens
    # Add from movies I have
    preindex = preindex_it(sys.argv[2])
    for (token, doc_id, sent_id, word_id) in preindex:
        token = token.decode('utf-8')
        if doc_id in POSSIBLE_DOCS:
            token_occurences[token].append((doc_id, sent_id, word_id))
            possible_tokens.add(token)
    omor = get_omorfi()
    if sys.argv[1] == 'make':
        #print("Possible tokens", len(possible_tokens))
        possible = []
        # Take only long tokens
        for token in possible_tokens:
            num_syllables = len(syllable_re.findall(token))
            if num_syllables >= 4:
                #print(token, num_syllables)
                possible.append(token)
        #print("Long tokens", len(possible))

        # Check word has right distribution
        occurs_possible = []
        tdf_db = sys.argv[3]
        with lmdb.open(tdf_db) as env:
            txn = env.begin()
            for token in possible:
                occurs = txn.get(token.encode('utf-8'))
                (occurs,) = struct.unpack('<Q', occurs)
                if 2 <= occurs < 30:
                    #print(token, occurs)
                    occurs_possible.append(token)
        print("Distributed tokens", len(occurs_possible))

        by_attrs = get_attrs(omor, occurs_possible)
        print(by_attrs)
        count_by_attrs(by_attrs)
        all = by_attrs['KIN'] | by_attrs['KO'] | by_attrs['COND']
        print('all1', len(all))
        random.seed(42)
        all.update(random.sample(by_attrs['VERB'], 10))
        all.update(random.sample(by_attrs['PASV'], 10))
        all.update(random.sample(by_attrs['POSSG1'], 5))
        print('all2', len(all))
        while 1:
            all.add(random.sample(by_attrs['front'], 1)[0])
            if len(all) >= 50:
                break
            all.add(random.sample(by_attrs['long'], 1)[0])
            if len(all) >= 50:
                break
    else:
        all = set()
        while 1:
            line = input().strip()
            if not line:
                break
            all.add(line)
    print('\n'.join(all))
    print('all3', len(all))
    movie_groups = defaultdict(list)
    for w in all:
        movie_groups[token_occurences[w][0][0]].append((token_occurences[w][0], w))
    for movie, word_info in movie_groups.items():
        word_info.sort()
        print(movie)
        for word in word_info:
            print(word)
    by_attrs = get_attrs(omor, all)
    count_by_attrs(by_attrs)


if __name__ == '__main__':
    main()
