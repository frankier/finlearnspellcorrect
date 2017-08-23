import sys
import pickle
import fileinput
import numpy as np

correct = [
    'maksamatta',
    'saneerataan',
    'vähennetään',
    'veljelleni',
    'syksymmällä',
    'tyttöseni',
    'varojeni',
    'henkilökohtaisesti',
    'mukanani',
    'lähetettiin',
    'kävelemään',
    'työskennellyt',
    'ajattelematon',
    'henkilökuntaa',
    'kiinteistöjä',
    'minustakin',
    'kyllästyttää',
    'sopisivat',
    'työskentelee',
    'työttömyyskorvausta',
    'sydänkäpyseni',
    'Epäilemättä',
    'riittävästi',
    'kyselevät',
    'mennessäni',
    'tupakoida',
    'ryhdistäytyä',
    'minullakin',
    'lajitella',
    'Ajattelinkin',
    'kuljeskella',
    'järjestellä',
    'käteisellä',
    'ymmärrettävää',
    'asemassanne',
    'samantekevää',
    'kuulusteltava',
    'selvitetty',
    'tuomittava',
    'väkivallan',
    'välittömästi',
    'toimitetaan',
    'jättäminen',
    'tekisitte',
    'Ymmärrättekö',
    'mielestäni',
    'lähettänyt',
    'puolestani',
    'meikäläisiä',
    'palattava',
]

NUM_RANKS = 128
NUM_RESULTS = 50
NUM_SUBJS = 2
i = -1
current_query = ""
data = np.zeros((NUM_RANKS, NUM_SUBJS))

subj = 0
ans = 0
rank = 0
correct_word = None
correct_score = None
current_rank = None


def finalise_result(subj, rank):
    if rank is None or rank >= NUM_RANKS:
        return
    print(rank, subj)
    data[rank, subj] += 1


for line in fileinput.input():
    if line.startswith('No results!'):
        continue
    elif line.startswith('Match'):
        _, word, score = line.strip().split(' ')
        if word == correct_word:
            correct_score = score
        if score == correct_score:
            current_rank = rank
        rank += 1
    else:
        finalise_result(subj, current_rank)
        current_query = line.strip()
        i += 1
        subj = i // NUM_RESULTS
        ans = i % NUM_RESULTS
        print('**', subj, ans)
        correct_word = correct[ans].lower()
        correct_score = None
        current_rank = None
        rank = 0

finalise_result(subj, current_rank)

x = np.arange(1, NUM_RANKS + 1)
data = np.cumsum(data, axis=0)
data /= 50.0
data = np.hstack((data, np.mean(data, axis=1)[:, np.newaxis]))
sys.stdout.buffer.write(pickle.dumps((x, data)))
sys.stdout.buffer.close()
