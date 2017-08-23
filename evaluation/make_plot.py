import sys
import pickle
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

LABELS = ['Candidate 1', 'Candidate 2', 'Mean']

mpl.use("pgf")
pgf_with_rc_fonts = {
    "font.family": "serif",
    "font.serif": [],
    "font.sans-serif": [],
}
mpl.rcParams.update(pgf_with_rc_fonts)

x, data = pickle.loads(sys.stdin.buffer.read())
for i in range(3):
    plt.step(x, data[:, i], label=LABELS[i], where='post')

plt.legend()

x_max = int(sys.argv[1])
plt.xlim(1, x_max)
plt.ylim(0, 0.3)
plt.xticks(np.arange(1, x_max, int(sys.argv[2])))

plt.title("Mean recall against rank (R@K)")
plt.xlabel("Rank")
plt.ylabel("Recall")

plt.savefig('ranks.pdf')
plt.savefig('ranks.pgf')
plt.show()
