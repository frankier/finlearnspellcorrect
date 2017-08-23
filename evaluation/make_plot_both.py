import sys
import pickle
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

mpl.use("pgf")
pgf_with_rc_fonts = {
    "font.family": "serif",
    "font.serif": [],
    "font.sans-serif": [],
}
mpl.rcParams.update(pgf_with_rc_fonts)

x, data_pho = pickle.load(open(sys.argv[1], 'rb'))
x, data_lev = pickle.load(open(sys.argv[2], 'rb'))
plt.step(x, data_pho[:, 2], label="Special purpose error model", where='post')
plt.step(x, data_lev[:, 2], label="Levenshtein baseline", where='post')

plt.legend()

plt.xlim(1, 128)
plt.ylim(0, 0.3)
plt.xticks(np.arange(1, 128, 10))

plt.title("Mean recall against rank (R@K)")
plt.xlabel("Rank")
plt.ylabel("Recall")

plt.savefig('both.pgf')
plt.show()
