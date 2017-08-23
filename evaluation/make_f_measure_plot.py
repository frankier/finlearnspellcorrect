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
data = data[:, 2]
recall = data
precision = (1 / x) * data
for idx in range(3):
    beta = idx + 1
    data = ((1 + beta*beta) * (precision * recall) /
            ((beta * beta * precision) + recall))
    plt.step(x, data, label=r"$\beta = {}$".format(beta), where='post')

plt.legend()

x_max = 10
plt.xlim(1, x_max)
plt.ylim(0, 0.15)
plt.xticks(np.arange(1, x_max, 1))

plt.title(r"F-measure varying with rank for $\beta = 1, 2, 3$")
plt.xlabel("Rank")
plt.ylabel("F-measure")

plt.savefig('f-measure.pgf')
plt.show()
