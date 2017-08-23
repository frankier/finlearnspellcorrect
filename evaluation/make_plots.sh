cat corrected | python3 dump_hist.py > hist.cor.dat
cat correctedlev | python3 dump_hist.py > hist.corlev.dat

python3 make_plot.py 60 5 < hist.cor.dat
python3 make_plot.py 128 10 < hist.corlev.dat

python3 make_plot_both.py hist.cor.dat hist.corlev.dat
