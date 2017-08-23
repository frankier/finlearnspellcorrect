hfst-txt2fst -i $1 -o /tmp/tmpfst
hfst-fst2txt -f dot -i /tmp/tmpfst -o $2
