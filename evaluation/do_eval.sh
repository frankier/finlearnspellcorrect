time sh -c 'cat answers | RUST_BACKTRACE=1 cargo run --release -- repl index.fst postings.lmdb ../fst/fixer.fst dump.att > corrected'
time sh -c 'cat answers | RUST_BACKTRACE=1 cargo run --release -- repl index.fst postings.lmdb levenshtein-30 dump.att > correctedlev'
