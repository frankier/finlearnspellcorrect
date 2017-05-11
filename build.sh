cargo run -- preindex movies.txt preindex.dat tdf.lmdb norm.lmdb docs.lmdb
cargo run -- fstindex preindex.dat index.fst postings.lmdb
cargo run -- stats index.fst postings.lmdb
