cargo run -- mkstopwords movies.txt stopwords.txt 10
cargo run -- preindex movies.txt preindex.dat tdf.lmdb norm.lmdb docs.lmdb stopwords.txt --lower
cargo run -- fstindex preindex.dat index.fst postings.lmdb
cargo run -- stats index.fst postings.lmdb
