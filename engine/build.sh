cargo run -- preindex ../OpenSubtitles2016/xml/fi/ preindex.dat tdf.lmdb
cargo run -- fstindex preindex.dat index.fst postings.lmdb
cargo run -- stats index.fst postings.lmdb
