extern crate fst;
extern crate lmdb_rs as lmdb;
extern crate byteorder;
#[macro_use] extern crate clap;
extern crate itertools;

use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::io;
use std::io::Error as IoError;
use std::path::Path;
use fst::{MapBuilder, Map};
use lmdb::{EnvBuilder, DbFlags};
use lmdb::{ToMdbValue, FromMdbValue, MdbValue, MDB_stat};
use std::mem;
use std::str;
use std::slice;
use std::fs;
use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::string::FromUtf8Error;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
use std::iter::FromIterator;

#[derive(Clone, Copy, Debug)]
struct Posting {
    doc_idx: u64,
    tf: u64,
}

type PostingsList = Vec<Posting>;

struct MdbPostingList<'a>(&'a [Posting]);

impl<'b> ToMdbValue for MdbPostingList<'b> {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            return MdbValue::new(std::mem::transmute(self.0.as_ptr()),
                                 self.0.len() * mem::size_of::<u64>() * 2);
        }
    }
}

impl<'a> FromMdbValue for MdbPostingList<'a> {
    fn from_mdb_value(value: &MdbValue) -> MdbPostingList<'a> {
        unsafe {
            let ptr = mem::transmute(value.get_ref());
            return MdbPostingList(&slice::from_raw_parts(
                ptr, value.get_size() / (mem::size_of::<u64>() * 2)));
        }
    }
}

struct PreindexReader<'a>(&'a mut File);

#[derive(Debug)]
enum PreindexReaderError {
    Io(IoError),
    Utf8(FromUtf8Error)
}

impl From<IoError> for PreindexReaderError {
    fn from(err: IoError) -> PreindexReaderError {
        PreindexReaderError::Io(err)
    }
}

impl From<FromUtf8Error> for PreindexReaderError {
    fn from(err: FromUtf8Error) -> PreindexReaderError {
        PreindexReaderError::Utf8(err)
    }
}

impl<'a> Iterator for PreindexReader<'a> {
    type Item = Result<(String, u64, u64), PreindexReaderError>;

    fn next(&mut self) -> Option<Result<(String, u64, u64), PreindexReaderError>> {
        fn read_record(mut f: &File, token_len: u64) -> Result<(String, u64, u64), PreindexReaderError> {
            let mut buf = vec![0; token_len as usize];
            f.read_exact(buf.as_mut_slice())?;
            return Ok((String::from_utf8(buf)?,
                       f.read_u64::<BigEndian>()?,
                       f.read_u64::<BigEndian>()?));
        }

        match self.0.read_u64::<BigEndian>() {
            Ok(token_len) => Some(read_record(self.0, token_len)),
            Err(err) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    None
                } else {
                    Some(Err(PreindexReaderError::from(err)))
                }
            }
        }
    }
}

fn open_new(filename: &str) -> File {
    let path = Path::new(filename);
    let display = path.display();

    return match File::create(&path) {
               // The `description` method of `io::Error` returns a string that
               // describes the error
               Err(why) => panic!("couldn't create {}: {}", display, why.description()),
               Ok(file) => file,
           };
}

fn del_if_exists(filename: &str) {
    if !Path::new(filename).exists() {
        return;
    }

    println!("Removing existing file {}", filename);
    fs::remove_dir_all(filename).unwrap();
}

fn get_env(db_fn: &str) -> lmdb::Environment {
    let env = EnvBuilder::new().open(db_fn, 0o777).unwrap();
    env.set_mapsize(1_000_000_000).unwrap(); // 1gb
    env
}

fn new_db_txn<F>(db_fn: &str, cb: F)
        where F: FnOnce(&lmdb::Transaction, &lmdb::Database) {
    del_if_exists(db_fn);
    let env = get_env(db_fn);
    let db_handle = (&env).get_default_db(DbFlags::empty()).unwrap();
    let txn = (&env).new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        cb(&txn, &db);
    }
    txn.commit().unwrap();
}

fn db_rdr<F>(db_fn: &str, cb: F)
        where F: FnOnce(&lmdb::ReadonlyTransaction, &lmdb::Database) {
    let env = get_env(db_fn);
    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let rdr = env.get_reader().unwrap();
    let db = rdr.bind(&db_handle);
    cb(&rdr, &db);
}

fn tokenize<'a>(line: &'a str, lowercase: bool)
        -> std::iter::Map<std::str::Split<'a, char>, fn(&str) -> String> {
    fn lower_token(token: &str) -> String {
        token.to_lowercase()
    }
    fn own_token(token: &str) -> String {
        token.to_owned()
    }
    return line
        .split(' ')
        .map(if lowercase { lower_token } else { own_token });
}

fn read_stopwords(stopwords_fn_opt: Option<&str>) -> HashSet<String> {
    if let Some(stopwords_fn) = stopwords_fn_opt {
        HashSet::from_iter(
            BufReader::new(File::open(stopwords_fn).unwrap())
            .lines().map(|res| res.unwrap()))
    } else {
        HashSet::new()
    }
}

fn preindex(collection_fn: &str, preindex_fn: &str, tdf_fn: &str, norm_fn: &str,
            docs_fn: &str, stopwords_fn_opt: Option<&str>, lowercase: bool) {
    /// Takes three file paths. Extracts tokens from pretokenised collection_fn, sorts in-memory
    /// and writes preliminary index to preindex_fn. Documents are writen to docs_fn.
    let mut lines = Vec::<(String, u64, u64)>::with_capacity(1000);

    new_db_txn(docs_fn, |_docs_txn, docs_db|
        new_db_txn(norm_fn, |_norm_txn, norm_db|
    {
        let stopwords = read_stopwords(stopwords_fn_opt);
        // read in collection
        let inf = File::open(collection_fn).unwrap();
        let inf_buf = BufReader::new(inf);
        // Read the file contents into a string, returns `io::Result<usize>`
        let mut lineno = 0;
        for line_res in inf_buf.lines() {
            let line = line_res.unwrap();

            docs_db.set(&lineno, &line).unwrap();

            let mut sq_sum : u64 = 0;
            let sorted_tokens = tokenize(line.as_str(), lowercase)
                .filter(|term| {
                    !stopwords.contains(term.as_str())
                }).sorted();

            sorted_tokens.into_iter()
                // XXX: Should be able to avoid copy here
                .group_by(|token| token.to_owned()).into_iter()
                .map(|(token, group)| {
                    (token, (group.count()) as u64)
                })
                .foreach(|(token, count)| {
                    lines.push((token, lineno, count));
                    sq_sum += count * count;
                });
            norm_db.set(&lineno, &sq_sum).unwrap();
            lineno += 1;
        }
    }));

    // XXX: Not external and needs entire collection
    lines.sort();

    {
        let mut outf = open_new(preindex_fn);
        for &(ref term, doc_idx, tf) in &lines {
            // term
            outf.write_u64::<BigEndian>(term.len() as u64).unwrap();
            outf.write_all(term.as_bytes()).unwrap();
            // doc index
            outf.write_u64::<BigEndian>(doc_idx).unwrap();
            // term frequency
            outf.write_u64::<BigEndian>(tf).unwrap();
        }
    }

    // count terms, group by term
    new_db_txn(tdf_fn, |_txn, tdf_db| {
        lines.iter()
            .group_by(|&&(ref token, _, _)| token).into_iter()
            .foreach(|(token, group)| {
                let tdf = group.count() as u64;
                tdf_db.set(&token.as_bytes(), &tdf).unwrap();
            });
    });
}

fn mkstopwords(collection_fn: &str, stopwords_fn: &str, num_stopwords: u64, lowercase: bool) {
    // XXX: Not external and needs entire collection
    let sorted_term_counts;
    {
        let mut term_counts = HashMap::<String, u64>::new();
        let inf = File::open(collection_fn).unwrap();
        let inf_buf = BufReader::new(inf);
        for line_res in inf_buf.lines() {
            for token in tokenize(line_res.unwrap().as_str(), lowercase) {
                let count = term_counts.entry(token).or_insert(0);
                *count += 1;
            }
        }
        // XXX: Could use heap based get_top_k rather than sort here
        sorted_term_counts = term_counts.into_iter()
            .map(|(term, count)| (count, term))
            .sorted_by((|a, b| a.cmp(b).reverse()));
    }
    {
        let mut stopwords = open_new(stopwords_fn);
        for &(_, ref term) in &sorted_term_counts.as_slice()[..num_stopwords as usize] {
            stopwords.write(term.as_bytes()).unwrap();
            stopwords.write("\n".as_bytes()).unwrap();
        }
    }
}

fn fstindex(preindex_fn: &str, fstindex_fn: &str, postings_fn: &str) {
    // input
    let mut preindex = File::open(preindex_fn).unwrap();
    // fst
    let wtr = BufWriter::new(File::create(fstindex_fn).unwrap());
    let mut map_builder = MapBuilder::new(wtr).unwrap();
    // set up postings db
    new_db_txn(postings_fn, |_txn, postings_db| {
        let reader = PreindexReader(&mut preindex);

        reader
            .map(|result| result.unwrap())
            .group_by(|&(ref term, _, _)| term.to_owned())
            .into_iter()
            .enumerate()
            .foreach(|(idx, (term, group))| {
                let idx = idx as u64;
                map_builder.insert(term.as_str(), idx).unwrap();
                let postings : PostingsList =
                        group.map(|(_, doc_idx, tf)|
                            Posting { doc_idx: doc_idx, tf: tf }).collect();
                postings_db.set(&idx, &MdbPostingList(&postings)).unwrap();
            });
    });
    map_builder.finish().unwrap();
}

fn stats(fstindex_fn: &str, postings_fn: &str) {
    let map = Map::from_path(fstindex_fn).unwrap();
    println!("Size of dictionary: {}", map.len());
    db_rdr(postings_fn, |_postings_rdr, postings_db| {
        let mut total_postings = 0;
        for cur in postings_db.iter().unwrap() {
            total_postings += cur.get_value::<MdbPostingList>().0.len();
        }
        println!("Total number of postings: {}", total_postings);
    });
}

fn repl(fstindex_fn: &str, postings_fn: &str, tdf_fn: &str, norm_fn: &str,
        docs_fn: &str, stopwords_fn_opt: Option<&str>, method: &str,
        norerank: bool, lowercase: bool, verbose: bool) {
    // FST db
    let map = Map::from_path(fstindex_fn).unwrap();
    // Postings db
    db_rdr(postings_fn, |_postings_rdr, postings_db|
        // Docs db
        db_rdr(docs_fn, |_docs_rdr, docs_db|
    {
        // read stopwords
        let stopwords = read_stopwords(stopwords_fn_opt);
        // count docs
        let MDB_stat {
            ms_entries: num_docs, ..
        } = docs_db.stat().unwrap();
        // get user input
        let stdin = std::io::stdin();
        let lock = stdin.lock();
        for input in lock.lines() {
            let input = input.unwrap();
            // XXX: Should tokenize query properly (deal with punctuation)
            // XXX: Copy here not strictly neccesary
            let terms : Vec<String> = tokenize(input.as_str(), lowercase)
                .filter(|term| {
                    !stopwords.contains(term.as_str())
                }).collect();
            let mut postings_lists: Vec<PostingsList> = terms.iter()
                .map(|token| {
                    return map.get(token)
                        .map_or(vec![], |posting_id|
                            postings_db
                                .get::<MdbPostingList>(&posting_id)
                                .unwrap().0.to_vec()
                    );
                })
                .collect();
            if postings_lists.len() == 0 {
                println!("Please enter at least (indexed) term!");
                continue;
            }
            let mut sort_cmp_count = 0;
            let mut intersect_cmp_count = 0;
            let docs;
            match method {
                "naive" => {
                    docs = intersect_many(&postings_lists, &mut intersect_cmp_count);
                }
                "ascending" => {
                    let mut cmp_len = mk_cmp_len(&mut sort_cmp_count);
                    postings_lists.sort_by(&mut (*cmp_len));
                    docs = intersect_many(&postings_lists, &mut intersect_cmp_count);
                }
                _ => {
                    println!("Invalid sort method!");
                    return;
                }
            };
            if verbose {
                println!("Sort comparisons: {}", sort_cmp_count);
                println!("Intersect comparisons: {}", intersect_cmp_count);
                println!("Total comparisons: {}",
                         sort_cmp_count + intersect_cmp_count);
            }
            if docs.len() == 0 {
                println!("No results!");
                continue;
            }
            if norerank {
                // Print results
                for (doc_idx, _tfs) in docs {
                    println!("Doc {}", doc_idx);
                    let text = docs_db.get::<String>(&doc_idx).unwrap();
                    println!("{}", text);
                }
            } else {
                let mut scores = vec![0.0; docs.len()];
                let mut score_components = vec![vec![0.0; terms.len()]; docs.len()];
                db_rdr(tdf_fn, |_tdf_rdr, tdf_db|
                    db_rdr(norm_fn, |_norm_rdr, norm_db|
                {
                    // Calculate cosine scores
                    let term_weight = 1.0 / (terms.len() as f64).sqrt();
                    for (result_idx, &(ref doc_idx, ref tfs)) in docs.iter().enumerate() {
                        for (term_idx, (term, tf)) in terms.iter().zip(tfs).enumerate() {
                            let tdf = tdf_db.get::<u64>(term).unwrap() as f64;
                            let tf_term = 1.0 + (*tf as f64).log(10.0);
                            // XXX: idf term does not depend on document, only so could be moved outside this loop
                            let idf_term = ((num_docs as f64) / tdf).log(10.0);
                            let component = tf_term * idf_term * term_weight;
                            score_components[result_idx][term_idx] = component;
                            scores[result_idx] += component;
                        }
                        let sq_sum = norm_db.get::<u64>(doc_idx).unwrap();
                        let norm = (sq_sum as f64).sqrt();
                        scores[result_idx] = scores[result_idx] / norm;
                    }
                }));
                // Print results
                for ((score, score_component), (doc_idx, _tf)) in
                        scores.iter()
                            .zip(score_components)
                            .zip(docs)
                            .sorted_by(|&((score_a, _), _), &((score_b, _), _)|
                                score_a.partial_cmp(score_b)
                                .unwrap()
                                .reverse())
                {
                    println!("Doc {}; Score {}", doc_idx, score);
                    println!("Score components {}", itertools::join(score_component, ", "));
                    let text = docs_db.get::<String>(&doc_idx).unwrap();
                    println!("{}", text);
                }
            }
        }
    }));
}

fn mk_cmp_len<'a>(cmp_counter: &'a mut u64) -> Box<FnMut(&PostingsList, &PostingsList) -> Ordering + 'a> {
    return Box::new(move |v1: &PostingsList, v2: &PostingsList| {
                        *cmp_counter += 1;
                        v1.len().cmp(&v2.len())
                    });
}

fn intersect_many(postings_lists: &Vec<PostingsList>, mut compares: &mut u64) -> Vec<(u64, Vec<u64>)> {
    let (head, tail) = postings_lists.split_first().unwrap();
    return tail.iter()
               .fold(
                   head.into_iter().map(
                       |&Posting { doc_idx, tf} | (doc_idx, vec![tf])).collect(),
                   |acc, postings_iter|
                    intersect2(acc.iter(), postings_iter.iter(), &mut compares)
                );
}

fn both<A, B>(a: Option<A>, b: Option<B>) -> Option<(A, B)> {
    a.and_then(|a| b.map(|b| (a, b)))
}

fn intersect2<'a, 'b, I1, I2>(pl1_it: I1, pl2_it: I2, compares: &mut u64) -> Vec<(u64, Vec<u64>)>
        where I1: Iterator<Item=&'a (u64, Vec<u64>)>, I2: Iterator<Item=&'b Posting> {
    let mut intersected = vec![];
    let mut pl1_peek = pl1_it.peekable();
    let mut pl2_peek = pl2_it.peekable();
    if pl1_peek.peek().is_none() || pl2_peek.peek().is_none() {
        return vec![];
    }
    let mut p1 = pl1_peek.next().unwrap();
    let mut p2 = pl2_peek.next().unwrap();
    loop {
        if p1.0 < p2.doc_idx {
            *compares += 1;
            match pl1_peek.next() {
                Some(p) => {
                    p1 = p;
                },
                None => break,
            }
        } else if p2.doc_idx < p1.0 {
            *compares += 2;
            match pl2_peek.next() {
                Some(p) => {
                    p2 = p;
                },
                None => break,
            }
        } else {
            // p1 = p2
            *compares += 2;
            // XXX: This copy can probably be avoided somehow
            let mut tfs = p1.1.to_owned();
            tfs.push(p2.tf);
            intersected.push((p1.0, tfs));
            match both(pl1_peek.next(), pl2_peek.next()) {
                Some((p1n, p2n)) => {
                    p1 = p1n;
                    p2 = p2n;
                }
                None => break,
            }
        }
    }
    return intersected;
}

fn main() {
    let matches = clap_app!(movie_search =>
        (@setting SubcommandRequiredElseHelp)
        (version: "0.0")
        (author: "Frankie Robertson <frankie@robertson.name>")
        (about: "Information Retrival demo for lab 1")
        (@arg verbose: -v --verbose "Print information about the information verbosely")
        (@subcommand preindex =>
            (about: "Preindex a text")
            (@arg COLLECTION: +required "The input file representing the document collection")
            (@arg PREINDEX: +required "The file to output the preindex to")
            (@arg TDF: +required "The file to output the term document frequencies to")
            (@arg NORM: +required "The file to output the document norms to")
            (@arg DOCS: +required "The file to write the document database to")
            (@arg STOPWORDS: "The file to read stopwords from")
            (@arg lowercase: -l --lower "Lowercase the index"))
        (@subcommand repl =>
            (about: ("Enter a REPL in which search terms can be entered and results will be \
                      returned."))
            (@arg FSTINDEX: +required "The file to read the FST index from")
            (@arg POSTINGS: +required "The file to read the postings list from")
            (@arg TDF: +required "The file to read the term document frequencies from")
            (@arg NORM: +required "The file to read the document norms from")
            (@arg DOCS: +required "The file to read the document database from")
            (@arg STOPWORDS: "The file to read stopwords from")
            (@arg method: -m --method +takes_value "Method to use to compute n-way intersection")
            // => (@possible_values: ["naive", "ascending"])
            // XXX: Should use an enum
            (@arg norerank: -r --norerank "Don't rerank results using tdf-idf scoring")
            (@arg lowercase: -l --lower "Lowercase the index"))
        (@subcommand stats =>
            (about: ("Read stats about the index and postings lists."))
            (@arg FSTINDEX: +required "The file to output the FST index")
            (@arg POSTINGS: +required "The file to output the postings list from the FST"))
        (@subcommand fstindex =>
            (about: "Produce an efficient FST index from a preindex")
            (@arg PREINDEX: +required "The preindex to read from")
            (@arg FSTINDEX: +required "The file to output the FST index")
            (@arg POSTINGS: +required "The file to output the postings list from the FST")
            (@arg stopwords: "A file containing a list of stopwords"))
        (@subcommand mkstopwords =>
            (about: "Produce an list of stopwords from a preindex")
            (@arg COLLECTION: +required "The preindex to read from")
            (@arg STOPWORDS: +required "The file to output the postings list from the FST")
            (@arg N: "The number of stopwords to produce")
            (@arg lowercase: -l --lower "Lowercase the stopwords"))
    ).get_matches();

    match matches.subcommand() {
        ("preindex", Some(sub_m)) => {
            preindex(sub_m.value_of("COLLECTION").unwrap(),
                     sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("TDF").unwrap(),
                     sub_m.value_of("NORM").unwrap(),
                     sub_m.value_of("DOCS").unwrap(),
                     sub_m.value_of("STOPWORDS"),
                     sub_m.is_present("lowercase"));
        }
        ("fstindex", Some(sub_m)) => {
            fstindex(sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("FSTINDEX").unwrap(),
                     sub_m.value_of("POSTINGS").unwrap());
        }
        ("repl", Some(sub_m)) => {
            repl(sub_m.value_of("FSTINDEX").unwrap(),
                 sub_m.value_of("POSTINGS").unwrap(),
                 sub_m.value_of("TDF").unwrap(),
                 sub_m.value_of("NORM").unwrap(),
                 sub_m.value_of("DOCS").unwrap(),
                 sub_m.value_of("STOPWORDS"),
                 sub_m.value_of("method").unwrap_or("ascending"),
                 sub_m.is_present("norerank"),
                 sub_m.is_present("lowercase"),
                 matches.is_present("verbose"));
        }
        ("stats", Some(sub_m)) => {
            stats(sub_m.value_of("FSTINDEX").unwrap(),
                  sub_m.value_of("POSTINGS").unwrap());
        }
        ("mkstopwords", Some(sub_m)) => {
            mkstopwords(sub_m.value_of("COLLECTION").unwrap(),
                        sub_m.value_of("STOPWORDS").unwrap(),
                        value_t!(sub_m, "N", u64).unwrap_or(10),
                        sub_m.is_present("lowercase"));
        }
        (_, _) => {
            panic!("Shan't")
        }
    }
}
