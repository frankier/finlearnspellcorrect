extern crate fst;
extern crate lmdb_rs as lmdb;
extern crate byteorder;
#[macro_use]
extern crate clap;

use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::io;
use std::io::Error as IoError;
use std::path::Path;
use std::env;
use fst::{MapBuilder, Map};
use lmdb::{EnvBuilder, DbFlags};
use lmdb::{ToMdbValue, FromMdbValue, MdbValue};
use std::mem;
use std::str;
use std::slice;
use std::fs;
use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::string::FromUtf8Error;

struct MdbableU64Vec<'a>(&'a [u64]);

impl<'b> ToMdbValue for MdbableU64Vec<'b> {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            return MdbValue::new(std::mem::transmute(self.0.as_ptr()),
                                 self.0.len() * mem::size_of::<u64>());
        }
    }
}

impl<'a> FromMdbValue for MdbableU64Vec<'a> {
    fn from_mdb_value(value: &MdbValue) -> MdbableU64Vec<'a> {
        unsafe {
            let ptr = mem::transmute(value.get_ref());
            return MdbableU64Vec(&slice::from_raw_parts(
                ptr, value.get_size() / mem::size_of::<u64>()));
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
    type Item = Result<(String, u64), PreindexReaderError>;

    fn next(&mut self) -> Option<Result<(String, u64), PreindexReaderError>> {
        fn read_len(mut f: &File, token_len: u64) -> Result<(String, u64), PreindexReaderError> {
            let mut buf = Vec::with_capacity(token_len as usize);
            f.read_exact(buf.as_mut_slice())?;
            return Ok((String::from_utf8(buf)?, f.read_u64::<BigEndian>()?));
        }

        match self.0.read_u64::<BigEndian>() {
            Ok(token_len) => Some(read_len(self.0, token_len)),
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

fn help() {
    println!("usage:
movie_search preindex <collection> <preindex> <docs>
    Tokenize/preindex <collection> to <preindex>.
movie_search fstindex <preindex> <fstindex> <postings>
    Create FST index of <preindex> to <fstindex>
movie_search repl <fstindex> <postings> <docs>
    Search repl of <fstindex>.
    Will also print the original document if <original> is provided.
    ");
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

fn preindex(collection_fn: &str, preindex_fn: &str, docs_fn: &str, lowercase: bool) {
    /// Takes three file paths. Extracts tokens from pretokenised collection_fn, sorts in-memory
    /// and writes preliminary index to preindex_fn. Documents are writen to docs_fn.
    let mut lines = Vec::<(String, u64)>::with_capacity(1000);

    del_if_exists(docs_fn);
    let env = EnvBuilder::new().open(docs_fn, 0o777).unwrap();
    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle);
        let inf = File::open(collection_fn).unwrap();
        let inf_buf = BufReader::new(inf);
        // Read the file contents into a string, returns `io::Result<usize>`
        let mut lineno = 0;
        for line_res in inf_buf.lines() {
            let line = line_res.unwrap();
            db.set(&lineno, &line).unwrap();
            for token in line.split(' ') {
                if lowercase {
                    lines.push((token.to_lowercase().to_owned(), lineno));
                } else {
                    lines.push((token.to_owned(), lineno));
                }
            }
            lineno += 1;
        }
    }

    txn.commit().unwrap();
    lines.sort();
    lines.dedup();

    {
        let mut outf = open_new(preindex_fn);

        for (token, lineno) in lines {
            outf.write_u64::<BigEndian>(token.len() as u64).unwrap();
            outf.write_all(token.as_bytes()).unwrap();
            outf.write_u64::<BigEndian>(lineno as u64).unwrap();
        }
    }
}

fn mkstopwords(preindex_fn: &str, stopwords_fn: &str, num_stopwords: u64) {
            /*outf.write_u64::<BigEndian>(token.len() as u64).unwrap();
            outf.write_all(token.as_bytes()).unwrap();
            outf.write_u64::<BigEndian>(lineno as u64).unwrap();*/

}

fn fstindex(preindex_fn: &str, fstindex_fn: &str, posting_fn: &str, stopwords_fn: Option<&str>) {
    // input
    let mut preindex = File::open(preindex_fn).unwrap();
    // fst
    let wtr = BufWriter::new(File::create(fstindex_fn).unwrap());
    let mut map_builder = MapBuilder::new(wtr).unwrap();
    // set up postings db
    del_if_exists(posting_fn);
    let env = EnvBuilder::new().open(posting_fn, 0o777).unwrap();
    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    {
        let postings_db = txn.bind(&db_handle);
        let mut term_buf = Vec::with_capacity(100);
        let mut postings = Vec::with_capacity(50);

        let mut prev_term = String::new();
        let mut first_iter = true;
        let mut idx = 0;
        loop {
            // XXX: Should check type of error here
            let term = preindex
                .read_u64::<BigEndian>()
                .map(|token_len| {
                         term_buf.resize(token_len as usize, 0);
                         preindex.read_exact(term_buf.as_mut_slice()).unwrap();
                         return String::from_utf8(term_buf.to_owned()).unwrap();
                     })
                .ok();
            match term {
                None => {
                    map_builder.insert(prev_term.as_str(), idx).unwrap();
                    postings_db.set(&idx, &MdbableU64Vec(&postings)).unwrap();
                    break;
                }
                Some(this_term) => {
                    if this_term != prev_term && !first_iter {
                        map_builder.insert(prev_term.as_str(), idx).unwrap();
                        postings_db.set(&idx, &MdbableU64Vec(&postings)).unwrap();
                        idx += 1;
                        postings.clear();
                    }
                    postings.push(preindex.read_u64::<BigEndian>().unwrap());
                    prev_term = this_term;
                    first_iter = false;
                }
            }
        }
    }
    txn.commit().unwrap();
    map_builder.finish().unwrap();
}

fn repl(fstindex_fn: &str, postings_fn: &str, docs_fn: &str, method: &str) {
    // FST db
    let map = Map::from_path(fstindex_fn).unwrap();
    let env_builder = EnvBuilder::new();
    // Postings db
    let postings_env = env_builder.open(postings_fn, 0o777).unwrap();
    let postings_db_hdl = postings_env.get_default_db(DbFlags::empty()).unwrap();
    let postings_rdr = postings_env.get_reader().unwrap();
    // Docs db
    let docs_env = env_builder.open(docs_fn, 0o777).unwrap();
    let docs_db_hdl = docs_env.get_default_db(DbFlags::empty()).unwrap();
    let docs_rdr = docs_env.get_reader().unwrap();
    {
        let postings_db = postings_rdr.bind(&postings_db_hdl);
        let docs_db = docs_rdr.bind(&docs_db_hdl);

        let stdin = std::io::stdin();
        let lock = stdin.lock();
        for input in lock.lines() {
            let input = input.unwrap();
            let mut postings_lists: Vec<Vec<u64>> = input
                .split(' ')
                .map(|token| {
                    return map.get(token)
                               .map_or(vec![], |posting_id| {
                        return postings_db
                                   .get::<MdbableU64Vec>(&posting_id)
                                   .unwrap()
                                   .0
                                   .to_vec();
                    });
                })
                .collect();
            if postings_lists.len() == 0 {
                println!("Please enter at least one term!");
                continue;
            }
            let mut sort_cmp_count = 0;
            let mut intersect_cmp_count = 0;
            let docs;
            match method {
                "naive" => {
                    docs = intersect_many(postings_lists, &mut intersect_cmp_count);
                }
                "ascending" => {
                    let mut cmp_len = mk_cmp_len(&mut sort_cmp_count);
                    postings_lists.sort_by(&mut (*cmp_len));
                    docs = intersect_many(postings_lists, &mut intersect_cmp_count);
                }
                /*"descending" => {
                    let cmp_len = mk_cmp_len(&mut sort_cmp_count);
                    postings_lists.sort_by(cmp_len.reverse());
                    intersect_many(&postings_lists, &mut intersect_cmp_count);
                }*/
                "3-way-asc" => {
                    docs = vec![];
                }
                _ => {
                    println!("Invalid sort method!");
                    return;
                }
            };
            println!("Sort comparisons: {}", sort_cmp_count);
            println!("Intersect comparisons: {}", intersect_cmp_count);
            println!("Total comparisons: {}",
                     sort_cmp_count + intersect_cmp_count);
            if docs.len() == 0 {
                println!("No results!");
                continue;
            }
            for doc_idx in docs {
                println!("Doc {}", doc_idx);
                let text = docs_db.get::<String>(&doc_idx).unwrap();
                println!("{}", text);
            }
        }
    }
}

fn mk_cmp_len<'a>(cmp_counter: &'a mut u64) -> Box<FnMut(&Vec<u64>, &Vec<u64>) -> Ordering + 'a> {
    return Box::new(move |v1: &Vec<u64>, v2: &Vec<u64>| {
                        *cmp_counter += 1;
                        v1.len().cmp(&v2.len())
                    });
}

fn intersect_many(postings_lists: Vec<Vec<u64>>, mut compares: &mut u64) -> Vec<u64> {
    let (head, tail) = postings_lists.split_first().unwrap();
    return tail.iter()
               .fold(head.to_owned(), |ref acc, ref postings_list| {
        return intersect2(&acc, &postings_list, &mut compares);
    });
}

fn both<A, B>(a: Option<A>, b: Option<B>) -> Option<(A, B)> {
    a.and_then(|a| b.map(|b| (a, b)))
}

fn intersect2(pl1: &Vec<u64>, pl2: &Vec<u64>, compares: &mut u64) -> Vec<u64> {
    let mut intersected = vec![];
    let mut pl1_it = pl1.iter();
    let mut pl2_it = pl2.iter();
    if pl1.len() == 0 || pl2.len() == 0 {
        return vec![];
    }
    let mut p1 = pl1_it.next().unwrap();
    let mut p2 = pl2_it.next().unwrap();
    loop {
        if p1 < p2 {
            *compares += 1;
            match pl1_it.next() {
                Some(p) => p1 = p,
                None => break,
            }
        } else if p2 < p1 {
            *compares += 2;
            match pl2_it.next() {
                Some(p) => p2 = p,
                None => break,
            }
        } else {
            // p1 = p2
            *compares += 2;
            intersected.push(*p1);
            match both(pl1_it.next(), pl2_it.next()) {
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

/*
fn intersect3(pl1: &Vec<u64>, pl2: &Vec<u64>, pl3: &Vec<u64>, compares: &mut u64) -> Vec<u64> {
}
*/

fn main() {
    let matches = clap_app!(irlab1 =>
        (@setting SubcommandRequiredElseHelp)
        (version: "0.0")
        (author: "Frankie Robertson <frankie@robertson.name>")
        (about: "Information Retrival demo for lab 1")
        (@arg verbose: -v --verbose "Print information about the information verbosely")
        (@subcommand preindex =>
            (about: "Preindex a text")
            (@arg COLLECTION: +required "The input file representing the document collection")
            (@arg PREINDEX: +required "The file to output the preindex to")
            (@arg DOCS: +required "The file to write the document database to")
            (@arg lowercase: -l --lower "Lowercase the index"))
        (@subcommand repl =>
            (about: ("Enter a REPL in which search terms can be entered and results will be \
                      returned."))
            (@arg FSTINDEX: +required "Where to output the FST index")
            (@arg POSTINGS: +required "Where to output the postings list from the FST")
            (@arg DOCS: +required "The file to read the document database from")
            (@arg method: -m --method "Method to use to compute n-way intersection"))
        // => (@possible_values: ["naive", "ascending", "3-way-asc"])
        (@subcommand fstindex =>
            (about: "Produce an efficient FST index from a preindex")
            (@arg PREINDEX: +required "The preindex to read from")
            (@arg FSTINDEX: +required "Where to output the FST index")
            (@arg POSTINGS: +required "Where to output the postings list from the FST")
            (@arg stopwords: "A file containing a list of stopwords"))
    ).get_matches();

    match matches.subcommand() {
        ("preindex", Some(sub_m)) => {
            preindex(sub_m.value_of("COLLECTION").unwrap(),
                     sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("DOCS").unwrap(),
                     sub_m.is_present("lowercase"),
                     );
        }
        ("fstindex", Some(sub_m)) => {
            fstindex(sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("FSTINDEX").unwrap(),
                     sub_m.value_of("POSTINGS").unwrap(),
                     sub_m.value_of("STOPWORDS"));
        }
        ("repl", Some(sub_m)) => {
            repl(sub_m.value_of("FSTINDEX").unwrap(),
                 sub_m.value_of("POSTINGS").unwrap(),
                 sub_m.value_of("DOCS").unwrap(),
                 sub_m.value_of("method").unwrap());
        }
        (_, _) => {
            panic!("Shan't")
        }
    }
}
