extern crate fst;
extern crate lmdb_rs as lmdb;
extern crate byteorder;
#[macro_use] extern crate clap;
extern crate itertools;
extern crate minidom;
extern crate xml;
extern crate flate2;
extern crate opensubtitles;
extern crate walkdir;
extern crate option_filter;
extern crate rayon;

use std::error::Error;
use std::fs::{File, remove_dir_all};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::io;
use std::io::Error as IoError;
use std::path::{Component, Path};
use fst::{MapBuilder, Map};
use lmdb::{EnvBuilder, DbFlags};
use lmdb::{ToMdbValue, FromMdbValue, MdbValue};
use std::mem;
use std::str;
use std::slice;
use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::string::FromUtf8Error;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
use std::iter::FromIterator;
use std::iter;
use std::fs::read_dir;
use opensubtitles::{OpenSubtitleStream, FlatStreamBit, Word, SentDelim, SubStreamBit, DelimType};
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use option_filter::OptionFilterExt;
use rayon::prelude::*;

#[derive(Clone, Copy, Debug)]
struct Posting {
    doc_idx: u64,
    snt_idx: u64,
    wrd_idx: u64,
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
    type Item = Result<(String, u64, u64, u64), PreindexReaderError>;

    fn next(&mut self) -> Option<Result<(String, u64, u64, u64), PreindexReaderError>> {
        fn read_record(mut f: &File, token_len: u64) -> Result<(String, u64, u64, u64), PreindexReaderError> {
            let mut buf = vec![0; token_len as usize];
            f.read_exact(buf.as_mut_slice())?;
            return Ok((String::from_utf8(buf)?,
                       f.read_u64::<BigEndian>()?,
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
    remove_dir_all(filename).unwrap();
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

/*
struct Meta {

}

struct Sentence {
    id: u64,
    words: Vec<Token>,
}

enum Token {
    Word {
        id: u64,
        word: String,
    },
    Time {
        id: u64,
        is_end: bool,
        offset: Duration,
    }
}
*/

fn entry_is_subtitle(entry: &DirEntry) -> bool {
    entry.file_type().is_file() &&
        entry.file_name().to_str().map(|s| s.ends_with(".xml.gz")).unwrap_or(false)
}

fn id_of_path(path: &Path) -> Option<u64> {
    let mut components = path.components();
    components.next_back();
    components.next_back().and_then(|comp|
        match comp {
            Component::Normal(path) => Some(path),
            _ => None
        })
        .and_then(|path| path.to_str())
        .and_then(|path| path.parse::<u64>().ok())
}

fn preindex(collection_dir: &str, preindex_fn: &str, tdf_fn: &str) {
    /// Takes three file paths. Extracts tokens from xml files collection_dir, sorts in-memory and
    /// writes preliminary index to preindex_fn. Documents are writen to docs_fn.

    // read in collection
    let walker = WalkDir::new(collection_dir).into_iter();
    println!("Collection dir {}", collection_dir);
    let mut seen = HashSet::new();
    let subtitles = walker
        .filter_map(|e| e.ok()
        .filter(entry_is_subtitle))
        .filter_map(|subtitle_entry| {
            let subtitle_path = subtitle_entry.path();
            let movie_id = id_of_path(subtitle_path).unwrap();
            if seen.contains(&movie_id) {
                None
            } else {
                seen.insert(movie_id);
                Some((movie_id, subtitle_path.to_owned()))
            }
        }).collect_vec();

    println!("{} candidates", subtitles.len());

    let mut lines: Vec<(String, u64, u64, u64)> =
            subtitles.par_iter().flat_map(|&(ref movie_id, ref subtitle_path)| {
        let mut ss = OpenSubtitleStream::from_path(subtitle_path).unwrap();
        let mut should_use = false;
        let mut new_lines = Vec::<(String, u64, u64)>::with_capacity(100);
        let mut cur_sent_id = 0;
        loop {
            match ss.next(){
                Ok(FlatStreamBit::SubStreamBit(bit)) => match bit {
                    SubStreamBit::SentDelim(SentDelim { id, delim_type: DelimType::Start }) => {
                        cur_sent_id = id;
                    }
                    SubStreamBit::SentDelim(SentDelim { id: _, delim_type: DelimType::End }) => {
                    }
                    SubStreamBit::Word(Word { id, word }) => {
                        new_lines.push((word, cur_sent_id, id));
                    }
                    _ => {}
                },
                Ok(FlatStreamBit::Meta(meta)) => {
                    should_use = meta.get(&("source".to_owned(), "original".to_owned()))
                        .map(|e| e.contains("Finnish"))
                        .unwrap_or(false);
                    if !should_use {
                        break;
                    }
                }
                Ok(FlatStreamBit::EndStream) => {
                    break;
                }
                Err(e) => {
                    println!("Skipping {}: {}", subtitle_path.to_string_lossy(), e.description());
                    should_use = false;
                    break;
                }
            }
        }
        // XXX: Can boxing be avoided here?
        if should_use {
            new_lines.into_iter().sorted().into_iter()
                    .map(|(word, snt_idx, wrd_idx)| (word, *movie_id, snt_idx, wrd_idx)).collect_vec().into_par_iter()
        } else {
            vec![].into_par_iter()
        }
    }).collect();

    println!("{} lines", lines.len());

    println!("Sorting");
    // XXX: Not external and needs entire collection
    lines.sort();

    {
        let mut outf = open_new(preindex_fn);
        for &(ref term, doc_idx, snt_idx, wrd_idx) in &lines {
            // term
            outf.write_u64::<BigEndian>(term.len() as u64).unwrap();
            outf.write_all(term.as_bytes()).unwrap();
            // doc index
            outf.write_u64::<BigEndian>(doc_idx).unwrap();
            // sent index
            outf.write_u64::<BigEndian>(snt_idx).unwrap();
            // word index
            outf.write_u64::<BigEndian>(wrd_idx).unwrap();
        }
    }

    // count terms, group by term
    new_db_txn(tdf_fn, |_txn, tdf_db| {
        lines.iter()
            .map(|&(ref token, doc_id, _, _)| (token, doc_id))
            .group_by(|&(ref token, doc_id)| token.to_owned()).into_iter()
            .foreach(|(token, group)| {
                let tdf = group.count() as u64;
                tdf_db.set(&token.as_bytes(), &tdf).unwrap();
            });
    });
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
            .group_by(|&(ref term, _, _, _)| term.to_owned())
            .into_iter()
            .enumerate()
            .foreach(|(idx, (term, group))| {
                let idx = idx as u64;
                map_builder.insert(term.as_str(), idx).unwrap();
                let postings : PostingsList =
                    group.map(|(_, doc_idx, snt_idx, wrd_idx)|
                        Posting {
                            doc_idx: doc_idx,
                            snt_idx: snt_idx,
                            wrd_idx: wrd_idx
                        }).collect();
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

fn repl(fstindex_fn: &str, postings_fn: &str, lowercase: bool, verbose: bool) {
    // FST db
    let map = Map::from_path(fstindex_fn).unwrap();
    // Postings db
    db_rdr(postings_fn, |_postings_rdr, postings_db| {
        // get user input
        let stdin = std::io::stdin();
        let lock = stdin.lock();
        for input in lock.lines() {
            let input = input.unwrap();
            // XXX: Should tokenize query properly (deal with punctuation)
            // XXX: Copy here not strictly neccesary
            let terms = tokenize(input.as_str(), lowercase).collect_vec();
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
            // XXX: Process multiple terms here
            let docs = &postings_lists[0];
            if docs.len() == 0 {
                println!("No results!");
                continue;
            }

            // Print results
            for &Posting { doc_idx, snt_idx, wrd_idx } in docs {
                println!("Match {} {} {}", doc_idx, snt_idx, wrd_idx);
                //let text = docs_db.get::<String>(&doc_idx).unwrap();
                //println!("{}", text);
            }
        }
    });
}

/*
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
*/

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
            (@arg TDF: +required "The file to output the term document frequencies to"))
        (@subcommand repl =>
            (about: ("Enter a REPL in which search terms can be entered and results will be \
                      returned."))
            (@arg FSTINDEX: +required "The file to read the FST index from")
            (@arg POSTINGS: +required "The file to read the postings list from")
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
    ).get_matches();

    match matches.subcommand() {
        ("preindex", Some(sub_m)) => {
            preindex(sub_m.value_of("COLLECTION").unwrap(),
                     sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("TDF").unwrap());
        }
        ("fstindex", Some(sub_m)) => {
            fstindex(sub_m.value_of("PREINDEX").unwrap(),
                     sub_m.value_of("FSTINDEX").unwrap(),
                     sub_m.value_of("POSTINGS").unwrap());
        }
        ("repl", Some(sub_m)) => {
            repl(sub_m.value_of("FSTINDEX").unwrap(),
                 sub_m.value_of("POSTINGS").unwrap(),
                 sub_m.is_present("lowercase"),
                 matches.is_present("verbose"));
        }
        ("stats", Some(sub_m)) => {
            stats(sub_m.value_of("FSTINDEX").unwrap(),
                  sub_m.value_of("POSTINGS").unwrap());
        }
        (_, _) => {
            panic!("Shan't")
        }
    }
}
