use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use clap::Parser;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

/// Ordered map indexer
#[derive(Parser)]
#[clap(name = "om", version = "0.1")]
struct CmdLine {
    /// Index file to operate on
    #[clap(short, long)]
    file: String,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    Build(Build),
    Search(Search),
}

/// Creates a new index at --file from a newline-delimited JSON flat file
#[derive(Parser, Debug)]
struct Build {
    /// File to read and create the index from, in JSON-object-per-line format
    #[clap(short, long)]
    pub source_file: String,
}

/// Searches the index at --file
#[derive(Parser, Debug)]
struct Search {
    /// Tag name, must be exact
    #[clap(short, long)]
    pub name: String,

    /// Tag value prefix
    #[clap(short, long)]
    pub value_prefix: String,
}

#[derive(Deserialize, Debug)]
struct Labels(BTreeMap<String, String>);

#[derive(Deserialize, Debug)]
struct MetricSample {
    #[serde(rename = "TimestampMs")]
    pub timestamp_ms: i64,

    #[serde(rename = "Value")]
    pub value: f64,

    #[serde(rename = "Labels")]
    pub labels: Labels,
}

struct DB {
    pub timeseries_name_to_id: HashMap<String, u32>,
    pub num_timeseries: u32,
    pub single_label_bitmaps: BTreeMap<(String, String), RoaringBitmap>,
    pub naive_column_store: Vec<Vec<(i64, f64)>>,
}

// on-disk representation of items in single_label_bitmaps
#[derive(Serialize, Deserialize)]
struct TagIndex {
    k: String,
    v: String,
    b: Vec<u8>,
}

impl DB {
    pub fn new() -> DB {
        DB {
            timeseries_name_to_id: HashMap::new(),
            num_timeseries: 0,
            single_label_bitmaps: BTreeMap::new(),
            naive_column_store: Vec::new(),
        }
    }

    fn new_buf_writer(base_file: &str, suffix: &str) -> Result<BufWriter<File>, Box<dyn Error>> {
        Ok(BufWriter::new(File::create(format!(
            "{}.{}",
            base_file, suffix
        ))?))
    }

    pub fn save_and_close(self: Self, out_file: String) -> Result<(), Box<dyn Error>> {
        let mut ts_names: Vec<String> = vec!["".to_owned(); self.timeseries_name_to_id.len()];
        for (name, idx) in self.timeseries_name_to_id {
            ts_names[idx as usize] = name;
        }
        let mut names_file = DB::new_buf_writer(&out_file, "names")?;
        for name in ts_names {
            writeln!(names_file, "{}", name)?;
        }
        drop(names_file);

        let mut timeseries_file = DB::new_buf_writer(&out_file, "timeseries")?;
        for ts in self.naive_column_store {
            serde_json::ser::to_writer(&mut timeseries_file, &ts)?;
            timeseries_file.write_all(&[b'\n'])?;
        }
        drop(timeseries_file);

        let mut index_file = DB::new_buf_writer(&out_file, "index")?;
        for ((tag_key, tag_val), bitmap) in self.single_label_bitmaps {
            let mut d = TagIndex {
                k: tag_key,
                v: tag_val,
                b: vec![],
            };
            bitmap.serialize_into(&mut d.b)?;
            serde_json::ser::to_writer(&mut index_file, &d)?;
        }
        drop(index_file);

        Ok(())
    }

    fn new_buf_reader(base_file: &str, suffix: &str) -> Result<BufReader<File>, Box<dyn Error>> {
        Ok(BufReader::new(File::open(format!(
            "{}.{}",
            base_file, suffix
        ))?))
    }

    pub fn load(in_file: String) -> Result<DB, Box<dyn Error>> {
        let mut timeseries_name_to_id: HashMap<String, u32> = HashMap::new();
        let mut num_timeseries = 0u32;
        for l in DB::new_buf_reader(&in_file, "names")?.lines() {
            timeseries_name_to_id.insert(l.unwrap(), num_timeseries);
            num_timeseries += 1;
        }

        let mut naive_column_store = Vec::new();
        let ts_reader = DB::new_buf_reader(&in_file, "timeseries")?;
        let dser = serde_json::Deserializer::from_reader(ts_reader).into_iter::<Vec<(i64, f64)>>();
        for rec in dser {
            naive_column_store.push(rec?);
        }

        let mut single_label_bitmaps = BTreeMap::new();
        let index_file = DB::new_buf_reader(&in_file, "index")?;
        let dser = serde_json::Deserializer::from_reader(index_file).into_iter::<TagIndex>();
        for rec in dser {
            let rec = rec.unwrap();
            let bitmap = RoaringBitmap::deserialize_from(&rec.b[..])?;
            single_label_bitmaps.insert((rec.k, rec.v), bitmap);
        }

        Ok(DB {
            timeseries_name_to_id,
            num_timeseries,
            single_label_bitmaps,
            naive_column_store,
        })
    }

    // gets existing timeseries id, or creates a new timeseries id, for a
    // metric sample with the provided labels.  Updates inverted indexes if necessary.
    fn get_timeseries_id(&mut self, labels: Labels) -> u32 {
        let l = labels.0;
        let full_name = (&l)
            .into_iter()
            .map(|(k, v)| k.to_owned() + "=" + &v)
            .reduce(|acc, n| acc + "|" + &n)
            .unwrap();

        let timeseries_id = *(self
            .timeseries_name_to_id
            .entry(full_name.to_owned())
            .or_insert_with(|| {
                let id = self.num_timeseries;
                self.num_timeseries += 1;
                self.naive_column_store.push(Vec::new());
                id
            }));

        for label_pair in l.into_iter() {
            let bitmap = self.single_label_bitmaps.entry(label_pair).or_default();
            bitmap.insert(timeseries_id);
        }

        timeseries_id
    }

    pub fn ingest(&mut self, s: MetricSample) -> u32 {
        let sample = (s.timestamp_ms, s.value);
        let ts_id = self.get_timeseries_id(s.labels);
        self.naive_column_store[ts_id as usize].push(sample);
        ts_id
    }

    pub fn search(&self) {
        let key_range_start = ("pod".to_owned(), "abc".to_owned());
        let key_range_end = ("pod".to_owned(), "abd".to_owned());
        let start_time = std::time::Instant::now();
        let matches = self
            .single_label_bitmaps
            .range(key_range_start..key_range_end)
            .fold(0u64, |acc, (_, bitmap)| acc + bitmap.len());
        let elapsed = start_time.elapsed();
        println!(
            "found {} matching in {} seconds",
            matches,
            elapsed.as_secs_f32()
        );
    }
}

fn cmd_build(source_file: String, index_file: String) -> Result<(), Box<dyn Error>> {
    let mut db = DB::new();
    let source_file = File::open(source_file)?;
    let source_reader = BufReader::new(source_file);
    for m in serde_json::Deserializer::from_reader(source_reader).into_iter::<MetricSample>() {
        let m = m.unwrap();
        let id = db.ingest(m);
        if (id + 1) % 100000 == 0 {
            println!("timeseries {}", id + 1);
        }
    }
    db.save_and_close(index_file)
}

fn cmd_search(index_file: String) -> Result<(), Box<dyn Error>> {
    let db = DB::load(index_file)?;
    db.search();
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let cmdline: CmdLine = CmdLine::parse();
    match cmdline.subcmd {
        SubCommand::Build(o) => cmd_build(o.source_file, cmdline.file),
        SubCommand::Search(_) => cmd_search(cmdline.file),
    }
}
