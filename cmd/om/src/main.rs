use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;

use clap::Parser;
use roaring::RoaringBitmap;
use serde::Deserialize;

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

struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

struct DB {
    pub timeseries_name_to_id: HashMap<String, u32>,
    pub num_timeseries: u32,
    pub single_label_bitmaps: BTreeMap<(String, String), RoaringBitmap>,
    pub naive_column_store: Vec<Vec<Sample>>,
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

    pub fn save(&self, out_file: String) -> Result<(), Box<dyn std::error::Error>> {
        todo!();
    }

    pub fn load(in_file: String) -> Result<DB, Box<dyn std::error::Error>> {
        todo!();
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
        let sample = Sample {
            timestamp_ms: s.timestamp_ms,
            value: s.value,
        };
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

fn build(source_file: String, _index_file: String) -> Result<(), Box<dyn std::error::Error>> {
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
    db.search();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmdline: CmdLine = CmdLine::parse();
    match cmdline.subcmd {
        SubCommand::Build(o) => build(o.source_file, cmdline.file),
        _ => todo!(),
    }
}
