use clap::Parser;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;

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

struct Index {
    pub label_keys: HashMap<String, u8>,
    pub label_values: HashMap<String, u32>,
    pub timeseries_name_to_id: HashMap<String, u32>,
    pub num_timeseries: u32,
}

impl Index {
    pub fn new() -> Index {
        Index {
            label_keys: HashMap::new(),
            label_values: HashMap::new(),
            timeseries_name_to_id: HashMap::new(),
            num_timeseries: 0,
        }
    }
    pub fn get_or_create_timeseries_id(&mut self, timeseries_name: String) -> u32 {
        *(self
            .timeseries_name_to_id
            .entry(timeseries_name)
            .or_insert_with(|| {
                let id = self.num_timeseries;
                self.num_timeseries += 1;
                id
            }))
    }
}

#[derive(Deserialize, Debug)]
struct Labels(BTreeMap<String, String>);

fn get_timeseries_name(l: Labels) -> String {
    l.0.into_iter()
        .map(|(k, v)| k + "=" + &v)
        .reduce(|acc, n| acc + "|" + &n)
        .unwrap()
}

#[derive(Deserialize, Debug)]
struct MetricSample {
    #[serde(rename = "TimestampMs")]
    pub timestamp_ms: i64,

    #[serde(rename = "Value")]
    pub value: f64,

    #[serde(rename = "Labels")]
    pub labels: Labels,
}

fn build(source_file: String, _index_file: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut index = Index::new();
    let source_file = File::open(source_file)?;
    let source_reader = BufReader::new(source_file);
    let metrics = serde_json::Deserializer::from_reader(source_reader)
        .into_iter::<MetricSample>()
        .map(|x| x.unwrap());
    for m in metrics {
        let name = get_timeseries_name(m.labels);
        let id = index.get_or_create_timeseries_id(name);
        if (id + 1) % 100000 == 0 {
            println!("timeseries {}", id + 1);
        }
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmdline: CmdLine = CmdLine::parse();
    match cmdline.subcmd {
        SubCommand::Build(o) => build(o.source_file, cmdline.file),
        _ => todo!(),
    }
}
