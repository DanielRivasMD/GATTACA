////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(author, version, about = "Compute read length distribution from FASTA via stdin")]
struct Args {
    /// Output CSV file
    #[arg(short, long, required = true)]
    output: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    let reader = BufReader::new(io::stdin());

    let mut length_counts: BTreeMap<usize, usize> = BTreeMap::new();
    let mut current_len = 0usize;
    let mut in_sequence = false;

    for line in reader.lines() {
        let line = line.context("Failed to read line from stdin")?;
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('>') {
            if in_sequence && current_len > 0 {
                *length_counts.entry(current_len).or_insert(0) += 1;
                current_len = 0;
            }
            in_sequence = true;
        } else if trimmed.starts_with(';') {
            continue;
        } else if in_sequence {
            current_len += trimmed.len();
        }
    }

    if in_sequence && current_len > 0 {
        *length_counts.entry(current_len).or_insert(0) += 1;
    }

    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create CSV file: {:?}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::Writer::from_writer(buf_out);

    wtr.write_record(&["length", "count"])?;
    for (len, count) in length_counts {
        wtr.write_record(&[len.to_string(), count.to_string()])?;
    }
    wtr.flush()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
