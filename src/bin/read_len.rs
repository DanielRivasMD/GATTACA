////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result, bail};
use clap::Parser;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Compute read length distribution from FASTA files",
    long_about = "Read length distribution from FASTA files\n\
                  Each input is assigned a source label (the filename, or 'stdin' if '-')\n\
                  \n\
                  Example:\n\
                  read_len <(samtools fasta bam1.bam) <(samtools fasta bam2.bam) --output out.csv"
)]
struct Args {
    /// Input FASTA files (use '-' for stdin; cannot mix stdin with other inputs)
    #[arg(required = true, num_args = 1.., value_name = "FASTA")]
    inputs: Vec<String>,

    /// Output CSV file (columns: length, count, source)
    #[arg(short, long, required = true)]
    output: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Derive a human‑readable source label from the input argument
fn source_label(input: &str) -> String {
    if input == "-" {
        "stdin".to_string()
    } else {
        Path::new(input)
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or(input)
            .to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    // Safety: stdin can only be sole input
    if args.inputs.contains(&"-".to_string()) && args.inputs.len() > 1 {
        bail!("Cannot combine stdin ('-') with other input files.");
    }

    // Prepare the CSV output
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create CSV file: {:?}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::Writer::from_writer(buf_out);
    wtr.write_record(["length", "count", "source"])?;

    // Process each input sequentially
    for input in &args.inputs {
        let source = source_label(input);

        // Build a buffered reader
        let reader: Box<dyn BufRead> = if input == "-" {
            Box::new(BufReader::new(io::stdin()))
        } else {
            let file = File::open(input)
                .with_context(|| format!("Failed to open FASTA file: {}", input))?;
            Box::new(BufReader::new(file))
        };

        let mut length_counts: BTreeMap<usize, usize> = BTreeMap::new();
        let mut current_len = 0usize;
        let mut in_sequence = false;

        for line in reader.lines() {
            let line = line.context("Failed to read line")?;
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue;
            }

            if trimmed.starts_with('>') {
                // New header: finalise the previous sequence
                if in_sequence && current_len > 0 {
                    *length_counts.entry(current_len).or_insert(0) += 1;
                    current_len = 0;
                }
                in_sequence = true;
            } else if trimmed.starts_with(';') {
                // Comment line, ignore
                continue;
            } else if in_sequence {
                // Accumulate sequence characters
                current_len += trimmed.len();
            }
        }

        // Last sequence
        if in_sequence && current_len > 0 {
            *length_counts.entry(current_len).or_insert(0) += 1;
        }

        // Write the counts for this source
        for (len, count) in &length_counts {
            wtr.write_record(&[len.to_string(), count.to_string(), source.clone()])?;
        }
    }

    wtr.flush()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
