////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
use clap::Parser;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};

use gattaca::reservoir_sample_iter;

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Detect Phred encoding offset from a sample of quality strings
/// Returns `Some(33)` if all observed bytes are in [33, 93]
/// Returns `Some(64)` if all observed bytes are in [64, 124]
/// Returns `None` otherwise
fn detect_encoding(qual_strings: &[&str]) -> Option<u8> {
    if qual_strings.is_empty() {
        return None;
    }

    let mut min_byte = 255u8;
    let mut max_byte = 0u8;

    for q in qual_strings {
        for &b in q.as_bytes() {
            if b < min_byte {
                min_byte = b;
            }
            if b > max_byte {
                max_byte = b;
            }
        }
    }

    if min_byte >= 33 && max_byte <= 93 {
        Some(33)
    } else if min_byte >= 64 && max_byte <= 124 {
        Some(64)
    } else {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(author, version, about = "Extract per‑position Phred scores from SAM stdin")]
struct Args {
    /// Read length to filter (reads of other lengths are discarded)
    #[arg(short, long, default_value_t = 76)]
    length: usize,

    /// Random sample size (if omitted, process all reads)
    #[arg(short, long)]
    sample: Option<usize>,

    /// Random seed (for reproducible sampling)
    #[arg(short, long, default_value_t = 42)]
    seed: u64,

    /// Output file path (if omitted, writes to stdout)
    #[arg(short, long)]
    output: Option<String>,

    /// Force Phred encoding (33 or 64). Overrides auto‑detection.
    #[arg(long, value_parser = |s: &str| -> Result<u8, String> {
        match s {
            "33" => Ok(33),
            "64" => Ok(64),
            _ => Err(format!("Invalid phred offset '{}', must be 33 or 64", s)),
        }
    })]
    phred: Option<u8>,

    /// Number of lines to scan for encoding auto‑detection
    #[arg(long, default_value_t = 10000)]
    detect_lines: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();
    let l = args.length;
    let sample_size = args.sample;

    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());

    // 1. Read a sample of lines for encoding detection
    let mut detection_buffer = Vec::new();
    let mut qual_samples = Vec::new();

    for _ in 0..args.detect_lines {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break; // EOF
        }
        detection_buffer.push(line);
    }

    // Extract quality strings from the buffered lines for detection.
    for line in &detection_buffer {
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() >= 11 {
            let seq = fields[9];
            let qual = fields[10];
            if seq.len() == l && qual.len() == l {
                qual_samples.push(qual);
            }
        }
    }

    // Determine offset.
    let offset = if let Some(forced) = args.phred {
        forced
    } else {
        match detect_encoding(&qual_samples) {
            Some(offset) => offset,
            None => {
                eprintln!(
                    "Warning: Could not auto‑detect Phred encoding. \
                     Outputting raw ASCII values (offset 0)."
                );
                0
            }
        }
    };

    // 2. Prepare output writer
    let mut out: Box<dyn Write> = if let Some(path) = args.output {
        let file =
            File::create(&path).with_context(|| format!("Cannot create output file: {}", path))?;
        Box::new(BufWriter::new(file))
    } else {
        Box::new(BufWriter::new(io::stdout()))
    };

    // Write header: seq, qual, then positions 1..l
    write!(out, "seq\tqual")?;
    for i in 1..=l {
        write!(out, "\t{}", i)?;
    }
    writeln!(out)?;

    // 3. Process lines
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    if let Some(k) = sample_size {
        if k == 0 {
            return Ok(());
        }

        let all_lines_iter = detection_buffer
            .into_iter()
            .map(Ok::<_, io::Error>)
            .chain(reader.lines());

        // Reservoir sampling over the entire stream
        let selected: Vec<String> =
            reservoir_sample_iter(all_lines_iter.filter_map(Result::ok), k, &mut rng);

        // Output selected reads
        for line in selected {
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 11 {
                continue;
            }
            let seq = fields[9];
            let qual = fields[10];
            if seq.len() != l || qual.len() != l {
                continue;
            }

            write!(out, "{}\t{}", seq, qual)?;
            for &b in qual.as_bytes() {
                let score = b.saturating_sub(offset);
                write!(out, "\t{}", score)?;
            }
            writeln!(out)?;
        }
    } else {
        for line in detection_buffer {
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 11 {
                continue;
            }
            let seq = fields[9];
            let qual = fields[10];
            if seq.len() != l || qual.len() != l {
                continue;
            }

            write!(out, "{}\t{}", seq, qual)?;
            for &b in qual.as_bytes() {
                let score = b.saturating_sub(offset);
                write!(out, "\t{}", score)?;
            }
            writeln!(out)?;
        }

        for line in reader.lines() {
            let line = line.context("Failed to read line")?;
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 11 {
                continue;
            }
            let seq = fields[9];
            let qual = fields[10];
            if seq.len() != l || qual.len() != l {
                continue;
            }

            write!(out, "{}\t{}", seq, qual)?;
            for &b in qual.as_bytes() {
                let score = b.saturating_sub(offset);
                write!(out, "\t{}", score)?;
            }
            writeln!(out)?;
        }
    }

    out.flush()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
