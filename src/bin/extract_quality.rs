////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
use clap::Parser;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::io::{self, BufRead, Write};

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Phred+33 score lookup table for ASCII characters 33..73 (0..40)
fn phred_score(c: u8) -> u8 {
    match c {
        b'!' => 0,
        b'"' => 1,
        b'#' => 2,
        b'$' => 3,
        b'%' => 4,
        b'&' => 5,
        b'\'' => 6,
        b'(' => 7,
        b')' => 8,
        b'*' => 9,
        b'+' => 10,
        b',' => 11,
        b'-' => 12,
        b'.' => 13,
        b'/' => 14,
        b'0' => 15,
        b'1' => 16,
        b'2' => 17,
        b'3' => 18,
        b'4' => 19,
        b'5' => 20,
        b'6' => 21,
        b'7' => 22,
        b'8' => 23,
        b'9' => 24,
        b':' => 25,
        b';' => 26,
        b'<' => 27,
        b'=' => 28,
        b'>' => 29,
        b'?' => 30,
        b'@' => 31,
        b'A' => 32,
        b'B' => 33,
        b'C' => 34,
        b'D' => 35,
        b'E' => 36,
        b'F' => 37,
        b'G' => 38,
        b'H' => 39,
        b'I' => 40,
        _ => 0, // fallback for unknown chars (should not happen)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(author, version, about = "Extract per‑position Phred scores from SAM stdin", long_about = None)]
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();
    let l = args.length;
    let sample_size = args.sample;

    let stdin = io::stdin();
    let reader = stdin.lock();

    // Prepare output (stdout)
    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Write header: seq, qual, then positions 1..l
    write!(out, "seq\tqual")?;
    for i in 1..=l {
        write!(out, "\t{}", i)?;
    }
    writeln!(out)?;

    // If sampling, we need to run reservoir sampling
    if let Some(k) = sample_size {
        if k == 0 {
            return Ok(());
        }
        // Reservoir sampling: store (seq, qual, scores) for each selected read
        let mut rng = ChaCha8Rng::seed_from_u64(args.seed);
        let mut reservoir: Vec<(String, String, Vec<u8>)> = Vec::with_capacity(k);
        let mut count = 0usize;

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

            // Compute scores (only if needed later; compute now for simplicity)
            let scores: Vec<u8> = qual.bytes().map(phred_score).collect();

            count += 1;
            if count <= k {
                reservoir.push((seq.to_string(), qual.to_string(), scores));
            } else {
                // Replace a random element with probability k / count
                let j = rng.gen_range(0..count);
                if j < k {
                    reservoir[j] = (seq.to_string(), qual.to_string(), scores);
                }
            }
        }

        // Output the reservoir
        for (seq, qual, scores) in reservoir {
            write!(out, "{}\t{}", seq, qual)?;
            for s in scores {
                write!(out, "\t{}", s)?;
            }
            writeln!(out)?;
        }
    } else {
        // Process all reads (no sampling)
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
            for c in qual.bytes() {
                write!(out, "\t{}", phred_score(c))?;
            }
            writeln!(out)?;
        }
    }

    out.flush()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
