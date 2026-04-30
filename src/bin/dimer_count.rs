////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result, bail};
use clap::Parser;
use csv::WriterBuilder;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};

use gattaca::reservoir_sample_iter;

////////////////////////////////////////////////////////////////////////////////////////////////////

const DIMERS: [&str; 16] = [
    "AA", "AC", "AG", "AT", "CA", "CC", "CG", "CT", "GA", "GC", "GG", "GT", "TA", "TC", "TG", "TT",
];

////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: add a sample flag similar to extract quality
#[derive(Parser)]
#[command(
    author,
    version,
    about = "Count dimers in DNA sequences from SAM input files",
    long_about = "Read SAM files and count all 16 dimers for each read\n\
                  Outputs a CSV with columns: sequence, AA, AC, ..., TT, label (label = 0‑based file index)\n\
                  Optionally balance by downsampling to match a reference file\n\
                  \n\
                  Example:\n\
                  dimer_count <(samtools view a.bam) <(samtools view m1.bam) <(samtools view m2.bam) \\\n\
                              --balance 1 --output dimers.csv"
)]
struct Args {
    /// SAM input files (or process substitution pipes)
    #[arg(required = true, num_args = 1.., value_name = "FILE")]
    files: Vec<String>,

    /// Output CSV file
    #[arg(short, long, required = true)]
    output: String,

    /// Balance classes: downsample all other files to match row count of the Nth file (1‑based)
    #[arg(long, value_parser = clap::value_parser!(usize))]
    balance: Option<usize>,

    /// Random seed for reproducible downsampling
    #[arg(short, long, default_value_t = 42)]
    seed: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn count_dimers(seq: &str) -> [usize; 16] {
    let mut counts = [0; 16];
    let bytes = seq.as_bytes();
    if bytes.len() < 2 {
        return counts;
    }
    let base_idx = |b: u8| -> Option<usize> {
        match b {
            b'A' => Some(0),
            b'C' => Some(1),
            b'G' => Some(2),
            b'T' => Some(3),
            _ => None,
        }
    };
    for window in bytes.windows(2) {
        let a = base_idx(window[0]);
        let b = base_idx(window[1]);
        if let (Some(i), Some(j)) = (a, b) {
            counts[i * 4 + j] += 1;
        }
    }
    counts
}

fn valid_sam_sequence(line: &str) -> Option<&str> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 11 {
        return None;
    }
    let seq = fields[9];
    if seq.is_empty() { None } else { Some(seq) }
}

fn sequence_iterator(path: &str) -> Result<impl Iterator<Item = String>> {
    let file = File::open(path).with_context(|| format!("Cannot open file: {}", path))?;
    let reader = BufReader::new(file);
    let iter = reader.lines().filter_map(|r| {
        r.ok()
            .and_then(|line| valid_sam_sequence(&line).map(|s| s.to_uppercase()))
    });
    Ok(iter)
}

fn dimer_row(seq: &str) -> Vec<String> {
    let dimers = count_dimers(seq);
    let mut row = vec![seq.to_string()];
    row.extend(dimers.iter().map(|c| c.to_string()));
    row
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    if args.files.is_empty() {
        bail!("At least one input file is required");
    }

    // Validate --balance
    if let Some(bal) = args.balance {
        if bal < 1 {
            bail!("--balance must be at least 1");
        }
        if bal > args.files.len() {
            bail!(
                "Balance reference index {} is out of range (1..{})",
                bal,
                args.files.len()
            );
        }
    }

    // Prepare output CSV writer
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    let mut header = vec!["sequence".to_string()];
    header.extend(DIMERS.iter().map(|&d| d.to_string()));
    header.push("label".to_string());
    wtr.write_record(&header)?;

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // Convert to 0‑based index
    let balance_idx = args.balance.map(|n| n - 1);

    if let Some(ref_idx) = balance_idx {
        // Balanced mode
        // 1. Process reference file completely, counting K
        let ref_iter = sequence_iterator(&args.files[ref_idx])?;
        let mut k = 0;
        for seq in ref_iter {
            let mut row = dimer_row(&seq);
            row.push(ref_idx.to_string());
            wtr.write_record(&row)?;
            k += 1;
        }
        if k == 0 {
            eprintln!(
                "Reference file {} has no valid reads. Skipping other files",
                ref_idx + 1
            );
        } else {
            // 2. Downsample all other files to k
            for (idx, file) in args.files.iter().enumerate() {
                if idx == ref_idx {
                    continue;
                }
                let iter = sequence_iterator(file)?;
                let sample = reservoir_sample_iter(iter, k, &mut rng);
                for seq in sample {
                    let mut row = dimer_row(&seq);
                    row.push(idx.to_string());
                    wtr.write_record(&row)?;
                }
            }
        }
    } else {
        // No balancing
        for (idx, file) in args.files.iter().enumerate() {
            let iter = sequence_iterator(file)?;
            for seq in iter {
                let mut row = dimer_row(&seq);
                row.push(idx.to_string());
                wtr.write_record(&row)?;
            }
        }
    }

    wtr.flush()?;
    eprintln!("Dimer counts written to {}", args.output);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
