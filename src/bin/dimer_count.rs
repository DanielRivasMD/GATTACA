////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
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

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Count dimers in DNA sequences from SAM input files",
    long_about = "Read SAM files (or pipes from process substitution) for ancient and modern samples,\n\
                  extract the DNA sequence, count all 16 possible dimers, and output a CSV with\n\
                  columns: sequence, AA, AC, ..., TT, label (0=ancient, 1=modern)\n\
                  Optionally balance classes by downsampling the majority class\n\
                  \n\
                  Example:\n\
                  dimer_count --ancient <(samtools view a1.bam) <(samtools view a2.bam)\n\
                              --modern <(samtools view modern.bam)\n\
                              --balance ancient --output dimers.csv"
)]
struct Args {
    #[arg(short, long, required = true, num_args = 1.., value_name = "FILE")]
    ancient: Vec<String>,

    #[arg(short, long, required = true, num_args = 1.., value_name = "FILE")]
    modern: Vec<String>,

    #[arg(short, long, required = true)]
    output: String,

    #[arg(long, value_parser = ["ancient", "modern"])]
    balance: Option<String>,

    #[arg(short, long, default_value_t = 42)]
    seed: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn valid_sam_sequence(line: &str) -> Option<&str> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 11 {
        return None;
    }
    let seq = fields[9];
    if seq.is_empty() { None } else { Some(seq) }
}

/// Create a sequence iterator over multiple files (or pipes).
fn seq_iterator(
    files: &[String],
) -> Result<Box<dyn Iterator<Item = Result<String, std::io::Error>>>> {
    let mut iter: Box<dyn Iterator<Item = Result<String, std::io::Error>>> =
        Box::new(std::iter::empty());
    for file in files {
        let f = File::open(file).with_context(|| format!("Cannot open file: {}", file))?;
        let reader = BufReader::new(f);
        let lines = reader.lines().filter_map(|r| match r {
            Ok(line) => valid_sam_sequence(&line).map(|s| Ok(s.to_uppercase())),
            Err(e) => Some(Err(e)),
        });
        iter = Box::new(iter.chain(lines));
    }
    Ok(iter)
}

/// Stream all sequences from an iterator, write dimers with label, return count.
fn stream_and_write(
    mut seq_iter: impl Iterator<Item = Result<String, std::io::Error>>,
    label: u8,
    wtr: &mut csv::Writer<BufWriter<File>>,
) -> Result<usize> {
    let mut count = 0;
    for seq_res in &mut seq_iter {
        let seq = seq_res?;
        let dimers = count_dimers(&seq);
        let mut row: Vec<String> = vec![seq];
        row.extend(dimers.iter().map(|c| c.to_string()));
        row.push(label.to_string());
        wtr.write_record(&row)?;
        count += 1;
    }
    Ok(count)
}

/// Reservoir sample exactly k sequences from an iterator and write them.
fn downsample_and_write(
    seq_iter: impl Iterator<Item = Result<String, std::io::Error>>,
    k: usize,
    rng: &mut impl Rng,
    label: u8,
    wtr: &mut csv::Writer<BufWriter<File>>,
) -> Result<()> {
    let valid_seqs = seq_iter.filter_map(|r| r.ok()); // ignore read errors in sampling (could log)
    let sample = reservoir_sample_iter(valid_seqs, k, rng);
    for seq in sample {
        let dimers = count_dimers(&seq);
        let mut row: Vec<String> = vec![seq];
        row.extend(dimers.iter().map(|c| c.to_string()));
        row.push(label.to_string());
        wtr.write_record(&row)?;
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    // Prepare output CSV writer
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    // Write header: sequence, AA, AC, ..., TT, label
    let mut header = vec!["sequence".to_string()];
    header.extend(DIMERS.iter().map(|&d| d.to_string()));
    header.push("label".to_string());
    wtr.write_record(&header)?;

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    match args.balance.as_deref() {
        None => {
            // No balancing – just stream both classes
            let ancient_count = stream_and_write(seq_iterator(&args.ancient)?, 0, &mut wtr)?;
            let modern_count = stream_and_write(seq_iterator(&args.modern)?, 1, &mut wtr)?;
            eprintln!(
                "Wrote {} ancient and {} modern rows",
                ancient_count, modern_count
            );
        }

        Some("ancient") => {
            // Stream ancient completely, then downsample modern to match
            let ancient_count = stream_and_write(seq_iterator(&args.ancient)?, 0, &mut wtr)?;
            eprintln!("Ancient rows: {}", ancient_count);
            if ancient_count > 0 {
                downsample_and_write(
                    seq_iterator(&args.modern)?,
                    ancient_count,
                    &mut rng,
                    1,
                    &mut wtr,
                )?;
                eprintln!("Downsampled modern to {} rows", ancient_count);
            } else {
                eprintln!("No ancient reads; skipping modern class.");
            }
        }

        Some("modern") => {
            // Stream modern completely, then downsample ancient to match
            let modern_count = stream_and_write(seq_iterator(&args.modern)?, 1, &mut wtr)?;
            eprintln!("Modern rows: {}", modern_count);
            if modern_count > 0 {
                downsample_and_write(
                    seq_iterator(&args.ancient)?,
                    modern_count,
                    &mut rng,
                    0,
                    &mut wtr,
                )?;
                eprintln!("Downsampled ancient to {} rows", modern_count);
            } else {
                eprintln!("No modern reads; skipping ancient class.");
            }
        }

        _ => unreachable!(),
    }

    wtr.flush()?;
    eprintln!("Dimer counts written to {}", args.output);
    Ok(())
}
////////////////////////////////////////////////////////////////////////////////////////////////////
