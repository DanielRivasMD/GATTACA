////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result, bail};
use clap::Parser;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};

use gattaca::reservoir_sample_iter;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Extract per‑position Phred scores from SAM inputs",
    long_about = "Read SAM files and extract per‑position quality scores\n\
                  Outputs a CSV with columns 1..L and a label (the 0‑based file index)\n\
                  Optionally balance by downsampling all other files to match a reference file\n\
                  \n\
                  Example:\n\
                  extract_quality <(samtools view a.bam) <(samtools view m1.bam) <(samtools view m2.bam) \\\n\
                                  --length 76 --balance 1 --output out.csv"
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

    /// Read length to filter (reads of other lengths are discarded)
    #[arg(short, long, default_value_t = 76)]
    length: usize,

    /// Force Phred encoding (33 or 64). Overrides auto‑detection
    #[arg(long, value_parser = |s: &str| -> Result<u8, String> {
        match s {
            "33" => Ok(33),
            "64" => Ok(64),
            _ => Err(format!("Invalid phred offset '{}', must be 33 or 64", s)),
        }
    })]
    phred: Option<u8>,

    /// Number of lines to scan for encoding auto‑detection (from the first file)
    #[arg(long, default_value_t = 10000)]
    detect_encoding: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

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

fn valid_sam_line(line: &str, read_len: usize) -> Option<&[u8]> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 11 {
        return None;
    }
    let seq = fields[9];
    let qual = fields[10];
    if seq.len() == read_len && qual.len() == read_len {
        Some(qual.as_bytes())
    } else {
        None
    }
}

fn scores_to_strings(qual_bytes: &[u8], offset: u8) -> Vec<String> {
    qual_bytes
        .iter()
        .map(|&b| (b.saturating_sub(offset)).to_string())
        .collect()
}

fn score_iterator(
    path: &str,
    read_len: usize,
    offset: u8,
    prefix_lines: Option<Vec<String>>,
) -> Result<impl Iterator<Item = Vec<String>>> {
    let file = File::open(path).with_context(|| format!("Cannot open file: {}", path))?;
    let file_lines = BufReader::new(file).lines();

    let all_lines: Box<dyn Iterator<Item = Result<String, std::io::Error>>> =
        if let Some(prefix) = prefix_lines {
            let prefix_iter = prefix.into_iter().map(|s| Ok(s));
            Box::new(prefix_iter.chain(file_lines))
        } else {
            Box::new(file_lines)
        };

    let iter = all_lines.filter_map(move |r| {
        r.ok()
            .and_then(|line| valid_sam_line(&line, read_len).map(|b| scores_to_strings(b, offset)))
    });
    Ok(iter)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();
    let l = args.length;

    if args.files.is_empty() {
        bail!("At least one input file is required");
    }

    // Validate --balance if given
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

    // Phred offset detection
    let first_file = &args.files[0];
    let file = File::open(first_file)
        .with_context(|| format!("Cannot open first file for detection: {}", first_file))?;
    let mut reader = BufReader::new(file);
    let mut detection_buffer: Vec<String> = Vec::new();
    let mut qual_samples: Vec<String> = Vec::new();
    for _ in 0..args.detect_encoding {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        detection_buffer.push(line);
    }
    for line in &detection_buffer {
        if let Some(qual_bytes) = valid_sam_line(line, l) {
            qual_samples.push(std::str::from_utf8(qual_bytes).unwrap().to_string());
        }
    }

    let offset = if let Some(forced) = args.phred {
        forced
    } else {
        match detect_encoding(&qual_samples.iter().map(|s| s.as_str()).collect::<Vec<_>>()) {
            Some(off) => off,
            None => {
                eprintln!(
                    "Warning: Could not auto‑detect Phred encoding. Outputting raw ASCII values (offset 0)."
                );
                0
            }
        }
    };

    // Output CSV setup
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    let mut header: Vec<String> = (1..=l).map(|i| i.to_string()).collect();
    header.push("label".to_string());
    wtr.write_record(&header)?;

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // Map --balance to 0‑based index
    let balance_idx = args.balance.map(|n| n - 1);

    // Helper: return prefix lines only for the first file (idx == 0)
    let prefix_for = |idx: usize| -> Option<Vec<String>> {
        if idx == 0 {
            Some(detection_buffer.clone())
        } else {
            None
        }
    };

    if let Some(ref_idx) = balance_idx {
        // Balanced mode
        // 1. Process reference file completely, counting K
        let ref_prefix = prefix_for(ref_idx);
        let ref_iter = score_iterator(&args.files[ref_idx], l, offset, ref_prefix)?;
        let mut k = 0;
        for row in ref_iter {
            let mut rec = row;
            rec.push(ref_idx.to_string());
            wtr.write_record(&rec)?;
            k += 1;
        }
        if k == 0 {
            eprintln!(
                "Reference file {} has no valid reads. Skipping other files.",
                ref_idx + 1
            );
        } else {
            // 2. For every other file, downsample to k
            for (idx, file) in args.files.iter().enumerate() {
                if idx == ref_idx {
                    continue;
                }
                let prefix = prefix_for(idx);
                let iter = score_iterator(file, l, offset, prefix)?;
                let sample = reservoir_sample_iter(iter, k, &mut rng);
                for mut row in sample {
                    row.push(idx.to_string());
                    wtr.write_record(&row)?;
                }
            }
        }
    } else {
        // No balancing
        for (idx, file) in args.files.iter().enumerate() {
            let prefix = prefix_for(idx);
            let iter = score_iterator(file, l, offset, prefix)?;
            for row in iter {
                let mut rec = row;
                rec.push(idx.to_string());
                wtr.write_record(&rec)?;
            }
        }
    }

    wtr.flush()?;
    eprintln!("Quality scores written to {}", args.output);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
