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
                  Padding mode (‑‑length 0): the longest read in the FIRST file defines L;\n\
                  shorter reads are centred and padded with -1 quality values\n\
                  \n\
                  Example:\n\
                  extract_quality <(samtools view a.bam) <(samtools view m1.bam) \\\n\
                                  --length 0 --balance 1 --output out.csv"
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

    /// Read length to filter, or 0 for padding mode (centred, -1 padded)
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

fn valid_sam_line_strict(line: &str, read_len: usize) -> Option<&[u8]> {
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

fn valid_sam_line_padding(line: &str) -> Option<(usize, &[u8])> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 11 {
        return None;
    }
    let seq = fields[9];
    let qual = fields[10];
    if seq.is_empty() || qual.len() != seq.len() {
        return None;
    }
    Some((seq.len(), qual.as_bytes()))
}

fn score_str(byte: u8, offset: u8) -> String {
    (byte.saturating_sub(offset)).to_string()
}

fn pad_row(qual: &[u8], seq_len: usize, max_len: usize, offset: u8) -> Vec<String> {
    let left = seq_len / 2;
    let right = seq_len - left;
    let pad_count = max_len - seq_len;

    let mut row = Vec::with_capacity(max_len);
    // left real scores
    for &b in &qual[..left] {
        row.push(score_str(b, offset));
    }
    // padding
    for _ in 0..pad_count {
        row.push("-1".to_string());
    }
    // right real scores
    for &b in &qual[left..] {
        row.push(score_str(b, offset));
    }
    row
}

fn write_row(
    wtr: &mut csv::Writer<BufWriter<File>>,
    mut row: Vec<String>,
    label: usize,
) -> Result<()> {
    row.push(label.to_string());
    wtr.write_record(&row).map_err(anyhow::Error::from)
}

fn strict_score_iter(
    path: &str,
    read_len: usize,
    offset: u8,
    prefix: Option<Vec<String>>,
) -> Result<impl Iterator<Item = Vec<String>>> {
    let file = File::open(path).with_context(|| format!("Cannot open: {}", path))?;
    let file_lines = BufReader::new(file).lines();
    let all_lines: Box<dyn Iterator<Item = Result<String, std::io::Error>>> =
        if let Some(pref) = prefix {
            Box::new(pref.into_iter().map(|s| Ok(s)).chain(file_lines))
        } else {
            Box::new(file_lines)
        };
    Ok(all_lines.filter_map(move |r| {
        r.ok().and_then(|line| {
            valid_sam_line_strict(&line, read_len)
                .map(|qual| qual.iter().map(|&b| score_str(b, offset)).collect())
        })
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    if args.files.is_empty() {
        bail!("At least one input file is required");
    }

    // Validate --balance
    if let Some(bal) = args.balance {
        if bal < 1 || bal > args.files.len() {
            bail!(
                "Balance reference index {} is out of range (1..{})",
                bal,
                args.files.len()
            );
        }
    }
    let balance_idx = args.balance.map(|n| n - 1);

    // Open first file for Phred detection
    let first_path = &args.files[0];
    let file = File::open(first_path)
        .with_context(|| format!("Cannot open first file: {}", first_path))?;
    let mut reader = BufReader::new(file);

    // Read detection lines
    let mut detection_buffer: Vec<String> = Vec::new();
    let mut qual_samples: Vec<String> = Vec::new();
    for _ in 0..args.detect_encoding {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        detection_buffer.push(line);
    }
    if args.length > 0 {
        // Strict length: collect quality samples for detection
        for line in &detection_buffer {
            if let Some(qual_bytes) = valid_sam_line_strict(line, args.length) {
                qual_samples.push(std::str::from_utf8(qual_bytes).unwrap().to_string());
            }
        }
    } else {
        // Padding mode: no length filtering for detection
        for line in &detection_buffer {
            if let Some((_len, qual_bytes)) = valid_sam_line_padding(line) {
                qual_samples.push(std::str::from_utf8(qual_bytes).unwrap().to_string());
            }
        }
    }
    let offset = if let Some(forced) = args.phred {
        forced
    } else {
        match detect_encoding(&qual_samples.iter().map(|s| s.as_str()).collect::<Vec<_>>()) {
            Some(off) => off,
            None => {
                eprintln!(
                    "Warning: Could not auto‑detect Phred encoding. Outputting raw ASCII values (offset 0)"
                );
                0
            }
        }
    };

    // Determine output mode
    let padding_mode = args.length == 0;
    let effective_max_len = if padding_mode { 0 } else { args.length };

    // Prepare output CSV writer
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    if padding_mode {
        let mut first_rows: Vec<(usize, Vec<u8>)> = Vec::new(); // (seq_len, qual_bytes)
        for line_result in detection_buffer
            .into_iter()
            .map(|s| Ok::<_, std::io::Error>(s))
            .chain(reader.lines())
        {
            let line = line_result?;
            if let Some((seq_len, qual)) = valid_sam_line_padding(&line) {
                first_rows.push((seq_len, qual.to_vec()));
            }
        }
        if first_rows.is_empty() {
            bail!("First file contains no valid reads");
        }
        let max_len = first_rows.iter().map(|&(len, _)| len).max().unwrap();

        // Write CSV header: 1..max_len, label
        let mut header: Vec<String> = (1..=max_len).map(|i| i.to_string()).collect();
        header.push("label".to_string());
        wtr.write_record(&header).map_err(anyhow::Error::from)?;

        match balance_idx {
            None => {
                // No balancing
                for (seq_len, qual) in &first_rows {
                    write_row(&mut wtr, pad_row(qual, *seq_len, max_len, offset), 0)?;
                }
                for (idx, file) in args.files.iter().enumerate().skip(1) {
                    let f =
                        File::open(file).with_context(|| format!("Cannot open file: {}", file))?;
                    let reader = BufReader::new(f);
                    for line in reader.lines() {
                        let line = line?;
                        if let Some((seq_len, qual)) = valid_sam_line_padding(&line) {
                            write_row(&mut wtr, pad_row(qual, seq_len, max_len, offset), idx)?;
                        }
                    }
                }
            }
            Some(ref_idx) => {
                if ref_idx == 0 {
                    let k = first_rows.len();
                    for (seq_len, qual) in &first_rows {
                        write_row(&mut wtr, pad_row(qual, *seq_len, max_len, offset), 0)?;
                    }
                    for (idx, file) in args.files.iter().enumerate().skip(1) {
                        let f = File::open(file).context("Cannot open file")?;
                        let reader = BufReader::new(f);
                        let iter = reader.lines().filter_map(|r| {
                            r.ok().and_then(|line| {
                                valid_sam_line_padding(&line)
                                    .map(|(seq_len, qual)| pad_row(&qual, seq_len, max_len, offset))
                            })
                        });
                        let sample = reservoir_sample_iter(iter, k, &mut rng);
                        for row in sample {
                            write_row(&mut wtr, row, idx)?;
                        }
                    }
                } else {
                    // 1. Process reference file completely, write rows, count k
                    let ref_file = &args.files[ref_idx];
                    let f = File::open(ref_file).context("Cannot open reference file")?;
                    let reader = BufReader::new(f);
                    let mut k = 0usize;
                    for line in reader.lines() {
                        let line = line?;
                        if let Some((seq_len, qual)) = valid_sam_line_padding(&line) {
                            write_row(&mut wtr, pad_row(&qual, seq_len, max_len, offset), ref_idx)?;
                            k += 1;
                        }
                    }
                    if k == 0 {
                        eprintln!(
                            "Reference file {} has no valid reads. Skipping other files.",
                            ref_idx + 1
                        );
                    } else {
                        // 2. Downsample first file (in memory)
                        let first_iter = first_rows
                            .iter()
                            .map(|(seq_len, qual)| pad_row(qual, *seq_len, max_len, offset));
                        let sample = reservoir_sample_iter(first_iter, k, &mut rng);
                        for row in sample {
                            write_row(&mut wtr, row, 0)?;
                        }
                        // 3. Downsample other files (excluding ref_idx and 0)
                        for idx in 0..args.files.len() {
                            if idx == 0 || idx == ref_idx {
                                continue;
                            }
                            let file = &args.files[idx];
                            let f = File::open(file).context("Cannot open file")?;
                            let reader = BufReader::new(f);
                            let iter = reader.lines().filter_map(|r| {
                                r.ok().and_then(|line| {
                                    valid_sam_line_padding(&line).map(|(seq_len, qual)| {
                                        pad_row(&qual, seq_len, max_len, offset)
                                    })
                                })
                            });
                            let sample = reservoir_sample_iter(iter, k, &mut rng);
                            for row in sample {
                                write_row(&mut wtr, row, idx)?;
                            }
                        }
                    }
                }
            }
        }
    } else {
        let l = args.length;
        let detection_prefix_detectionbuf = detection_buffer.clone();

        let prefix_for = |idx: usize| -> Option<Vec<String>> {
            if idx == 0 {
                Some(detection_prefix_detectionbuf.clone())
            } else {
                None
            }
        };

        // Write CSV header
        let mut header: Vec<String> = (1..=l).map(|i| i.to_string()).collect();
        header.push("label".to_string());
        wtr.write_record(&header).map_err(anyhow::Error::from)?;

        match balance_idx {
            None => {
                for (idx, file) in args.files.iter().enumerate() {
                    let prefix = prefix_for(idx);
                    let iter = strict_score_iter(file, l, offset, prefix)?;
                    for row in iter {
                        write_row(&mut wtr, row, idx)?;
                    }
                }
            }
            Some(ref_idx) => {
                // Process reference file completely, counting k
                let ref_prefix = prefix_for(ref_idx);
                let ref_iter = strict_score_iter(&args.files[ref_idx], l, offset, ref_prefix)?;
                let mut k = 0;
                for row in ref_iter {
                    write_row(&mut wtr, row, ref_idx)?;
                    k += 1;
                }
                if k == 0 {
                    eprintln!(
                        "Reference file {} has no valid reads. Skipping others.",
                        ref_idx + 1
                    );
                } else {
                    for (idx, file) in args.files.iter().enumerate() {
                        if idx == ref_idx {
                            continue;
                        }
                        let prefix = prefix_for(idx);
                        let iter = strict_score_iter(file, l, offset, prefix)?;
                        let sample = reservoir_sample_iter(iter, k, &mut rng);
                        for row in sample {
                            write_row(&mut wtr, row, idx)?;
                        }
                    }
                }
            }
        }
    }

    wtr.flush().map_err(anyhow::Error::from)?;
    eprintln!("Quality scores written to {}", args.output);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
