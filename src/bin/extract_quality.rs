// src/bin/extract_quality.rs
////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result, bail};
use clap::Parser;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};

use gattaca::reservoir_sample_iter;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Extract per‑position Phred scores from SAM inputs",
    long_about = "Read SAM files (or pipes from process substitution) for ancient and modern samples,\n\
                  extract per‑position quality scores, concatenate them, and output a CSV with\n\
                  columns: 1..L, label (0=ancient, 1=modern). Optionally balance classes by\n\
                  downsampling the majority class\n\
                  \n\
                  Example:\n\
                  extract_quality --ancient <(samtools view a1.bam) <(samtools view a2.bam)\n\
                                  --modern <(samtools view m1.bam)\n\
                                  --balance ancient --output out.csv"
)]
struct Args {
    /// Ancient SAM input files (or process substitution pipes)
    #[arg(short, long, required = true, num_args = 1.., value_name = "FILE")]
    ancient: Vec<String>,

    /// Modern SAM input files (or process substitution pipes)
    #[arg(short, long, required = true, num_args = 1.., value_name = "FILE")]
    modern: Vec<String>,

    /// Output CSV file
    #[arg(short, long, required = true)]
    output: String,

    /// Balance classes: 'ancient' downsamples modern to ancient size, 'modern' downsamples ancient to modern size
    #[arg(long, value_parser = ["ancient", "modern"])]
    balance: Option<String>,

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

    /// Number of lines to scan for encoding auto‑detection
    #[arg(long, default_value_t = 10000)]
    detect_encoding: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Detect Phred encoding offset from a sample of quality strings
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

/// Parse a SAM line and return Some(quality_bytes) if valid, else None
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

/// Convert a quality byte slice to a vector of per‑position scores as strings (offset applied)
fn scores_to_strings(qual_bytes: &[u8], offset: u8) -> Vec<String> {
    qual_bytes
        .iter()
        .map(|&b| (b.saturating_sub(offset)).to_string())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();
    let l = args.length;

    // 1. Phred offset detection (from first ancient file)
    if args.ancient.is_empty() {
        bail!("At least one --ancient input is required.");
    }
    let first_ancient = &args.ancient[0];
    // Open first ancient file and read detection buffer
    let file = if first_ancient == "-" {
        bail!("Cannot use stdin with multiple inputs; provide files or pipes.");
    } else {
        File::open(first_ancient)
            .with_context(|| format!("Cannot open ancient file: {}", first_ancient))?
    };
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

    // 2. Row counting for ancient and modern
    let count_ancient = count_class_rows(&args.ancient, l)?;
    let count_modern = count_class_rows(&args.modern, l)?;

    eprintln!(
        "Ancient rows: {}, Modern rows: {}",
        count_ancient, count_modern
    );

    // Determine target sizes
    let (target_ancient, target_modern) = match args.balance.as_deref() {
        Some("ancient") => (count_ancient, count_ancient.min(count_modern)),
        Some("modern") => (count_ancient.min(count_modern), count_modern),
        _ => (count_ancient, count_modern),
    };

    if target_ancient == 0 && target_modern == 0 {
        eprintln!("No reads to process. Exiting.");
        return Ok(());
    }

    // 3. Output CSV setup
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    // Header: 1..l, label
    let mut header: Vec<String> = (1..=l).map(|i| i.to_string()).collect();
    header.push("label".to_string());
    wtr.write_record(&header)?;

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // 4. Write ancient class
    if target_ancient > 0 {
        if target_ancient == count_ancient {
            // Stream all ancient rows directly
            stream_class_rows(&args.ancient, l, offset, 0u8, &mut wtr, &detection_buffer)?;
        } else {
            // Downsample ancient to target_ancient
            downsample_class_rows(
                &args.ancient,
                l,
                offset,
                target_ancient,
                &mut rng,
                0u8,
                &mut wtr,
                &detection_buffer,
            )?;
        }
    }

    // 5. Write modern class
    if target_modern > 0 {
        if target_modern == count_modern {
            stream_class_rows(&args.modern, l, offset, 1u8, &mut wtr, &[])?;
        } else {
            downsample_class_rows(
                &args.modern,
                l,
                offset,
                target_modern,
                &mut rng,
                1u8,
                &mut wtr,
                &[],
            )?;
        }
    }

    wtr.flush()?;
    eprintln!("Labelled dataset written to {}", args.output);
    Ok(())
}

/// Count the number of valid reads (matching length) across multiple files
fn count_class_rows(files: &[String], read_len: usize) -> Result<usize> {
    let mut total = 0;
    for file in files {
        if file == "-" {
            bail!("Stdin ('-') not supported as a class file.");
        }
        let f = File::open(file).with_context(|| format!("Cannot open file: {}", file))?;
        let reader = BufReader::new(f);
        for line in reader.lines() {
            let line = line?;
            if valid_sam_line(&line, read_len).is_some() {
                total += 1;
            }
        }
    }
    Ok(total)
}

/// Stream all valid rows from a class, prepending a detection buffer before the first file
fn stream_class_rows(
    files: &[String],
    read_len: usize,
    offset: u8,
    label: u8,
    wtr: &mut csv::Writer<BufWriter<File>>,
    detection_buffer: &[String],
) -> Result<()> {
    let mut first = true;
    for file in files.iter() {
        let reader = if file == "-" {
            bail!("Stdin ('-') not supported.");
        } else {
            BufReader::new(File::open(file).context("Cannot open file")?)
        };
        let lines_iter: Box<dyn Iterator<Item = Result<String, io::Error>>> =
            if first && !detection_buffer.is_empty() {
                first = false;
                let buffer_iter = detection_buffer.iter().cloned().map(Ok::<_, io::Error>);
                let file_iter = reader.lines();
                Box::new(buffer_iter.chain(file_iter))
            } else {
                Box::new(reader.lines())
            };

        for line in lines_iter {
            let line = line?;
            if let Some(qual_bytes) = valid_sam_line(&line, read_len) {
                let row = scores_to_strings(qual_bytes, offset);
                let mut record = row;
                record.push(label.to_string());
                wtr.write_record(&record)?;
            }
        }
    }
    Ok(())
}

/// Downsample a class to exactly `k` rows using reservoir sampling, then write
fn downsample_class_rows(
    files: &[String],
    read_len: usize,
    offset: u8,
    k: usize,
    rng: &mut impl Rng,
    label: u8,
    wtr: &mut csv::Writer<BufWriter<File>>,
    detection_buffer: &[String],
) -> Result<()> {
    let mut iter: Box<dyn Iterator<Item = Vec<String>>> = Box::new(std::iter::empty());
    // Build a combined iterator over files, prepending detection buffer before the first ancient file (if applicable)
    let mut first = true;
    for file in files.iter() {
        let reader = BufReader::new(File::open(file).context("Cannot open file")?);
        let lines_iter: Box<dyn Iterator<Item = Result<String, io::Error>>> =
            if first && !detection_buffer.is_empty() {
                first = false;
                let buffer_iter = detection_buffer.iter().cloned().map(Ok::<_, io::Error>);
                let file_iter = reader.lines();
                Box::new(buffer_iter.chain(file_iter))
            } else {
                Box::new(reader.lines())
            };
        // Map valid lines to score vectors
        let score_iter = lines_iter.filter_map(move |r| {
            r.ok().and_then(|line| {
                valid_sam_line(&line, read_len).map(|b| scores_to_strings(b, offset))
            })
        });
        iter = Box::new(iter.chain(score_iter));
    }

    let sample = reservoir_sample_iter(iter, k, rng);
    for mut row in sample {
        row.push(label.to_string());
        wtr.write_record(&row)?;
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
