////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read};

use gattaca::reservoir_sample_iter;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(author, version, about = "Concatenate ancient & modern quality files")]
struct Args {
    /// Ancient sample TSV file
    #[arg(short, long)]
    ancient: String,

    /// Modern sample TSV file
    #[arg(short, long)]
    modern: String,

    /// Output CSV file
    #[arg(short, long)]
    out: String,

    /// Balance classes by downsampling to the smaller class size
    #[arg(short, long, default_value_t = false)]
    balance: bool,

    /// Random seed for reproducible downsampling
    #[arg(short, long, default_value_t = 42)]
    seed: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Count the number of data rows (excluding header) in a TSV file.
fn count_rows(path: &str) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("Cannot open {}", path))?;
    let reader = BufReader::new(file);
    // Skip header line
    let mut lines = reader.lines();
    lines.next(); // header
    let count = lines.count();
    Ok(count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stream all records from a TSV reader, writing them to the CSV writer with the given label.
/// Returns the number of rows written.
fn stream_records<R: Read>(
    mut rdr: csv::Reader<R>,
    label: u8,
    wtr: &mut csv::Writer<BufWriter<File>>,
) -> Result<usize> {
    let mut count = 0;
    for result in rdr.records() {
        let record = result?;
        let fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        // Skip the first two columns (seq and qual), keep the rest
        if fields.len() < 3 {
            continue;
        }
        let mut row = fields[2..].to_vec();
        row.push(label.to_string());
        wtr.write_record(&row)?;
        count += 1;
    }
    Ok(count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reservoir sample exactly `k` rows from a TSV reader.
fn reservoir_sample_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    k: usize,
    rng: &mut impl Rng,
) -> Result<Vec<Vec<String>>> {
    if k == 0 {
        return Ok(Vec::new());
    }

    // Convert records iterator into a vector of row fields (skipping first two columns).
    let iter = rdr.records().filter_map(|r| {
        r.ok().and_then(|rec| {
            let fields: Vec<String> = rec.iter().map(|s| s.to_string()).collect();
            if fields.len() >= 3 {
                Some(fields[2..].to_vec())
            } else {
                None
            }
        })
    });

    Ok(reservoir_sample_iter(iter, k, rng))
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    // 1. Determine number of per‑position columns from ancient file header
    let ancient_file = File::open(&args.ancient)
        .with_context(|| format!("Cannot open ancient file: {}", args.ancient))?;
    let mut ancient_rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_reader(BufReader::new(ancient_file));

    let headers = ancient_rdr.headers()?.clone();
    if headers.len() < 3 {
        anyhow::bail!("Ancient file has fewer than 3 columns");
    }
    let n_cols = headers.len() - 2;

    // 2. Prepare output CSV writer with generic header
    let out_file = File::create(&args.out).context("Cannot create output file")?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    let mut header: Vec<String> = (1..=n_cols).map(|i| format!("col{}", i)).collect();
    header.push("label".to_string());
    wtr.write_record(&header)?;

    // 3. Process based on balance flag
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    if args.balance {
        // Count rows in both files
        let n_ancient = count_rows(&args.ancient)?;
        let n_modern = count_rows(&args.modern)?;
        let min_rows = n_ancient.min(n_modern);
        eprintln!(
            "Ancient rows: {}, Modern rows: {}, Balancing to {} each",
            n_ancient, n_modern, min_rows
        );

        // Reservoir sample from ancient file
        let ancient_f = File::open(&args.ancient)
            .with_context(|| format!("Cannot open ancient file: {}", args.ancient))?;
        let ancient_rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_reader(BufReader::new(ancient_f));
        let ancient_sample = reservoir_sample_from_reader(ancient_rdr, min_rows, &mut rng)?;

        // Reservoir sample from modern file
        let modern_f = File::open(&args.modern)
            .with_context(|| format!("Cannot open modern file: {}", args.modern))?;
        let modern_rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_reader(BufReader::new(modern_f));
        let modern_sample = reservoir_sample_from_reader(modern_rdr, min_rows, &mut rng)?;

        // Write samples
        for mut row in ancient_sample {
            row.push("0".to_string());
            wtr.write_record(&row)?;
        }
        for mut row in modern_sample {
            row.push("1".to_string());
            wtr.write_record(&row)?;
        }
    } else {
        // Stream all rows without balancing
        // Ancient
        let ancient_f = File::open(&args.ancient)
            .with_context(|| format!("Cannot open ancient file: {}", args.ancient))?;
        let ancient_rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_reader(BufReader::new(ancient_f));
        let count_a = stream_records(ancient_rdr, 0, &mut wtr)?;

        // Modern
        let modern_f = File::open(&args.modern)
            .with_context(|| format!("Cannot open modern file: {}", args.modern))?;
        let modern_rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_reader(BufReader::new(modern_f));
        let count_m = stream_records(modern_rdr, 1, &mut wtr)?;

        eprintln!("Wrote {} ancient and {} modern rows", count_a, count_m);
    }

    wtr.flush()?;
    eprintln!("Concatenated dataset written to {}", args.out);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
