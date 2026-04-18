////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{BufReader, BufWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Parser)]
#[command(author, version, about = "Concatenate ancient and modern quality files", long_about = None)]
struct Args {
    #[arg(short, long)]
    ancient: String,

    #[arg(short, long)]
    modern: String,

    #[arg(short, long)]
    out: String,

    #[arg(short, long, default_value_t = false)]
    balance: bool,

    #[arg(short, long, default_value_t = 42)]
    seed: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn read_quality_file(path: &str) -> Result<(Vec<Vec<String>>, Vec<String>)> {
    let file = File::open(path).context(format!("Cannot open file: {}", path))?;
    let buf = BufReader::new(file);
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_reader(buf);

    let headers = rdr
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if headers.len() < 3 {
        anyhow::bail!("File {} has fewer than 3 columns", path);
    }

    let mut records = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        records.push(fields[2..].to_vec());
    }
    Ok((records, headers))
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn sample_records(records: &[Vec<String>], k: usize, rng: &mut ChaCha8Rng) -> Vec<Vec<String>> {
    if k >= records.len() {
        return records.to_vec();
    }
    let mut indices: Vec<usize> = (0..k).collect();
    for i in k..records.len() {
        let j = rng.gen_range(0..=i);
        if j < k {
            indices[j] = i;
        }
    }
    indices.iter().map(|&idx| records[idx].clone()).collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    let (ancient_records, ancient_header) = read_quality_file(&args.ancient)?;
    let (modern_records, _modern_header) = read_quality_file(&args.modern)?;

    let n_ancient = ancient_records.len();
    let n_modern = modern_records.len();
    eprintln!("Ancient rows: {}, Modern rows: {}", n_ancient, n_modern);

    let n_cols = ancient_header.len().saturating_sub(2);
    if n_cols == 0 {
        anyhow::bail!("No per‑position columns found in ancient file");
    }

    // Use BufWriter for buffering
    let out_file = File::create(&args.out).context("Cannot create output file")?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(buf_out);

    // Write header: per‑position columns + "label"
    let mut header = (1..=n_cols)
        .map(|i| format!("col{}", i))
        .collect::<Vec<_>>();
    header.push("label".to_string());
    wtr.write_record(&header)?;

    let write_records = |records: &[Vec<String>],
                         label: u8,
                         wtr: &mut csv::Writer<BufWriter<File>>|
     -> Result<()> {
        for rec in records {
            let mut row = rec.clone();
            row.push(label.to_string());
            wtr.write_record(&row)?;
        }
        Ok(())
    };

    if args.balance {
        let min_rows = n_ancient.min(n_modern);
        eprintln!("Balancing to {} rows per class", min_rows);

        let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

        let ancient_sample = if n_ancient > min_rows {
            sample_records(&ancient_records, min_rows, &mut rng)
        } else {
            ancient_records
        };
        let modern_sample = if n_modern > min_rows {
            sample_records(&modern_records, min_rows, &mut rng)
        } else {
            modern_records
        };

        write_records(&ancient_sample, 0, &mut wtr)?;
        write_records(&modern_sample, 1, &mut wtr)?;
    } else {
        write_records(&ancient_records, 0, &mut wtr)?;
        write_records(&modern_records, 1, &mut wtr)?;
    }

    wtr.flush()?;
    eprintln!("Concatenated dataset written to {}", args.out);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
