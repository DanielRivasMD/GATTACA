use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// All 16 possible dimers in a fixed order
const DIMERS: [&str; 16] = [
    "AA", "AC", "AG", "AT",
    "CA", "CC", "CG", "CT",
    "GA", "GC", "GG", "GT",
    "TA", "TC", "TG", "TT",
];

#[derive(Debug, Serialize)]
struct DimerRow {
    sequence: String,
    AA: usize,
    AC: usize,
    AG: usize,
    AT: usize,
    CA: usize,
    CC: usize,
    CG: usize,
    CT: usize,
    GA: usize,
    GC: usize,
    GG: usize,
    GT: usize,
    TA: usize,
    TC: usize,
    TG: usize,
    TT: usize,
}

#[derive(Parser)]
#[command(author, version, about = "Count dimers in DNA sequences from a TSV file (first column = sequence)", long_about = None)]
struct Args {
    /// Input TSV file (sequence in column 1)
    input: PathBuf,

    /// Output CSV file (with dimer counts)
    output: PathBuf,
}

fn count_dimers(seq: &str) -> [usize; 16] {
    let mut counts = [0; 16];
    let bytes = seq.as_bytes();
    if bytes.len() < 2 {
        return counts;
    }
    // Build a quick lookup for dimer index from two characters
    // Map each base to a 2-bit index: A=0, C=1, G=2, T=3
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
            let idx = i * 4 + j;
            counts[idx] += 1;
        }
    }
    counts
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Open input TSV
    let file = std::fs::File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {:?}", args.input))?;
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)   // assume no header, first column is sequence
        .from_reader(file);

    // Prepare output CSV writer
    let out_file = std::fs::File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {:?}", args.output))?;
    let mut wtr = WriterBuilder::new().from_writer(out_file);

    // Write CSV header
    let header = vec!["sequence".to_string()]
        .into_iter()
        .chain(DIMERS.iter().map(|&d| d.to_string()))
        .collect::<Vec<_>>();
    wtr.write_record(&header)?;

    // Process each line
    for result in rdr.records() {
        let record = result?;
        if record.len() < 1 {
            continue; // skip empty lines
        }
        let seq = record[0].to_uppercase();
        let counts = count_dimers(&seq);
        // Build row
        let mut row = DimerRow {
            sequence: seq,
            AA: 0, AC: 0, AG: 0, AT: 0,
            CA: 0, CC: 0, CG: 0, CT: 0,
            GA: 0, GC: 0, GG: 0, GT: 0,
            TA: 0, TC: 0, TG: 0, TT: 0,
        };
        // Assign counts in order
        for (i, &dimer) in DIMERS.iter().enumerate() {
            let field = match dimer {
                "AA" => &mut row.AA, "AC" => &mut row.AC, "AG" => &mut row.AG, "AT" => &mut row.AT,
                "CA" => &mut row.CA, "CC" => &mut row.CC, "CG" => &mut row.CG, "CT" => &mut row.CT,
                "GA" => &mut row.GA, "GC" => &mut row.GC, "GG" => &mut row.GG, "GT" => &mut row.GT,
                "TA" => &mut row.TA, "TC" => &mut row.TC, "TG" => &mut row.TG, "TT" => &mut row.TT,
                _ => unreachable!(),
            };
            *field = counts[i];
        }
        wtr.serialize(&row)?;
    }

    wtr.flush()?;
    println!("Dimer counts written to {}", args.output.display());
    Ok(())
}
