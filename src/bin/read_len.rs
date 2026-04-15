use anyhow::{Context, Result};
use clap::Parser;
use needletail::parse_fastx_file;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about = "Compute read length distribution from a FASTA file", long_about = None)]
struct Args {
    /// Input FASTA file (can be .gz compressed)
    fasta: PathBuf,

    /// Output CSV file for distribution (columns: length,count)
    #[arg(long)]
    csv: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut reader = parse_fastx_file(&args.fasta)
        .with_context(|| format!("Failed to open FASTA file: {:?}", args.fasta))?;

    let mut length_counts: BTreeMap<usize, usize> = BTreeMap::new();

    while let Some(record) = reader.next() {
        let seqrec = record?;
        let len = seqrec.seq().len();
        *length_counts.entry(len).or_insert(0) += 1;
    }

    println!("Read length distribution for {}:", args.fasta.display());
    for (len, count) in &length_counts {
        println!("Length {}: {} reads", len, count);
    }

    if let Some(csv_path) = args.csv {
        let file = File::create(&csv_path)
            .with_context(|| format!("Cannot create CSV file: {:?}", csv_path))?;
        let mut wtr = csv::Writer::from_writer(BufWriter::new(file));
        wtr.write_record(&["length", "count"])?;
        for (len, count) in length_counts {
            wtr.serialize((len, count))?;
        }
        wtr.flush()?;
        println!("Distribution saved to {}", csv_path.display());
    }

    Ok(())
}
