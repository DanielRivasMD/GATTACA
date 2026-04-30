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
    about = "Per‑position base composition from FASTA files",
    long_about = "Read FASTA files and compute per‑position percentages of A, T, G, C, N\n\
                  Outputs a CSV with columns:\n\
                  position, then for each input file (0‑based index) _A, _T, _G, _C, _N\n\
                  \n\
                  Example:\n\
                  position_composition <(samtools fasta a.bam) <(samtools fasta m.bam) \\\n\
                                       --balance 1 --output comp.csv"
)]
struct Args {
    /// FASTA input files (or process substitution pipes)
    #[arg(required = true, num_args = 1.., value_name = "FILE")]
    files: Vec<String>,

    /// Output CSV file
    #[arg(short, long, required = true)]
    output: String,

    /// Balance: downsample all other files to match row count of the Nth file (1‑based)
    #[arg(long, value_parser = clap::value_parser!(usize))]
    balance: Option<usize>,

    /// Random seed for reproducible downsampling
    #[arg(short, long, default_value_t = 42)]
    seed: u64,

    /// Read length: 0 = padding mode (default), >0 = strict length filter
    #[arg(short, long, default_value_t = 0)]
    length: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn pad_sequence(seq: &str, target_len: usize) -> String {
    let seq_len = seq.len();
    if seq_len >= target_len {
        // If equal or longer, truncate (shouldn't happen if target_len is max)
        return seq[..target_len].to_uppercase();
    }
    let left = seq_len / 2;
    let right = seq_len - left;
    let pad_len = target_len - seq_len;
    let mut padded = String::with_capacity(target_len);
    padded.push_str(&seq[..left]);
    for _ in 0..pad_len {
        padded.push('N');
    }
    padded.push_str(&seq[left..]);
    padded.to_uppercase()
}

fn base_index(b: u8) -> usize {
    match b {
        b'A' | b'a' => 0,
        b'T' | b't' => 1,
        b'G' | b'g' => 2,
        b'C' | b'c' => 3,
        _ => 4,
    }
}

fn count_composition<I>(seqs: I, length: usize) -> Vec<[usize; 5]>
where
    I: Iterator<Item = String>,
{
    let mut counts = vec![[0usize; 5]; length];
    for seq in seqs {
        for (pos, &byte) in seq.as_bytes().iter().enumerate() {
            let idx = base_index(byte);
            counts[pos][idx] += 1;
        }
    }
    counts
}

fn counts_to_percentages(counts: &[[usize; 5]], total_reads: usize) -> Vec<[f64; 5]> {
    counts
        .iter()
        .map(|cnt| {
            let sum: usize = cnt.iter().sum();
            if sum == 0 {
                [0.0; 5]
            } else {
                [
                    (cnt[0] as f64 * 100.0) / sum as f64,
                    (cnt[1] as f64 * 100.0) / sum as f64,
                    (cnt[2] as f64 * 100.0) / sum as f64,
                    (cnt[3] as f64 * 100.0) / sum as f64,
                    (cnt[4] as f64 * 100.0) / sum as f64,
                ]
            }
        })
        .collect()
}

fn parse_fasta_to_vec<R: BufRead>(mut reader: R) -> Result<Vec<String>> {
    let mut seqs = Vec::new();
    let mut header = false;
    let mut current_seq = String::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('>') {
            if header {
                // previous sequence finished
                if !current_seq.is_empty() {
                    seqs.push(current_seq.clone());
                    current_seq.clear();
                }
                header = true;
            } else {
                header = true;
            }
        } else if header {
            current_seq.push_str(trimmed);
        }
    }
    if header && !current_seq.is_empty() {
        seqs.push(current_seq);
    }
    Ok(seqs)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

struct FastaSeqIter<R: BufRead> {
    reader: R,
    finished: bool,
    current_seq: String,
    in_record: bool,
}

impl<R: BufRead> FastaSeqIter<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            finished: false,
            current_seq: String::new(),
            in_record: false,
        }
    }
}

impl<R: BufRead> Iterator for FastaSeqIter<R> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => {
                    // EOF
                    self.finished = true;
                    if self.in_record && !self.current_seq.is_empty() {
                        let seq = std::mem::take(&mut self.current_seq);
                        return Some(seq);
                    }
                    return None;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if trimmed.starts_with('>') {
                        if self.in_record && !self.current_seq.is_empty() {
                            let seq = std::mem::take(&mut self.current_seq);
                            // start new record
                            self.in_record = true;
                            return Some(seq);
                        }
                        self.in_record = true;
                        // skip header line itself
                        continue;
                    } else if self.in_record {
                        self.current_seq.push_str(trimmed);
                    }
                    // ignore lines before first header
                }
                Err(e) => {
                    self.finished = true;
                    eprintln!("Error reading FASTA: {}", e);
                    return None;
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<()> {
    let args = Args::parse();

    if args.files.is_empty() {
        bail!("At least one input file is required");
    }
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

    let padding_mode = args.length == 0;
    if !padding_mode && args.length == 0 {
        bail!("Length must be >0 in strict mode (or use 0 for padding)");
    }
    let fixed_len = if padding_mode { 0 } else { args.length };
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // collect per‑file composition
    let mut compositions: Vec<Option<Vec<[f64; 5]>>> = vec![None; args.files.len()];
    let mut output_length = 0usize;

    fn process_file<I>(
        seq_iter: I,
        length: usize,
        sample_size: Option<usize>,
        rng: &mut impl Rng,
    ) -> (Vec<[f64; 5]>, usize)
    where
        I: Iterator<Item = String>,
    {
        let (counts, total) = if let Some(k) = sample_size {
            let sample = reservoir_sample_iter(seq_iter, k, rng);
            let total = sample.len();
            (count_composition(sample.into_iter(), length), total)
        } else {
            let all_seqs: Vec<String> = seq_iter.collect();
            let total = all_seqs.len();
            (count_composition(all_seqs.into_iter(), length), total)
        };
        let percentages = counts_to_percentages(&counts, total);
        (percentages, total)
    }

    if padding_mode {
        // 1. Read first file entirely to get sequences and max_len
        let first_path = &args.files[0];
        let file = File::open(first_path)
            .with_context(|| format!("Cannot open first file: {}", first_path))?;
        let reader = BufReader::new(file);
        let first_seqs = parse_fasta_to_vec(reader)?;
        if first_seqs.is_empty() {
            bail!("First file contains no sequences.");
        }
        let max_len = first_seqs.iter().map(|s| s.len()).max().unwrap();
        output_length = max_len;

        // A closure to pad a sequence to max_len
        let pad = |s: &str| -> String { pad_sequence(s, max_len) };

        // Process first file in memory depending on balance
        if balance_idx == Some(0) {
            // First file is reference. Count all its sequences
            let seq_iter = first_seqs.iter().map(|s| pad(s));
            let (comp, _) = process_file(seq_iter, max_len, None, &mut rng);
            compositions[0] = Some(comp);
        } else if balance_idx.is_some() {
            // Reference is not first file. Sample from first file later using reference K
            // TODO: Leave composition[0] as None for now.
        } else {
            // No balancing: process first file now
            let seq_iter = first_seqs.iter().map(|s| pad(s));
            let (comp, _) = process_file(seq_iter, max_len, None, &mut rng);
            compositions[0] = Some(comp);
        }

        let ref_is_not_first = balance_idx.map_or(false, |idx| idx > 0);
        let mut ref_k: Option<usize> = None;

        if ref_is_not_first {
            let ref_idx = balance_idx.unwrap();
            let ref_path = &args.files[ref_idx];
            let file = File::open(ref_path)
                .with_context(|| format!("Cannot open reference file: {}", ref_path))?;
            let reader = BufReader::new(file);
            let seq_iter = FastaSeqIter::new(reader).map(|s| pad_sequence(&s, max_len));
            let (comp, k) = process_file(seq_iter, max_len, None, &mut rng);
            compositions[ref_idx] = Some(comp);
            ref_k = Some(k);
        }

        if compositions[0].is_none() {
            // First file not yet processed. It is not the reference, and we either have a reference K or no balancing.
            let k_sample = if let Some(k) = ref_k { Some(k) } else { None }; // if no balancing, None
            let seq_iter = first_seqs.iter().map(|s| pad_sequence(s, max_len));
            let (comp, _) = process_file(seq_iter, max_len, k_sample, &mut rng);
            compositions[0] = Some(comp);
        }

        for idx in 1..args.files.len() {
            if compositions[idx].is_some() {
                continue; // reference already done
            }
            let file_path = &args.files[idx];
            let file = File::open(file_path)
                .with_context(|| format!("Cannot open file: {}", file_path))?;
            let reader = BufReader::new(file);
            let seq_iter = FastaSeqIter::new(reader).map(|s| pad_sequence(&s, max_len));
            let k_sample = if ref_k.is_some() { ref_k } else { None }; // if balancing, sample to K; else None
            let (comp, _) = process_file(seq_iter, max_len, k_sample, &mut rng);
            compositions[idx] = Some(comp);
        }
    } else {
        let L = fixed_len;
        output_length = L;

        fn seqs_of_len<R: BufRead>(reader: R, len: usize) -> impl Iterator<Item = String> {
            FastaSeqIter::new(reader)
                .filter_map(move |s| if s.len() == len { Some(s) } else { None })
        }

        if let Some(ref_idx) = balance_idx {
            let ref_path = &args.files[ref_idx];
            let file = File::open(ref_path)
                .with_context(|| format!("Cannot open reference file: {}", ref_path))?;
            let reader = BufReader::new(file);
            let seq_iter = seqs_of_len(reader, L);
            let (comp, ref_k) = process_file(seq_iter, L, None, &mut rng);
            compositions[ref_idx] = Some(comp);
            // Now process all other files with sampling.
            for idx in 0..args.files.len() {
                if idx == ref_idx {
                    continue;
                }
                let file_path = &args.files[idx];
                let file = File::open(file_path)
                    .with_context(|| format!("Cannot open file: {}", file_path))?;
                let reader = BufReader::new(file);
                let seq_iter = seqs_of_len(reader, L);
                let (comp, _) = process_file(seq_iter, L, Some(ref_k), &mut rng);
                compositions[idx] = Some(comp);
            }
        } else {
            // No balancing: process all files sequentially, full count
            for (idx, file_path) in args.files.iter().enumerate() {
                let file = File::open(file_path)
                    .with_context(|| format!("Cannot open file: {}", file_path))?;
                let reader = BufReader::new(file);
                let seq_iter = seqs_of_len(reader, L);
                let (comp, _) = process_file(seq_iter, L, None, &mut rng);
                compositions[idx] = Some(comp);
            }
        }
    }

    // Write CSV output
    let out_file = File::create(&args.output)
        .with_context(|| format!("Cannot create output file: {}", args.output))?;
    let buf_out = BufWriter::new(out_file);
    let mut wtr = csv::Writer::from_writer(buf_out);

    // Build header
    let mut header: Vec<String> = vec!["position".to_string()];
    for i in 0..args.files.len() {
        header.push(format!("{i}_A"));
        header.push(format!("{i}_T"));
        header.push(format!("{i}_G"));
        header.push(format!("{i}_C"));
        header.push(format!("{i}_N"));
    }
    wtr.write_record(&header)?;

    // Write data rows for each position
    for pos in 0..output_length {
        let mut row: Vec<String> = vec![(pos + 1).to_string()];
        for i in 0..args.files.len() {
            if let Some(ref comp) = compositions[i] {
                let perc = &comp[pos];
                row.push(format!("{:.2}", perc[0]));
                row.push(format!("{:.2}", perc[1]));
                row.push(format!("{:.2}", perc[2]));
                row.push(format!("{:.2}", perc[3]));
                row.push(format!("{:.2}", perc[4]));
            } else {
                // Should not happen, but fill with 0
                for _ in 0..5 {
                    row.push("0.00".to_string());
                }
            }
        }
        wtr.write_record(&row)?;
    }

    wtr.flush()?;
    eprintln!("Composition written to {}", args.output);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
