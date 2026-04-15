use anyhow::{Context, Result};
use clap::Parser;
use regex::Regex;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

#[derive(Parser)]
#[command(author, version, about = "Convert BAM to FASTA using samtools + seqtk", long_about = None)]
struct Args {
    #[arg(long, default_value = "whole", value_parser = ["whole", "per-chrom"])]
    mode: String,
}

const BAM_DIR: &str = "data/bam";
const FASTA_DIR: &str = "data/fasta";
const SAMPLES: [&str; 2] = ["simulation_ancient", "simulation_modern"];

fn is_canonical_chrom(chrom: &str) -> bool {
    let re = Regex::new(r"^chr([0-9]+|X|Y|M)$").unwrap();
    re.is_match(chrom)
}

/// Run a pipeline of commands, writing final stdout to a file.
fn run_pipeline(commands: Vec<(&str, Vec<&str>)>, output_path: &Path) -> Result<()> {
    let mut children = Vec::new();
    let mut prev_stdout = None;

    for (i, (cmd, args)) in commands.iter().enumerate() {
        let mut child = Command::new(cmd)
            .args(args)
            .stdin(if i == 0 { Stdio::null() } else { Stdio::from(prev_stdout.take().unwrap()) })
            .stdout(if i == commands.len() - 1 { Stdio::piped() } else { Stdio::piped() })
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("Failed to spawn {}", cmd))?;

        if i < commands.len() - 1 {
            prev_stdout = child.stdout.take();
        } else {
            let mut out_file = fs::File::create(output_path)
                .with_context(|| format!("Cannot create {}", output_path.display()))?;
            let stdout = child.stdout.take().context("No stdout from last command")?;
            let mut reader = stdout;
            std::io::copy(&mut reader, &mut out_file)
                .context("Failed to write output to file")?;
        }
        children.push(child);
    }

    for mut child in children {
        let status = child.wait()?;
        if !status.success() {
            anyhow::bail!("Command failed with exit code: {:?}", status.code());
        }
    }
    Ok(())
}

fn convert_whole_bam(bam_path: &Path, output_fasta: &Path) -> Result<()> {
    println!("Converting whole genome for {:?}", bam_path.file_name().unwrap());
    let commands = vec![
        ("samtools", vec!["fastq", bam_path.to_str().unwrap()]),
        ("seqtk", vec!["seq", "-A"]),
    ];
    run_pipeline(commands, output_fasta)
}

fn convert_per_chrom(bam_path: &Path, sample_name: &str) -> Result<()> {
    println!("Converting per chromosome for {}", sample_name);
    let index_path = bam_path.with_extension("bam.bai");
    if !index_path.exists() {
        println!("  Indexing BAM...");
        let status = Command::new("samtools")
            .arg("index")
            .arg(bam_path)
            .status()
            .context("Failed to run samtools index")?;
        if !status.success() {
            anyhow::bail!("samtools index failed");
        }
    }

    let idxstats_output = Command::new("samtools")
        .args(["idxstats", bam_path.to_str().unwrap()])
        .output()
        .context("Failed to run samtools idxstats")?;
    if !idxstats_output.status.success() {
        anyhow::bail!("samtools idxstats failed");
    }
    let stdout = String::from_utf8(idxstats_output.stdout)?;
    let chroms: Vec<&str> = stdout
        .lines()
        .filter_map(|line| line.split('\t').next())
        .filter(|&c| is_canonical_chrom(c))
        .collect();

    for chrom in chroms {
        println!("  Extracting {}...", chrom);
        let output_fasta = Path::new(FASTA_DIR).join(format!("{}_{}.fasta", sample_name, chrom));
        let commands = vec![
            ("samtools", vec!["view", "-b", bam_path.to_str().unwrap(), chrom]),
            ("samtools", vec!["fastq", "-"]),
            ("seqtk", vec!["seq", "-A"]),
        ];
        run_pipeline(commands, &output_fasta)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    fs::create_dir_all(FASTA_DIR).context("Failed to create fasta directory")?;

    for tool in ["samtools", "seqtk"] {
        if let Err(e) = Command::new(tool).arg("--version").stdout(Stdio::null()).stderr(Stdio::null()).status() {
            eprintln!("Error: {} not found in PATH: {}", tool, e);
            std::process::exit(1);
        }
    }

    for sample in SAMPLES {
        let bam_path = Path::new(BAM_DIR).join(format!("{}.bam", sample));
        if !bam_path.exists() {
            eprintln!("Warning: BAM file not found: {}", bam_path.display());
            continue;
        }
        match args.mode.as_str() {
            "whole" => {
                let output_fasta = Path::new(FASTA_DIR).join(format!("{}.fasta", sample));
                convert_whole_bam(&bam_path, &output_fasta)?;
            }
            "per-chrom" => {
                convert_per_chrom(&bam_path, sample)?;
            }
            _ => unreachable!(),
        }
    }

    println!("Conversion complete: FASTA files written to {}", FASTA_DIR);
    Ok(())
}
