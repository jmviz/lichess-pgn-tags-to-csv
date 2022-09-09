mod csv;
mod tags;

use crate::{csv::pgn2csv, tags::Tags};

use std::{env::current_dir, fs::create_dir_all, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Ok, Result};
use clap::Parser;
use globwalk::{DirEntry, GlobWalkerBuilder};
use indicatif::{HumanDuration, ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

#[derive(Parser)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Directory containing compressed or uncompressed pgn files
    #[clap(short, long = "pgn", value_parser)]
    pgn_dir: Option<PathBuf>,
    /// Output directory for the resulting csv files
    #[clap(short, long = "csv", value_parser)]
    csv_dir: Option<PathBuf>,
    /// Comma-separated list of pgn tags desired as csv columns
    #[clap(short, long, value_parser, value_delimiter = ',')]
    tags: Option<Vec<String>>,
    /// Minify each csv by using short forms for certain pgn tag values
    #[clap(short, long, action)]
    minify: bool,
    /// Compress each csv with bzip2
    #[clap(short = 'z', long = "compress", action)]
    compress: bool,
}

fn main() -> Result<()> {
    let total_time = Instant::now();

    let args = Args::parse();

    let tags = if let Some(arg_tags) = args.tags {
        if let Result::Ok(tags) = Tags::from(&arg_tags) {
            tags
        } else {
            println!("No valid lichess pgn tags provided. See --help. Exiting.");
            return Ok(());
        }
    } else {
        Tags::default()
    };

    let tags = Arc::from(tags);

    let pgn_dir = args.pgn_dir.unwrap_or(current_dir()?);

    let pgns: Vec<DirEntry> = GlobWalkerBuilder::from_patterns(&pgn_dir, &["*.pgn", "*.pgn.bz2"])
        .max_depth(1)
        .build()?
        .into_iter()
        .filter_map(Result::ok)
        .collect();

    let n_pgn = u64::try_from(pgns.len())?;

    if n_pgn == 0 {
        println!(
            "Found no files with extensions .pgn or .pgn.bz2 in {}. Exiting.",
            pgn_dir.display()
        );
        return Ok(());
    }

    let csv_dir = args.csv_dir.unwrap_or_else(|| pgn_dir.clone());
    if !csv_dir.is_dir() {
        create_dir_all(&csv_dir)?;
    }

    println!(
        "Processing {} pgn files in {} to csv files in {}.",
        n_pgn,
        pgn_dir.display(),
        csv_dir.display()
    );

    let pb = ProgressBar::new(n_pgn);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} Elapsed: {elapsed} (ETA: {eta}) [{wide_bar:.cyan/blue}] files: {human_pos}/{human_len}")?
        .progress_chars("#>-"));

    pgns.par_iter()
        .progress_with(pb)
        .map(|pgn: &DirEntry| {
            pgn2csv(
                pgn.path(),
                &csv_dir,
                Arc::clone(&tags),
                args.minify,
                args.compress,
            )
        })
        .collect::<Result<()>>()?;

    let duration = total_time.elapsed();
    println!("All done. Took {}.", HumanDuration(duration));

    Ok(())
}
