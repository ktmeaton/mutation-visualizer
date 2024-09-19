use clap::Parser;
use color_eyre::eyre::{eyre, Report, Result};
use datafusion::prelude::*;
use datafusion::common::arrow::record_batch::RecordBatch;
use log;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::ffi::OsStr;

/// ---------------------------------------------------------------------------
/// RunArgs
/// ---------------------------------------------------------------------------

/// Detect recombination in a dataset population and/or input alignment.
#[derive(Clone, Debug, Deserialize, Serialize, Parser)]
pub struct AnnotateArgs {

    /// Input annotations tsv.
    #[clap(help = "Input annotations tsv file.")]
    #[clap(long)]
    #[clap(required = true)]
    #[clap(requires = "variants")]
    pub annotations: PathBuf,    

    /// Input nextclade tsv (mutually exclusive with --ivar).
    #[clap(help = "Input nextclade tsv (mutually exclusive with --ivar)")]
    #[clap(long)]
    #[clap(group("variants"))]
    pub nextclade: Option<PathBuf>,

    /// Input ivar tsv (mutually exclusive with --nextclade).
    #[clap(help = "Input ivar tsv (mutually exclusive with ---nextclade)")]
    #[clap(long)]
    #[clap(group("variants"))]
    pub ivar: Option<PathBuf>,
}

/// Run annotation
pub async fn annotate(args: &AnnotateArgs) -> Result<(), Report> {
    log::info!("Annotate.");

    // Start a new datafusion session
    let ctx = SessionContext::new();

    log::debug!("Parsing annotations file path: {:?}", &args.annotations);
    // Convert our annotations file path from a `PathBuf` to a plain String
    // with error handling. This is necessary because Operating System
    // strings can have wild exceptions.
    let annotations_path = args.annotations.to_str()
        .ok_or(eyre!("Failed to parse annotations file path: {:?}", args.annotations))?
        .to_string();

    // Parse the file extension ('tsv', 'csv', etc.))
    log::debug!("Parsing annotations file extension: {:?}", &args.annotations);
    let annotations_ext = args.annotations.extension()
        .and_then(|p| p.to_str())
        .ok_or(eyre!("Failed to parse annotations file extension: {:?}", args.annotations))?
        .to_string();

    // Identify the delimiter based on the file extension (comma or tab separated).
    log::debug!("Parsing annotations file delimiter: {:?}", &args.annotations);
    let annotations_delimiter = match annotations_ext.as_str() {
        "csv" => {
            log::warn!("File is assumed to be comma delimited.");
            b','
        },
        _     => {
            log::warn!("File is assumed to be tab delimited.");
            b'\t'
        },
    };

    // Configure out table reading options
    let read_options = CsvReadOptions{ delimiter: annotations_delimiter, ..Default::default()};
    // Read in the file as a dataframe
    let df = ctx.read_csv(&annotations_path, read_options).await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the results
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
    .to_string();

    println!("{pretty_results}");
    
    Ok(())
}