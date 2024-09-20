use arrow::array::StringArray;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use color_eyre::eyre::{eyre, Report, Result};
use datafusion::prelude::*;
use log;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::register_csv;

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
    log::info!("Beginning annotation.");

    // Start a new datafusion session for reading and querying tables
    let ctx = SessionContext::new();

    // Don't use a hard-coded delimiter, dynamically detect based on file extension
    let delimiter = None;

    // Parse the annotations file into a dataframe that is registered to accept SQL queries.
    log::info!("Reading annotations file: {:?}", &args.annotations);
    let path = &args.annotations;
    let name = "annotations";
    let ctx  = register_csv(path, ctx, name, delimiter).await?;

    // Select the annotations table, check to make sure there are records, and display a preview.
    let df = ctx.sql("SELECT * FROM annotations").await?;

    // Grab the first 10 records (or less)
    let preview = df.limit(0, Some(10))?.collect().await?;

    // Check that the table is not empty
    if preview.len() == 0 { 
        return Err(eyre!("No annotations were found in file: {:?}", &args.annotations)) 
    }

    // Display records as a preview
    log::info!("Annotation table preview:\n{}", pretty_format_batches(&preview)?.to_string());

    // Identify the columns in nextclade that we should search for mutations.
    let batches = ctx.sql("SELECT nextclade_column FROM annotations").await?.collect().await?;
    // Convert the data in the table from arrow format (StringArray) to a plain list of strings.
    let mut column_names: Vec<&str> = batches
        .iter()
        .filter_map(|batch| batch.column(0).as_any().downcast_ref::<StringArray>())
        .flat_map(|x| x.iter().filter_map(|s| s).collect::<Vec<_>>())
        .collect();
    // Dedeuplicate
    column_names.sort();
    column_names.dedup();

    if let Some(nextclade) = &args.nextclade {
        log::info!("Reading nextclade file: {:?}", nextclade);
        let path = nextclade;
        let name = "nextclade";
        let ctx = register_csv(path, ctx, name, delimiter).await?;
        // Select the nextclade table, check to make sure there are records, and display a preview.
        let df = ctx.sql("SELECT * FROM nextclade").await?;

        // Grab the first 10 records (or less)
        let preview = df.limit(0, Some(10))?.collect().await?;

        // Check that the table is not empty
        if preview.len() == 0 { 
            return Err(eyre!("No nextclade records were found in file: {:?}", &args.nextclade))
        }

        // Extract the sample IDs and mutations
        for col in column_names {
            log::info!("col: {col}");
            let batches = ctx.sql(&format!("SELECT nextclade.\"seqName\",nextclade.\"{col}\" FROM nextclade")).await?.collect().await?;
            println!("{}", pretty_format_batches(&batches).unwrap().to_string());
        }

        // column_names.iter().map(|col| async move {
        //     log::info!("col: {col}");
        //     let df = ctx.sql("SELECT seqName,{col} FROM nextclade").await.unwrap().collect().await().unwrap();
        //     println!("{}", pretty_format_batches(&df).unwrap().to_string());
        // });
    }
        

    //annotations.join(annotations_2, join_keys=(["customer_id"], ["id"]), how="left")
    
    Ok(())
}