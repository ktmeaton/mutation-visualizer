use arrow::array::StringArray;
//use arrow::compute::min;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use color_eyre::eyre::{eyre, Report, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::CsvOptions;
use datafusion::prelude::*;
use log;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use range_set_blaze::RangeSetBlaze;

use crate::{read_csv, register_csv};

/// Names of columns with nextclade that contain ',' separate mutations
pub const MUTATION_COLUMNS: &[&str] = &[
    "substitutions", 
    "deletions", 
    "insertions", 
    "frameShifts", 
    "aaSubstitutions", 
    "aaDeletions", 
    "aaInsertions", 
    "privateNucMutations.reversionSubstitutions"
];

pub const GENOME_LENGTH: u32 = 29903;

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
    let delimiter: Option<u8> = None;

    // Parse the annotations file into a dataframe that is registered to accept SQL queries.
    log::info!("Reading annotations file: {:?}", &args.annotations);

    // ------------------------------------------------------------------------
    // Annotations Input

    let name = "annotations";
    let ctx = crate::register_csv(&args.annotations, ctx, delimiter, name).await?;

    // Preview the first 10 records (or less)
    let batches = ctx.sql("SELECT * FROM annotations LIMIT 10").await?.collect().await?;
    log::info!("batches:\n{}", pretty_format_batches(&batches)?.to_string());
    // Check that the table is not empty
    if batches.len() == 0 { 
        return Err(eyre!("No annotations were found in file: {:?}", &args.annotations))
    }

    // ------------------------------------------------------------------------
    // Nextclade Input

    if let Some(nextclade_path) = &args.nextclade {
        log::info!("Reading nextclade file: {:?}", nextclade_path);
        let name = "nextclade";
    
        // Read in the nextclade tsv, registering it as for sql queries
        let ctx = register_csv(nextclade_path, ctx, delimiter, name).await?;
         // Select the nextclade table, check to make sure there are records, and display a preview.

        // Preview the first 10 records (or less)
        let batches = ctx.sql("SELECT * FROM nextclade LIMIT 10").await?.collect().await?;
        // Check that the table is not empty
        if batches.len() == 0 { 
            return Err(eyre!("No nextclade records were found in file: {:?}", nextclade_path))
        }

        // Create a table of missing data (long)
        // If the alignmentEnd field is null, consider the whole genome is missing
        ctx
            .sql(&format!("
                CREATE TABLE missing AS
                SELECT 
                    sample,
                    CAST(split_part(missing, '-', 1) as int) as start,
                    CAST(split_part(missing, '-', 2) as int) as stop
                FROM
                    (SELECT
                        nextclade.\"seqName\" as sample,
                        unnest(string_to_array(nextclade.\"missing\", ',', '')) as missing
                    FROM nextclade

                    UNION

                    SELECT 
                        nextclade.\"seqName\" as sample,
                        '1-{GENOME_LENGTH}' as missing
                    FROM nextclade
                    WHERE nextclade.\"alignmentEnd\" IS NULL
                    )
            ")).await?;
        let batches = ctx.sql("SELECT * FROM missing").await?.collect().await?;
        log::info!("batches:\n{}", pretty_format_batches(&batches)?.to_string());

        // Create a table of mutations (long)
        ctx
            .sql("
                CREATE TABLE mutations AS
                (SELECT 
                    nextclade.\"seqName\" as sample,
                    'aaSubstitutions' as column,
                    unnest(string_to_array(nextclade.\"aaSubstitutions\", ',', '')) as mutation
                FROM nextclade
                
                UNION

                SELECT 
                    nextclade.\"seqName\" as sample,
                    'substitutions' as column,
                    unnest(string_to_array(nextclade.\"substitutions\", ',', '')) as mutation
                FROM nextclade
                
                ORDER BY sample,column,mutation)
                "
            ).await?;

        // Create a table of annotated mutations (long)
        ctx
            .sql(
                "CREATE TABLE annotated_mutations AS
                (
                    SELECT 
                        mutations.sample,annotations.* 
                    FROM 
                        annotations INNER JOIN mutations ON annotations.mutation = mutations.mutation ORDER BY sample,start,stop
                )"
            ).await?;
            
        let batches = ctx.sql("SELECT * FROM annotated_mutations").await?.collect().await?;
        log::info!("batches:\n{}", pretty_format_batches(&batches)?.to_string());

        // Identify missing mutations based on overlaps with missing ranges

        // Create a table of missing annotated mutations (long)
        ctx
            .sql(
                "CREATE TABLE missing_mutations AS
                (
                    SELECT 
                        missing.sample,annotations.* 
                    FROM 
                        annotations INNER JOIN missing ON annotations.start >= missing.start AND annotations.stop <= missing.stop
                    ORDER BY
                        sample,start,stop
                )"
            ).await?;

        let batches = ctx.sql("SELECT * FROM missing_mutations").await?.collect().await?;
        log::info!("batches:\n{}", pretty_format_batches(&batches)?.to_string());

        // Create the final table
        let df = ctx.sql("
            SELECT *,'false' as missing FROM annotated_mutations
            UNION
            SELECT *,'true' as missing FROM missing_mutations
            ORDER BY sample,start,stop").await?;

        let batches = df.clone().collect().await?;
        log::info!("batches:\n{}", pretty_format_batches(&batches)?.to_string());            
            
        let write_options = DataFrameWriteOptions::default();
        let csv_options = CsvOptions::default().with_delimiter(b'\t');
        let output =   "nextclade_annotated.tsv";      
        df.write_csv(output, write_options, Some(csv_options)).await?;
    }

     // a is the set of integers from 100 to 499 (inclusive) and 501 to 1000 (inclusive)
    let a = RangeSetBlaze::from_iter([0..=50, 100..=200]);
    log::info!("a: {a:?}");
    // b is the set of integers -20 and the range 400 to 599 (inclusive)
    let b = RangeSetBlaze::from_iter([10..=10, 40..=60]);
    log::info!("b: {b:?}");
    // c is the union of a and b, namely -20 and 100 to 999 (inclusive)
    let c = a & b;
    log::info!("c: {c:?}");

    Ok(())
}