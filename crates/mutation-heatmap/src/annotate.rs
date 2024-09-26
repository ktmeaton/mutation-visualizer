use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use color_eyre::eyre::{eyre, Report, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::CsvOptions;
use datafusion::prelude::*;
use log;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::register_csv;

/// Names of columns with nextclade that contain ',' separate mutations
pub const MUTATION_COLUMNS: &[&str] = &[
    "substitutions",
    "deletions",
    "insertions",
    "frameShifts",
    "aaSubstitutions",
    "aaDeletions",
    "aaInsertions",
    //"privateNucMutations.reversionSubstitutions"
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
    log::info!("Annotations preview:\n{}", pretty_format_batches(&batches)?.to_string());
    // Check that the table is not empty
    if batches.len() == 0 { 
        return Err(eyre!("No annotations were found in file: {:?}", &args.annotations))
    }

    // ------------------------------------------------------------------------
    // Nextclade Input

    if let Some(nextclade_path) = &args.nextclade {
        log::info!("Reading nextclade file: {:?}", nextclade_path);
        let name = "nextclade_raw";
    
        // Read in the nextclade tsv, registering it as for sql queries
        let ctx = register_csv(nextclade_path, ctx, delimiter, name).await?;
         // Select the nextclade table, check to make sure there are records, and display a preview.

        // Preview the first 10 records (or less)
        let batches = ctx.sql("SELECT * FROM nextclade_raw LIMIT 10").await?.collect().await?;
        // Check that the table is not empty
        if batches.len() == 0 { 
            return Err(eyre!("No nextclade records were found in file: {:?}", nextclade_path))
        }

        // --------------------------------------------------------------------
        // Column Renaming and Type Conversion (Wide Dataframe)

        // Extract only the columns we need, convert them all to UTF-8.
        let select_options = vec!["seqName", "missing", "alignmentEnd"]
            .iter()
            .chain(MUTATION_COLUMNS)
            .map(|column| format!("arrow_cast(nextclade_raw.\"{column}\", 'Utf8') as {column}"))
            .collect::<Vec<_>>().join(",");

        ctx.sql(&format!("CREATE TABLE nextclade AS SELECT {select_options} FROM nextclade_raw")).await?;

        // Drop the raw table?
        ctx.sql("DROP TABLE nextclade_raw").await?;

        // --------------------------------------------------------------------
        // Missing Data (Long Dataframe)

        // If the alignmentEnd field is null, consider the whole genome is missing
        ctx
            .sql(&format!("
                CREATE TABLE missing AS
                SELECT 
                    sample,
                    arrow_cast(split_part(missing, '-', 1), 'Int32') as start,
                    arrow_cast(split_part(missing, '-', 2), 'Int32') as stop
                FROM
                    (SELECT
                        seqName as sample,
                        unnest(string_to_array(missing, ',', '')) as missing
                    FROM nextclade

                    UNION

                    SELECT 
                        seqName as sample,
                        '1-{GENOME_LENGTH}' as missing
                    FROM nextclade
                    WHERE alignmentEnd IS NULL
                    )
                ORDER BY sample,start,stop
            ")).await?;
        let batches = ctx.sql("SELECT * FROM missing LIMIT 10").await?.collect().await?;
        log::info!("Missing preview:\n{}", pretty_format_batches(&batches)?.to_string());

        // --------------------------------------------------------------------
        // Mutations (Long Dataframe)

        // Create a table of mutations (long)
        let query = MUTATION_COLUMNS
            .iter()
            .map(|column| {
                format!(
                "SELECT 
                    seqName as sample,
                    '{column}' as column,
                    unnest(string_to_array({column}, ',', '')) as mutation
                FROM nextclade"
                )
            })
        .collect::<Vec<_>>().join(" UNION ");
        ctx.sql(&format!("CREATE TABLE mutations AS {query}")).await?;
        let batches = ctx.sql("SELECT * FROM mutations LIMIT 10").await?.collect().await?;
        log::info!("Mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

        // // --------------------------------------------------------------------
        // //  Observed Annotated Mutations (Regex)

        // ctx
        //     .sql(
        //         "CREATE TABLE regex_mutations AS
        //         SELECT * FROM annotations WHERE column == 'regex'
        //         "
        //     ).await?;

        // let batches = ctx.sql("SELECT * FROM regex_mutations LIMIT 10").await?.collect().await?;
        // log::info!("Regex mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

        // --------------------------------------------------------------------
        //  Observed Annotated Mutations (Exact)

        ctx
            .sql(
                "CREATE TABLE annotated_mutations AS
                (
                    SELECT 
                        M.sample,'present' as status, A.* 
                    FROM 
                        annotations A INNER JOIN (SELECT * FROM mutations WHERE column != 'regex') M ON 
                            (A.mutation = M.mutation AND A.column = M.column)
                         ORDER BY sample,start,stop
                )"
            ).await?;
            
        let batches = ctx.sql("SELECT * FROM annotated_mutations LIMIT 10").await?.collect().await?;
        log::info!("Annotated mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

        // --------------------------------------------------------------------
        // Missing Mutations (Long Dataframe)        

        // Create a table of missing annotated mutations (long)
        // A mutation is considered missing if a missing range overlaps its
        ctx
            .sql(
                "CREATE TABLE missing_mutations AS
                SELECT 
                    missing.sample,'missing' as status, annotations.* 
                FROM 
                    annotations INNER JOIN missing ON 
                        (missing.start >= annotations.start AND missing.stop <= annotations.stop) OR
                        (missing.start <= annotations.start AND missing.stop >= annotations.start) OR
                        (missing.start <= annotations.stop  AND missing.stop >= annotations.stop)
                ORDER BY
                    sample,start,stop
                "
            ).await?;

        let batches = ctx.sql("SELECT * FROM missing_mutations LIMIT 10").await?.collect().await?;
        log::info!("Missing mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

        // --------------------------------------------------------------------
        // Final Dataframe

        // Create the final table
        let df = ctx.sql("
            SELECT * FROM annotated_mutations
            UNION
            SELECT * FROM missing_mutations
            ORDER BY sample,start,stop").await?;

        let batches = df.clone().limit(0, Some(10))?.collect().await?;
        log::info!("Final preview:\n{}", pretty_format_batches(&batches)?.to_string());            
            
        let write_options = DataFrameWriteOptions::default();
        let csv_options = CsvOptions::default().with_delimiter(b'\t');
        let output = "nextclade_annotated.tsv";      
        df.write_csv(output, write_options, Some(csv_options)).await?;
    }

    Ok(())
}