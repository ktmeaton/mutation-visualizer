use arrow::array::StringArray;                    // Convert arrow column to String type
use arrow::util::pretty::pretty_format_batches;   // Pretty print arrow records
use color_eyre::eyre::{eyre, Report, Result};     // Generic error handling with pretty logging
use datafusion::dataframe::DataFrameWriteOptions; // Customize how to write the final dataframe.
use datafusion::config::CsvOptions;               // Customize how to write CSV.
use datafusion::prelude::*;                       // All the essential datafusion functions.
use log;                                          // Logging, with verbosity filters
use std::path::{Path, PathBuf};                   // System file paths

// Dev constants, to be turned into function arguments
pub const GENOME_LENGTH: u32 = 29903;
pub const PREVIEW_ROWS: u32 = 20;

/// Extract annotated mutations from nextclade tsv.
///
/// # Arguments
/// 
///   - `annotations` : A file path to a custom annotations table.
///       - Mandatory columns: `mutation`, `column`, 
///   - `nextclade`   : A file path to nextclade TSV output.
///
pub async fn annotate<P>(annotations: &P, nextclade: &P) -> Result<(), Report> 
where
    // The annotations and nextclade arguments can be any type, as long as we can
    // convert it to a path, and print it out in a debug log
    P: AsRef<Path> + std::fmt::Debug,
{
    log::info!("Beginning annotation.");

    // Start a new datafusion session for reading and querying tables
    // This is kind of like a pseudo-SQL database, in which we can load 
    // multiple tables for querying and joining
    let ctx = SessionContext::new();

    // ------------------------------------------------------------------------
    // Annotations Input

    log::info!("Reading annotations file: {:?}", &annotations);

    // We won't hard-coded a delimiter, we'll detect based on file extension, ex. .tsv -> '\t', .csv -> ','
    // Convert the annotations path from a generic <P> to specifically a Path object
    // Give the table a name for SQL queries
    // Read the annotations table and register for SQL queries
    let delimiter: Option<u8> = None;
    let annotations: PathBuf  = annotations.as_ref().into();
    let name                  = "annotations";    
    let ctx                   = crate::register_csv(&annotations, ctx, delimiter, name).await?;

    // Preview the annotations table, and check that it's not empty
    let batches = ctx.sql(&format!("SELECT * FROM annotations LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
    log::info!("Annotations preview:\n{}", pretty_format_batches(&batches)?.to_string());
    if batches.len() == 0 { 
        return Err(eyre!("No annotations were found in file: {:?}", &annotations))
    }

    // Type casting. the is_gene column should be treatead as boolean ('true', 'false')
    // ctx.sql("UPDATE annotations SET is_gene=CAST(is_gene AS BOOLEAN)").await?;

    // Parse out the mutation columns we'd like to search in the nextclade tsv
    // Convert them from the specialized arrow array to Strings
    let batches = ctx.sql("SELECT DISTINCT ON (column) column FROM annotations").await?.collect().await?;    
    let mutation_columns: Vec<&str> = batches
        .iter()
        .filter_map(|batch| batch.column(0).as_any().downcast_ref::<StringArray>())
        .flat_map(|x| x.iter().filter_map(|s| s).collect::<Vec<_>>())
        .collect();
    log::debug!("Mutation columns to search: {mutation_columns:?}");

    // ------------------------------------------------------------------------
    // Nextclade Input

    log::info!("Reading nextclade file: {:?}", &nextclade);

    // Convert the nextclade path from a generic <P> to specifically a Path object
    // Give the table a name for SQL queries
    // Read the nextclade table and register for SQL queries
    let nextclade: PathBuf = nextclade.as_ref().into();
    let name               = "nextclade_raw";
    let ctx                = crate::register_csv(&nextclade, ctx, delimiter, name).await?;

    // Check that the table is not empty
    // We don't display the table preview, because nextclade output is huge!    
    let batches = ctx.sql("SELECT * FROM nextclade_raw LIMIT 10").await?.collect().await?;
    if batches.len() == 0 { 
        return Err(eyre!("No nextclade records were found in file: {:?}", nextclade))
    }

    // --------------------------------------------------------------------
    // Column Renaming and Type Conversion (Wide Dataframe)

    // Extract only the columns we need, convert them all to UTF-8.
    // Aside from the mutation columns specified in the annotations table,
    // There are a few mandatory columns we need for figuring out missing data
    let select_options = vec!["seqName", "missing", "alignmentEnd", "unknownAaRanges"]
        .iter()
        .chain(&mutation_columns)
        .map(|column| format!("arrow_cast(nextclade_raw.\"{column}\", 'Utf8') as {column}"))
        .collect::<Vec<_>>().join(",");

    ctx.sql(&format!("CREATE TABLE nextclade AS SELECT {select_options} FROM nextclade_raw")).await?;

    // Drop the raw table
    ctx.sql("DROP TABLE nextclade_raw").await?;

    // Again, we're not going to display a preview, because nextclade output is too wide
    // at this point

    // --------------------------------------------------------------------
    // Mutations (Long Dataframe)

    // Create a table of mutations (long) by splitting all the mutation columns
    // by their internal separator (',')
    let subquery = mutation_columns
        .iter()
        .map(|column| {
            format!(
            "SELECT 
                seqName as sample,
                unnest(string_to_array({column}, ',', '')) as mutation,
                '{column}' as column                
            FROM nextclade"
            )
        })
    .collect::<Vec<_>>().join(" UNION ");

    // Convert the nextclade wide mutations table to long format, with a separate
    // row for each mutation, the SQL statements are constants defined at the end of
    // this file
    let query = format!("{SQL_CREATE_MUTATIONS_TABLE} {subquery} {SQL_CLOSE_MUTATIONS_TABLE} ORDER BY sample,column,gene,aa_start,aa_stop,start,stop");
    log::info!("Query: {query}");
    ctx.sql(&query).await?;
   
    let batches = ctx.sql(&format!("SELECT * FROM mutations LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
    log::info!("Mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // --------------------------------------------------------------------
    //  Observed Annotated Mutations

    // These include both specific mutations (ex. S:F456R) and gene-based
    // such as searching for anything in gene 'S' if requested.
    // SQL statement is defined as a constant at the bottom of this file
    let query = format!("{SQL_CREATE_ANNOTATED_MUTATIONS_TABLE}");
    log::info!("Query: {query}");
    ctx.sql(&query).await?;
        
    let batches = ctx.sql(&format!("SELECT * FROM annotated_mutations LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
    log::info!("Annotated mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // --------------------------------------------------------------------
    //  Expand Mutations

    // Expand our list of annotated mutations, based on the dynamic search
    // of mutations within genes.

    // let query = "CREATE TABLE annotations_expanded AS (
    //     SELECT DISTINCT * 
    //     FROM (
    //         SELECT 
    //             * EXCEPT(sample,is_gene,gene,status,aa_start,aa_stop),
    //             CAST(is_gene AS BOOLEAN) as is_gene
    //         FROM 
    //             annotated_mutations
    //         ) 
    //     UNION 
    //     SELECT * EXCEPT(is_gene),CAST(is_gene AS BOOLEAN) as is_gene FROM annotations)";
    let query = "
        SELECT DISTINCT * FROM ( SELECT * EXCEPT(sample,gene,status,aa_start,aa_stop) FROM annotated_mutations)";  
    let batches = ctx.sql(query).await?.collect().await?;
    //let batches = ctx.sql("SELECT * FROM annotations_expanded").await?.collect().await?;
    log::info!("Preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // // --------------------------------------------------------------------
    // // Missing Data (Long Dataframe)

    // // If the alignmentEnd field is null, consider the whole genome is missing
    // ctx
    //     .sql(&format!("
    //         CREATE TABLE missing AS
    //         SELECT 
    //             sample,
    //             arrow_cast(split_part(missing, '-', 1), 'Int32') as start,
    //             arrow_cast(split_part(missing, '-', 2), 'Int32') as stop
    //         FROM
    //             (SELECT
    //                 seqName as sample,
    //                 unnest(string_to_array(missing, ',', '')) as missing
    //             FROM nextclade

    //             UNION

    //             SELECT 
    //                 seqName as sample,
    //                 '1-{GENOME_LENGTH}' as missing
    //             FROM nextclade
    //             WHERE alignmentEnd IS NULL
    //             )
    //         ORDER BY sample,start,stop
    //     ")).await?;
    // let batches = ctx.sql("SELECT * FROM missing LIMIT 10").await?.collect().await?;
    // log::info!("Missing preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // // --------------------------------------------------------------------
    // // Add any generic gene mutations to the annotations

    // // --------------------------------------------------------------------
    // // Missing Mutations (Long Dataframe)        

    // // Create a table of missing annotated mutations (long)
    // // A mutation is considered missing if a missing range overlaps its
    // ctx
    //     .sql(
    //         "CREATE TABLE missing_mutations AS
    //         SELECT 
    //             missing.sample,'missing' as status, annotations.* 
    //         FROM 
    //             annotations INNER JOIN missing ON 
    //                 (missing.start >= annotations.start AND missing.stop <= annotations.stop) OR
    //                 (missing.start <= annotations.start AND missing.stop >= annotations.start) OR
    //                 (missing.start <= annotations.stop  AND missing.stop >= annotations.stop)
    //         ORDER BY
    //             sample,start,stop
    //         "
    //     ).await?;

    // let batches = ctx.sql("SELECT * FROM missing_mutations LIMIT 10").await?.collect().await?;
    // log::info!("Missing mutations preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // // --------------------------------------------------------------------
    // // Final Dataframe

    // // Create the final table
    // let df = ctx.sql("
    //     SELECT * FROM annotated_mutations
    //     UNION
    //     SELECT * FROM missing_mutations
    //     ORDER BY sample,start,stop").await?;

    // let batches = df.clone().limit(0, Some(10))?.collect().await?;
    // log::info!("Final preview:\n{}", pretty_format_batches(&batches)?.to_string());            
        
    // let write_options = DataFrameWriteOptions::default();
    // let csv_options = CsvOptions::default().with_delimiter(b'\t');
    // let output = "nextclade_annotated.tsv";      
    // df.write_csv(output, write_options, Some(csv_options)).await?;

    Ok(())
}

// This is a horrendous SQL statement. The alternative would be to break up 
// each subquery into creating a new table, and drop the tables as we go, which
// seems like a memory limit risk.
// To be used as: format!("{CREATE_MUTATIONS_TABLE_QUERY} {subquery} {CREATE_MUTATIONS_TABLE_PARENTHESES}");
pub const SQL_CREATE_MUTATIONS_TABLE: &str = "
CREATE TABLE mutations AS
    SELECT 
        * EXCEPT(start,stop),
        CASE WHEN gene IS NULL THEN start ELSE NULL  END as start,
        CASE WHEN gene IS NULL THEN stop  ELSE NULL  END as stop,
        CASE WHEN gene is NULL THEN NULL  ELSE start END as aa_start,
        CASE WHEN gene is NULL THEN NULL  ELSE stop  END as aa_stop
    FROM (
        SELECT
            * EXCEPT(coordinates),
            arrow_cast(CASE WHEN coordinates = '' AND gene IS NULL THEN split_part(mutation, ':', 1) ELSE split_part(coordinates, '-', 1) END, 'Int32') as start,
            arrow_cast(CASE WHEN coordinates LIKE '%-%' 
                THEN 
                    split_part(coordinates, '-', 2) 
                ELSE 
                    (CASE WHEN coordinates = '' AND gene IS NULL THEN split_part(mutation, ':', 1) ELSE split_part(coordinates, '-', 1) END)
                END, 'Int32') as stop
        FROM (
            SELECT 
                * EXCEPT(gene),
                CASE WHEN TRY_CAST(gene AS Int) THEN NULL ELSE gene END as gene,
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(mutation, '^(.*:)', ''), '^[A-Za-z]', '' ), '[A-Za-z]$', '') as coordinates
            FROM (
                SELECT 
                    *,
                    CASE WHEN mutation LIKE '%:%' THEN split_part(mutation, ':', 1) ELSE NULL END as gene
                FROM ("; // insert SELECT subquery after
pub const SQL_CLOSE_MUTATIONS_TABLE: &str = "))))";

pub const SQL_CREATE_ANNOTATED_MUTATIONS_TABLE: &str = "
CREATE TABLE annotated_mutations AS
(
    SELECT M.*,'present' as status,A.* EXCEPT(mutation,column,start,stop)
    FROM (SELECT * FROM annotations WHERE is_gene = TRUE) A 
    INNER JOIN (SELECT * FROM mutations WHERE gene IS NOT NULL) M 
    ON (A.mutation = M.gene AND A.column = M.column)

    UNION

    SELECT M.*,'present' as status,A.* EXCEPT(mutation,column,start,stop)
    FROM (SELECT * FROM annotations WHERE is_gene = FALSE) A 
    INNER JOIN (SELECT * FROM mutations WHERE gene IS NULL) M 
    ON (A.mutation = M.mutation AND A.column = M.column)

) ORDER BY sample,start,stop,gene,aa_start,aa_stop";
