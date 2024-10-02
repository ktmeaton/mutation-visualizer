use arrow::util::pretty::pretty_format_batches;   // Pretty print arrow records
use color_eyre::eyre::{eyre, Report, Result};     // Generic error handling with pretty logging
use datafusion::dataframe::DataFrameWriteOptions; // Customize how to write the final dataframe.
use datafusion::config::{CsvOptions, TableParquetOptions};  // Customize how to write output CSV or Parquet.
use datafusion::prelude::*;                       // All the essential datafusion functions.
use log;                                          // Logging, with verbosity filters
use std::path::{Path, PathBuf};                   // System file paths

// Dev constants, to be turned into function arguments
pub const GENOME_LENGTH: u32 = 29903;
pub const PREVIEW_ROWS: u32 = 20;
pub const NUCLEOTIDE_COLUMNS: &[&str] = &[
    "substitutions",
    "deletions",
    "insertions",
];

pub const AMINO_ACID_COLUMNS: &[&str] = &[
    "frameShifts",
    "aaSubstitutions",
    "aaDeletions",
    "aaInsertions", 
];

/// Extract mutations from nextclade tsv.
///
/// # Arguments
/// 
///   - `nextclade`: A file path to nextclade TSV output.
///   - `gff`      : A file path to nextclade dataset GFF3 annotations.
///       - Example: <https://github.com/nextstrain/nextclade_data/blob/master/data/nextstrain/sars-cov-2/wuhan-hu-1/orfs/genome_annotation.gff3>
///
pub async fn extract<P>(nextclade: P, gff: P) -> Result<(), Report>
where
    // The nextclade and gff arguments can be any type, as long as we can
    // convert it to a path, and print it out in a debug log
    P: AsRef<Path> + std::fmt::Debug,
{
    log::info!("Beginning extraction.");

    log::info!("Beginning extraction.");    

    // Start a new datafusion session for reading and querying tables
    // This is kind of like a pseudo-SQL database, in which we can load 
    // multiple tables for querying and joining
    let ctx = SessionContext::new();

    // We won't hard-coded a delimiter for input files, we'll detect 
    // based on file extension ex. .tsv -> '\t', .csv -> ','
    let delimiter: Option<u8> = None;

    // ------------------------------------------------------------------------
    // GFF Input

    // Read in the GFF annotations and register the table for sql queries
    let name = "gff";
    let ctx  = crate::register_gff(&gff, ctx, name).await?;

    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("SELECT * FROM gff LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("GFF preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

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
    let batches = ctx.sql("SELECT * FROM nextclade_raw LIMIT 1").await?.collect().await?;
    if batches.len() == 0 { 
        return Err(eyre!("No nextclade records were found in file: {:?}", nextclade))
    }

    // --------------------------------------------------------------------
    // Column Renaming and Type Conversion (Wide Dataframe)

    log::info!("Converting columns to Utf-8.");

    // Extract only the columns we need, convert them all to UTF-8.
    let select_options = vec!["seqName"]
        .iter()
        .chain(NUCLEOTIDE_COLUMNS)
        .chain(AMINO_ACID_COLUMNS)
        .map(|column| format!("arrow_cast(nextclade_raw.\"{column}\", 'Utf8') as {column}"))
        .collect::<Vec<_>>().join(",");

    ctx.sql(&format!("CREATE TABLE nextclade AS SELECT {select_options} FROM nextclade_raw")).await?.collect().await?;

    // Drop the raw table?
    ctx.sql("DROP TABLE nextclade_raw").await?;

    // Again, we're not going to display a preview, because nextclade output is too wide

    // --------------------------------------------------------------------
    // Convert Wide Mutations Dataframe to Long Dataframe

    // Split all mutation columns by their internal separator (',').
    // ie. Convert the wide nextclade table to a long table with 
    // a separate row for each mutation. The UNNEST function takes an 
    // ARRAY and returns a table with a row for each element in the ARRAY.
    log::info!("{}", format!("Extracting nucleotide mutation columns: {NUCLEOTIDE_COLUMNS:?}"));
    log::info!("{}", format!("Extracting amino-acid mutation columns: {AMINO_ACID_COLUMNS:?}"));
    let aa_columns_sql = format!("( '{}' )", AMINO_ACID_COLUMNS.join("','"));
    let query = NUCLEOTIDE_COLUMNS
        .iter()
        .chain(AMINO_ACID_COLUMNS)
        .map(|column| format!("
            SELECT 
                seqName as sample,
                unnest(string_to_array({column}, ',', '')) as mutation,
                '{column}' as column,
                CASE WHEN '{column}' IN {aa_columns_sql} THEN 'amino-acid' ELSE 'nucleotide' END as type
            FROM nextclade"))
        .collect::<Vec<_>>().join(" UNION ");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Mutation columns preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    // --------------------------------------------------------------------
    // Gene Name

    // Extract gene name from amino acid mutations -> (ORF1a:T3255I -> ORF1a)
    log::info!("Extracting gene name from amino acid mutations: {AMINO_ACID_COLUMNS:?}");
    let aa_columns_sql = format!("( '{}' )", AMINO_ACID_COLUMNS.join("','"));
    let query = format!("SELECT *,CASE WHEN column IN {aa_columns_sql} THEN split_part(mutation, ':', 1) ELSE NULL END as gene FROM ({query})");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Gene preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    // --------------------------------------------------------------------
    // Coordinates

    // Extract coordinates from mutations ->  (ORF1a:T3255I -> 3255, 28933:T -> 28933, S:214:EPE -> 214, N:221-298 -> 221-298)
    // Amino Acid mutations are in codon coordinates, so we'll store that as a 
    // separate column from the nucleotide coordinates for now.
    log::info!("Extracting mutation coordinates.");
    let query = format!("
    SELECT 
        *,
        CASE WHEN column IN {aa_columns_sql} 
            THEN CASE WHEN column = 'aaInsertions' 
                THEN split_part(mutation, ':', 2) 
                ELSE REGEXP_REPLACE(split_part(mutation, ':', 2), '([A-Za-z:]+|-$)', '', 'g')
                END
            ELSE
                NULL            
            END as aa_coord,
        CASE WHEN column NOT IN {aa_columns_sql} 
            THEN REGEXP_REPLACE(mutation, '(:.*$|[A-Za-z:]+)', '', 'g') 
            ELSE NULL 
            END as nuc_coord
    FROM ({query})");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Coordinates preview:\n{}", pretty_format_batches(&batches)?.to_string()); 
    }


    // --------------------------------------------------------------------
    // Coordinate Ranges

    // Convert the coordinate ranges (ex. 221-223) to separate 
    // start (ex. 221) and end (ex. 223) columns and convert them
    // from string type to explicitly 32-bit unsigned integer.

    log::info!("Extracting start and end positions of coordinates.");
    let query = format!("
    SELECT 
        * EXCEPT(nuc_coord,aa_coord),
        arrow_cast(split_part(nuc_coord, '-', 1), 'UInt32') as nuc_start,
        arrow_cast(CASE WHEN nuc_coord LIKE '%-%' THEN split_part(nuc_coord, '-', 2) ELSE split_part(nuc_coord, '-', 1) END, 'UInt32')  as nuc_end,
        arrow_cast(split_part(aa_coord, '-', 1), 'UInt32') as aa_start,
        arrow_cast(CASE WHEN aa_coord LIKE '%-%' THEN split_part(aa_coord, '-', 2) ELSE split_part(aa_coord, '-', 1) END, 'UInt32') as aa_end
    FROM ({query})");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Coordinate ranges preview:\n{}", pretty_format_batches(&batches)?.to_string());
    } 

    // --------------------------------------------------------------------
    // Join Mutations to GFF

    // Left Join mutations to the GFF annotations, to get gene start and end coordinates

    log::info!("Joining mutations to GFF annotations.");
    let query = format!("
        SELECT 
            * EXCEPT(gene,name),
            CASE WHEN gene IS NULL and name IS NOT NULL THEN name ELSE gene END as gene
        FROM ({query}) M
        LEFT JOIN (SELECT name,start as gene_start,end as gene_end FROM gff WHERE gff.type = 'gene') G 
        ON M.gene = G.name OR (M.nuc_start >= G.gene_start AND M.nuc_end <= G.gene_end)
    ");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Join preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    // ------------------------------------------------------------------------
    // Finalize coordinates

    // We will use the GFF gene coordinates to convert aa positions to nucleotide
    // positions and vice-versa when a nucleotide mutation falls within a gene.

    log::info!("Finalizing coordinates.");
    let query = format!("
    SELECT 
        * EXCEPT(aa_start,aa_end,nuc_start,nuc_end,gene_start,gene_end),
        CASE WHEN nuc_start IS NULL AND gene_start IS NOT NULL AND aa_start IS NOT NULL 
            THEN ((aa_start - 1) * 3) + gene_start
            ELSE nuc_start
            END as nuc_start,
        CASE WHEN nuc_end IS NULL AND gene_start IS NOT NULL AND aa_end IS NOT NULL 
            THEN (aa_start * 3) + gene_start
            ELSE nuc_end
            END as nuc_end,
        CASE WHEN aa_start IS NULL AND gene_start IS NOT NULL AND nuc_start IS NOT NULL 
            THEN ((nuc_start - gene_start) / 3) + 1
            ELSE aa_start
            END as aa_start,
        CASE WHEN aa_end IS NULL AND gene_start IS NOT NULL AND nuc_end IS NOT NULL 
            THEN ((nuc_end - gene_start) / 3) + 1
            ELSE aa_end
            END as aa_end
    FROM ({query})");
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("{query} LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Finalized coordiantes preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    // ------------------------------------------------------------------------
    // Create Table

    log::info!("Creating the final table.");
    let query = format!("CREATE TABLE mutations AS SELECT * FROM ({query}) ORDER BY sample,nuc_start,nuc_end");
    ctx.sql(&query).await?;
    // Debug Preview
    if log::log_enabled!(log::Level::Debug) {
        let batches = ctx.sql(&format!("SELECT * FROM mutations LIMIT {PREVIEW_ROWS}")).await?.collect().await?;
        log::debug!("Final table preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    // ------------------------------------------------------------------------
    // Write Table

    log::info!("Writing the final tsv table.");
    let df = ctx.sql("SELECT * FROM mutations").await?;
    let write_options = DataFrameWriteOptions::default();
    let csv_options = CsvOptions::default().with_delimiter(b'\t');
    let output = "mutations.tsv";      
    df.write_csv(output, write_options, Some(csv_options)).await?; 

    log::info!("Writing the final parquet table.");
    let df = ctx.sql("SELECT * FROM mutations").await?;
    let parquet_options = TableParquetOptions::default();
    let write_options = DataFrameWriteOptions::default(); 
    let output = "mutations.parquet";
    df.write_parquet(output, write_options, Some(parquet_options)).await?; 

    log::info!("Finished extraction.");

    Ok(())
}
