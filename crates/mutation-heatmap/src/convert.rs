use arrow::util::pretty::pretty_format_batches;   // Pretty print arrow records
use color_eyre::eyre::{Result, Report};
use datafusion::prelude::*; 

pub const SCHEMA_INFER_MAX_RECORDS: usize = 100;

pub async fn convert() -> Result<(), Report> {

    let ctx = SessionContext::new();
    // Read csv (tsv) file

    let path = "data/sars-cov-2/nextclade/nextclade.ndjson";
    let options = NdJsonReadOptions { 
        schema_infer_max_records: SCHEMA_INFER_MAX_RECORDS, 
        file_extension: "ndjson",
        ..Default::default()
    };

    // // Register for SQL queries
    // ctx.register_json("nextclade", path, options).await?;
    // let query = "
    //     SELECT 
    //         \"seqName\",
    //         unnest(\"frameShifts\") as \"frameShifts\"
    //     FROM nextclade";
    
    
    // let batches = ctx.sql(&query).await?.collect().await?;
    // println!("Preview:\n{}", pretty_format_batches(&batches)?.to_string());

    // Register for SQL queries
    let df = ctx.read_json(path, options).await?;
    let df = df
        .with_column_renamed("\"seqName\"", "seqname")?
        .with_column_renamed("\"frameShifts\"", "frameshifts")?;

    let df = df
        .select(vec![col("seqName"), col("frameshifts")])?
        .unnest_columns(&["frameshifts"])?
        .unnest_columns(&["frameshifts"])?
        .unnest_columns(&["frameshifts.codon"])?;
        //.with_column_renamed("codon.begin")?
    
    
    let batches = df.collect().await?;
    println!("Preview:\n{}", pretty_format_batches(&batches)?.to_string());    
    
    // Write to delta lake
    Ok(())
}