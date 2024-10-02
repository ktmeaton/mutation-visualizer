use arrow::array::{StringArray, UInt32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use color_eyre::eyre::{eyre, Report, Result};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use noodles::gff;
use std::path::{Path, PathBuf};
use std::io::BufReader;
use std::sync::Arc;

pub mod extract;
#[cfg(feature = "plot")]
pub mod plot;

#[doc(inline)]
pub use crate::extract::extract;
#[cfg(feature = "plot")]
pub use crate::plot::plot;

#[derive(Copy, Clone, Debug)]
pub enum OutputFormat {
    Tsv,
    Parquet
}

/// Light wrapper around datafusions register_csv.
pub async fn register_csv<P,N>(path: &P, ctx: SessionContext, delimiter: Option<u8>, name: N) -> Result<SessionContext, Report>
where
    P: AsRef<Path> + std::fmt::Debug,
    N: ToString,
{
    // Convert the csv path to a plain string, and identify the extension and delimiter
    // This is needed to make datafusion happy.
    let (path, ext, delimiter) = parse_csv_path(path, delimiter)?;
    // Use our dynamically detected extensions and delimiter to configure the reader
    let read_options = CsvReadOptions::new().file_extension(&ext).delimiter(delimiter);  
    // Register the csv as dataframe that can accept SQL queries.
    ctx.register_csv(&name.to_string(), &path, read_options).await?;
    Ok(ctx)
}

/// Light wrapper around datafusions read_csv.
pub async fn read_csv<P>(path: &P, ctx: &SessionContext, delimiter: Option<u8>) -> Result<DataFrame, Report>
where
    P: AsRef<Path> + std::fmt::Debug,
{
    // Convert the csv path to a plain string, and identify the extension and delimiter
    // This is needed to make datafusion happy.
    let (path, ext, delimiter) = parse_csv_path(path, delimiter)?;
    // Use our dynamically detected extensions and delimiter to configure the reader
    let read_options = CsvReadOptions::new().file_extension(&ext).delimiter(delimiter);  
    // Register the csv as dataframe that can accept SQL queries.
    let df = ctx.read_csv(path, read_options).await?;
    Ok(df)
}

pub fn parse_csv_path<P>(path: P, delimiter: Option<u8>) -> Result<(String, String, u8), Report>
where
    P: AsRef<Path> + std::fmt::Debug
{
    log::debug!("Parsing file path: {:?}", path);

    // Datafusion has very specific requires about what format the input path can be.
    // The easiest is to convert it into a plain String.

    // Step 1. Convert from generic <P> to an owned PathBuf. This gives us a unified
    //         way to convert it ot a plain String.
    let path: PathBuf = path.as_ref().into();

    // Step 2. Parse the file extension ('tsv', 'csv', etc.))
    let ext = path.extension()
        .and_then(|p| p.to_str())
        .ok_or(eyre!("Failed to parse file extension: {:?}", path))?
        .to_string();

    // Step 3. Convert PathBuf to String to make Datafusion happy.
    let path = path
        .to_str()
        .ok_or(eyre!("Failed to parse file path: {:?}", path))?
        .to_string();

    // Step 4. Identify the delimiter if it was not supplied.
    let delimiter = match delimiter {
        Some(d) => d,
        None    => match ext.as_str() {
            "csv" => { log::debug!("File is assumed to be comma delimited."); b','  },
            _     => { log::debug!("File is assumed to be tab delimited.");   b'\t' },
        },
    };

    Ok((path, ext, delimiter))
}

/// Light wrapper around noodles GFF reader and datafusion register.
pub async fn register_gff<N, P>(path: P, ctx: SessionContext, name: N) -> Result<SessionContext, Report>
where
    P: AsRef<Path> + std::fmt::Debug,
    N: ToString,
{
    log::info!("Reading gff file: {path:?}");

    let input = std::fs::File::open(&path)?;
    let buffered = BufReader::new(input);
    let mut reader = gff::io::Reader::new(buffered);

    // define the schema.
    // example: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udaf.rs

    let schema = Arc::new(Schema::new(vec![
        Field::new("name",  DataType::Utf8,   false),
        Field::new("type",  DataType::Utf8,   false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end",   DataType::UInt32, false),
    ]));

    // Containers for the essential fields we need from the GFF
    let mut names:  Vec<String> = Vec::new();
    let mut types:  Vec<String> = Vec::new();
    let mut starts: Vec<u32>    = Vec::new();
    let mut ends:   Vec<u32>    = Vec::new();

    // Search the attributes for these possible identifier names
    // The sars-cov-2 gff has a strange space before " gene_name"
    let name_attributes = vec!["Name", "gene_name", " gene_name", "gene"];

    for result in reader.records() {
        let record = result?;
        let attributes = record.attributes();
        for n in &name_attributes {
            if let Some(name) = attributes.get(&n.to_string()) {
                names.push(name.to_string());
                types.push(record.ty().to_string());
                starts.push(record.start().get() as u32);
                ends.push(record.end().get() as u32);
                break
            }
        }
    }

    let records = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(UInt32Array::from(starts)),
            Arc::new(UInt32Array::from(ends)),
        ],
    )?;   

    // declare a table in memory..
    let provider = MemTable::try_new(schema, vec![vec![records]])?;
    ctx.register_table(&name.to_string(), Arc::new(provider))?;

    Ok(ctx)
}