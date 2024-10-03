// use arrow::util::pretty::pretty_format_batches;
// use arrow::datatypes::{Field, Schema};
// use datafusion::prelude::*;
// use datafusion::datasource::stream::{FileStreamProvider, StreamTable, StreamTableFactory};
// use datafusion::datasource::streaming::StreamingTable;
// use datafusion::catalog::TableProviderFactory;
// use std::io::{self, BufRead};

use std::fs;
use std::error::Error;

use itertools::Itertools; // 0.8.0
use std::io::BufRead;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //let stdin = std::io::stdin();
    use arrow::util::pretty::pretty_format_batches;
    use std::fs::File;
    use std::io::BufReader;
    use tempfile::tempdir;
    use std::io::Write;
    use datafusion::prelude::*;

    let file = BufReader::new(File::open("nextclade.ndjson")?);
    let n = 100;

    let ctx = SessionContext::new();
    let options = NdJsonReadOptions {
        file_extension: "ndjson",
        ..Default::default()
    };

    // Create a directory inside of `env::temp_dir()`.
    let dir = tempdir()?;

    let file_path = dir.path().join("tmp.ndjson");
    let mut tmpfile = File::create(file_path.clone())?;

    for chunk in &file.lines().chunks(n) {
        for line in chunk {
            writeln!(tmpfile, "{}", line?)?;
        }

        let path = file_path.clone().into_os_string().into_string().unwrap();
        let df = ctx.read_json(&path, options.clone()).await?;
        let batches = df.collect().await?;
        println!("Preview:\n{}", pretty_format_batches(&batches)?.to_string());
    }

    Ok(())
}
// fn main() -> Result<(), Box<dyn Error>> {
//     println!("Reading file!");
//     let message: String = fs::read_to_string("nextclade.ndjson")?;
//     println!("{}", message);
//     Ok(())
// }

// #[tokio::main]
// async fn main() {

//     fs::read_to_string

//     // let path = "nextclade.ndjson";
//     // let options = NdJsonReadOptions { 
//     //     schema_infer_max_records: 10, 
//     //     file_extension: "ndjson",
//     //     ..Default::default()
//     // };
//     // let ctx = SessionContext::new();
//     // let df = ctx.read_json(path, options).await.unwrap();    
//     // let batches = df.collect().await.unwrap();
//     // println!("Preview:\n{}", pretty_format_batches(&batches).unwrap().to_string());    
    

//     // let schema = arrow::datatypes::Schema::empty();
//     // let path = "test.ndjson";
//     // let provider = FileStreamProvider::new_file(schema.into(), path.into());

//     // let path = "test.ndjson";
//     // let options = NdJsonReadOptions { 
//     //     schema_infer_max_records: 10, 
//     //     file_extension: "ndjson",
//     //     ..Default::default()
//     // };
//     // let ctx = SessionContext::new();
//     // let df = ctx.read_json(path, options).await.unwrap();

//     // let batches = df.collect().await.unwrap();
//     // println!("Preview:\n{}", pretty_format_batches(&batches).unwrap().to_string());

//     // let schema = Schema::new(vec![
//     //     Field::new("c1", DataType::Int64, true),
//     //     Field::new("c2", DataType::Utf8, false),
//     //     Field::new("c3", DataType::Utf8, false),
//     // ]);
//     // let table = StreamingTable::try_new(schema.into()).unwrap();

//     // let ctx = SessionContext::new();

//     // // read_json(DataFilePaths, NdJsonReadOptions)
//     // // DataFilePaths is a trait

//     // // let path = "data/sars-cov-2/nextclade/nextclade.ndjson";
//     // let options = NdJsonReadOptions { 
//     //     schema_infer_max_records: 10, 
//     //     file_extension: "ndjson",
//     //     ..Default::default()
//     // };    

//     // let stdin = io::stdin();
//     // for line in stdin.lock().lines() {
//     //     let content = line.unwrap();
//     //     println!("content: {content}");
//     //     let df = ctx.read_json(content, options.clone()).await.unwrap();
//     //     println!("df: {df:?}");
//     //     break
//     // }
// }
