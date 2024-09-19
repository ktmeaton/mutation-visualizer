use chrono::Local;
use clap::Parser;
use color_eyre::eyre::{Report, Result};
use env_logger::Builder;
use mutation_visualizer::{annotate};
use mutation_visualizer_cli::{Cli, Command};
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Report> {

    // Parse arguments from the CLI
    let args = Cli::parse();
    // initialize color_eyre crate for colorized logs
    color_eyre::install()?;

    // Customize logging message format
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf, 
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, args.verbosity.to_levelfilter())
        .init();

    // check which CLI command we're running (dataset, run, plot)
    match args.command {
        // Annotate
        Command::Annotate(args) => annotate(&args).await?,  
    }

    Ok(())
}
