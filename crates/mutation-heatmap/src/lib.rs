use color_eyre::eyre::{eyre, Report, Result};
use color_eyre::Help;
use clap::ValueEnum;
use datafusion::prelude::*;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use strum_macros::EnumIter;
use strum::IntoEnumIterator;
use thiserror::Error;

pub mod annotate;
pub mod plot;

#[doc(inline)]
pub use crate::annotate::{annotate, AnnotateArgs};
pub use crate::plot::{plot, PlotArgs};

#[derive(Clone, Debug, Default, Deserialize, EnumIter, Serialize, ValueEnum)]
pub enum Verbosity {
    Debug,
    Error,
    #[default]
    Info,
    Trace,
    Warn
}

impl Display for Verbosity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // Convert to lowercase for RUST_LOG env var compatibility
        let lowercase = format!("{:?}", self).to_lowercase();
        write!(f, "{lowercase}")
    }
}

impl Verbosity {
    /// Convert Verbosity to log LevelFilter
    pub fn to_levelfilter(self) -> log::LevelFilter {
        match self {
            Verbosity::Error => LevelFilter::Error,
            Verbosity::Warn  => LevelFilter::Warn,
            Verbosity::Info  => LevelFilter::Info,
            Verbosity::Debug => LevelFilter::Debug,
            Verbosity::Trace => LevelFilter::Trace,
        }
    }
}

impl FromStr for Verbosity {

    type Err = Report;

    /// Returns a [`Verbosity`] converted from a [`str`].
    ///
    /// ## Examples
    ///
    fn from_str(verbosity: &str) -> Result<Self, Self::Err> {
        let verbosity = match verbosity {
            "error" => Verbosity::Error,
            "warn"  => Verbosity::Warn,
            "info"  => Verbosity::Info,
            "debug" => Verbosity::Debug,
            "trace" => Verbosity::Trace,
            _       => Err(eyre!("Unknown verbosity level: {verbosity}"))
                        .suggestion(
                            format!(
                                "Please choose from: {:?}", 
                                Verbosity::iter().map(|v| v.to_string()).collect::<Vec<String>>()
                            ))?,
        };

        Ok(verbosity)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("Verbosity level {0} is unknown.")]
pub struct UnknownVerbosityError(pub String);

/// Light wrapper around datafusions register_csv function.
pub async fn register_csv<P,N>(path: &P, ctx: SessionContext, name: N, delimiter: Option<u8>) -> Result<SessionContext, Report>
where
    P: AsRef<Path> + std::fmt::Debug,
    N: Into<String>,
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

    // Configure out table reading options
    let read_options = CsvReadOptions::new().file_extension(&ext).delimiter(delimiter);
    let name: String = name.into();
    ctx.register_csv(&name, &path, read_options).await?;

    Ok(ctx)
}