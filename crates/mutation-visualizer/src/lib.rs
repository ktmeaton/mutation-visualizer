use color_eyre::eyre::{eyre, Report, Result};
use color_eyre::Help;
use clap::ValueEnum;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use strum_macros::EnumIter;
use strum::IntoEnumIterator;
use thiserror::Error;

pub mod annotate;

#[doc(inline)]
pub use crate::annotate::annotate;
pub use crate::annotate::AnnotateArgs;

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
