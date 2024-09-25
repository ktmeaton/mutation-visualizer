#![doc = include_str!("../README.md")]

pub mod cli;

#[doc(inline)]
pub use crate::cli::{Cli, Command};
