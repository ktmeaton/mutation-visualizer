use clap::{Parser, Subcommand };
use mutation_heatmap::{AnnotateArgs, PlotArgs, Verbosity};
use serde::{Deserialize, Serialize};

/// The command-line interface (CLI).
/// ---
/// The CLI is intended for parsing user input from the command-line in the main function.
/// This is achieved with the `parse` function, which parses the command line arguments from [`std::env::args`](https://doc.rust-lang.org/stable/std/env/fn.args.html).
/// ```no_run
/// use clap::Parser;
/// let args = mutation_visualizer_cli::Cli::parse();
/// ```
#[derive(Debug, Deserialize, Parser, Serialize)]
#[clap(name = "mutation-visualizer", author, version)]
#[clap(about = "This is the about message.")]
#[clap(after_help = "This is long message after help.")]
#[clap(trailing_var_arg = true)]
#[clap(arg_required_else_help = true)]
pub struct Cli {

    /// Pass CLI arguments to a particular [Command].
    #[clap(subcommand)]
    #[clap(help = "Choose a command.")]
    pub command: Command,

    /// Set the logging [`Verbosity`] level.
    #[clap(help = "Set the logging verbosity level.")]
    #[clap(short = 'v', long)]
    #[clap(hide_possible_values = false)]
    #[clap(value_enum, default_value_t = Verbosity::default())]
    #[clap(global = true)]
    pub verbosity: Verbosity,
}

/// CLI [commands](#variants). Used to decide which runtime [Command](#variants) the CLI arguments should be passed to.
#[derive(Debug, Deserialize, Serialize, Subcommand)]
#[clap(arg_required_else_help = true)]
pub enum Command {
    /// Pass CLI arguments to the [Dataset](dataset::Command) subcommands.
    /// ## Examples
    /// ```rust
    /// use rebar::{Cli, cli::Command};
    /// use clap::Parser;
    /// let input = ["rebar", "dataset", "--help"];
    /// let args = Cli::parse_from(input);
    /// matches!(args.command, Command::Dataset(_));
    /// ```
    #[clap(about = "Annotate mutations.")]
    Annotate(AnnotateArgs),

    #[clap(about = "Plot mutations.")]
    Plot(PlotArgs),
}
