use std::path::PathBuf;

#[derive(Debug, clap::Parser)]
pub struct MyArgs {
    /// The path to be archived.
    pub input_path: PathBuf,

    /// The path to write the resulting archives to.
    pub output_path: PathBuf,

    /// The maximum size of a generated archive, in bytes.
    #[arg(short = 's', long)]
    pub archive_size: u64,

    /// What word to put in front of the names of the generated archives
    #[arg(short = 'n', long)]
    pub archive_name_prefix: Option<String>,
}
