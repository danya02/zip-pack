use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use pack_it_up::{
    Pack,
    online::{OnlinePacker, next_k_fit::NextKFitPacker},
};

use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use tokio::io::AsyncWriteExt;

pub mod args;

const PROGRESS_STYLE: &str = "{msg} {wide_bar} {pos}/{len} ..{eta}";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = args::MyArgs::parse();
    println!("{args:?}");
    if !args.input_path.is_dir() {
        println!(
            "Input path {:?} needs to be a directory -- it will be zipped",
            args.input_path
        );
        return;
    }
    if !args.output_path.is_dir() {
        println!(
            "Output path {:?} needs to be a directory -- we will create files there",
            args.output_path
        );
        return;
    }

    let multiprogress = indicatif::MultiProgress::new();
    let total_pb = Arc::new(Mutex::new(
        multiprogress.add(
            ProgressBar::new(0)
                .with_message("All files")
                .with_style(ProgressStyle::with_template(PROGRESS_STYLE).unwrap()),
        ),
    ));

    let (file_send, file_recv) = tokio::sync::mpsc::channel(100 * 1024);
    let (arc_send, arc_recv) = tokio::sync::mpsc::channel(100);
    tokio::spawn(traversal(
        args.input_path.clone(),
        file_send,
        args.input_path.clone(),
        total_pb.clone(),
    ));
    tokio::spawn(packer(file_recv, args.archive_size as usize, arc_send));

    archiver(
        arc_recv,
        args.archive_name_prefix
            .unwrap_or_else(|| "archive".to_string()),
        args.input_path,
        args.output_path,
        multiprogress,
        total_pb.clone(),
    )
    .await
    .unwrap();
}

#[derive(Debug)]
struct FileSpec {
    path: PathBuf,
    size: u64,
}

impl Pack for FileSpec {
    fn size(&self) -> usize {
        // According to https://stackoverflow.com/a/22347070/5936187, there is an overhead of
        // 30+16+46+(2*filename len) per file,
        // plus an extra 22 bytes per archive.
        let overhead = 30 + 16 + 46 + 2 * self.path.to_string_lossy().len();

        // To be safe, we estimate the overhead a bit higher, so that we don't end up with
        // an archive bigger than the limit.
        // TODO: figure out how small we can get this
        let overhead = overhead * 5;

        self.size as usize + overhead
    }
}

/// Scans the input path for files.
/// Evaluates the file size and sends it to the output channel.
/// The files sent are relative to `relative_root`.
#[async_recursion::async_recursion]
async fn traversal(
    path: PathBuf,
    output: tokio::sync::mpsc::Sender<FileSpec>,
    relative_root: PathBuf,
    total_pb: Arc<Mutex<ProgressBar>>,
) -> anyhow::Result<usize> {
    let mut total = 0;
    let is_base_case = path == relative_root;

    let mut children = tokio::fs::read_dir(&path).await?;
    while let Some(child) = children.next_entry().await? {
        let meta = child.metadata().await?;

        if meta.is_dir() {
            let count = traversal(
                child.path(),
                output.clone(),
                relative_root.clone(),
                total_pb.clone(),
            )
            .await?;
            total += count;
            if is_base_case {
                println!("Discovered {count} files in {:?}", child.path())
            }
        } else {
            let path = child.path();
            let relative_path = path.strip_prefix(relative_root.clone())?;

            output
                .send(FileSpec {
                    path: relative_path.to_path_buf(),
                    size: meta.len(),
                })
                .await?;
            total += 1;
            total_pb.lock().unwrap().inc_length(1);
        }
    }

    if is_base_case {
        println!("Discovered {total} total files in {path:?}");
    }

    Ok(total)
}

/// Performs bin-packing to produce archives whose size is at most `max_size`.
/// Returns the FileSpecs needed to produce the archives.
async fn packer(
    mut input: tokio::sync::mpsc::Receiver<FileSpec>,
    max_size: usize,
    output: tokio::sync::mpsc::Sender<Vec<FileSpec>>,
) {
    let mut packer = NextKFitPacker::new(2, max_size);
    while let Some(file) = input.recv().await {
        match packer.try_add(file) {
            Err(why) => match why {
                pack_it_up::online::online_packer::OnlinePackerError::ItemTooLarge(file) => {
                    eprintln!("File too large to be packed: {file:?}")
                }
            },
            Ok(bins) => {
                for bin in bins {
                    output.send(bin.into_contents()).await.unwrap();
                }
            }
        }
    }

    for bin in packer.finalize() {
        output.send(bin.into_contents()).await.unwrap();
    }
}

/// Creates archives from the list of FileSpecs
/// received from packing.
/// This is the function that can fail the most,
/// so it should be called from the main scope.
async fn archiver(
    mut input: tokio::sync::mpsc::Receiver<Vec<FileSpec>>,
    prefix: String,
    source_dir: PathBuf,
    destination: PathBuf,
    multiprogress: indicatif::MultiProgress,
    total_pb: Arc<Mutex<ProgressBar>>,
) -> anyhow::Result<()> {
    let mut counter = 1;
    while let Some(arc_files) = input.recv().await {
        let archive_path = destination.join(format!("{prefix}-{counter}.zip"));

        println!(
            "Generating archive {archive_path:?} with {} files",
            arc_files.len()
        );
        let progressbar = indicatif::ProgressBar::new(arc_files.len() as u64)
            .with_message("This archive")
            .with_style(ProgressStyle::with_template(PROGRESS_STYLE).unwrap());
        let progressbar = multiprogress.add(progressbar);

        counter += 1;
        let file = tokio::fs::File::create(archive_path).await?;
        let file = tokio::io::BufWriter::new(file);
        let mut writer = ZipFileWriter::with_tokio(file);

        for arc_file in arc_files {
            let arc_file_read = tokio::fs::read(source_dir.join(&arc_file.path)).await?;
            let entry = ZipEntryBuilder::new(
                arc_file
                    .path
                    .as_os_str()
                    .to_string_lossy()
                    .to_string()
                    .into(),
                Compression::Stored,
            )
            .build();

            writer.write_entry_whole(entry, &arc_file_read).await?;
            progressbar.inc(1);
            total_pb.lock().unwrap().inc(1);
        }

        let file = writer.close().await?;
        file.into_inner().flush().await?;

        progressbar.finish_and_clear();
    }

    Ok(())
}
