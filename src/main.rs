use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use thiserror::Error;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{fs::OpenOptions, sync::Semaphore};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite, LinesCodec};

#[derive(Debug, Error)]
enum InternalError {
    #[error("failed to upload chunk, status code {0}")]
    ChunkUploadError(u16),
    #[error("framed reader received None instead of line")]
    FramedReaderError,
    #[error("chunk_size header is missing")]
    MissingChunkSizeHeader,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "event")]
#[serde(rename_all = "lowercase")]
enum Event {
    Init(Init),
    Download(DownloadRequest),
    Upload(UploadRequest),
    Progress(Progress),
    Complete(Complete),
    Terminate,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Download,
    Upload,
}

#[derive(Debug, Deserialize, Serialize)]
struct Init {
    operation: Operation,
    remote: String,
    concurrent: bool,
    concurrenttransfers: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct Action {
    href: String,
    header: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Request {
    oid: String,
    size: u64,
    path: String,
    action: Action,
}

#[derive(Debug, Deserialize, Serialize)]
struct DownloadRequest {
    #[serde(flatten)]
    request: Request,
}

#[derive(Debug, Deserialize, Serialize)]
struct UploadRequest {
    #[serde(flatten)]
    request: Request,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Progress {
    oid: String,
    bytes_so_far: u64,
    bytes_since_last: u64,
}

impl Progress {
    fn new(oid: &str, bytes_so_far: u64, bytes_since_last: u64) -> Self {
        Self {
            oid: oid.to_owned(),
            bytes_so_far,
            bytes_since_last,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Complete {
    oid: String,
    path: Option<String>,
    error: Option<ErrorInner>,
}

impl Complete {
    fn new(oid: &str, path: Option<String>, error: Option<ErrorInner>) -> Self {
        Self {
            oid: oid.to_owned(),
            path,
            error,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ErrorInner {
    code: u16,
    message: String,
}

impl ErrorInner {
    pub fn new(code: u16, message: String) -> Self {
        Self { code, message }
    }
}

#[derive(Debug, Serialize)]
struct Error {
    error: ErrorInner,
}

impl Error {
    fn new(code: u16, message: String) -> Self {
        Self {
            error: ErrorInner::new(code, message),
        }
    }
}

async fn upload_chunk(
    client: reqwest::Client,
    url: String,
    path: String,
    start: u64,
    stop: u64,
) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;

    file.seek(SeekFrom::Start(start as u64)).await?;
    let chunk = file.take(stop);
    let response = client
        .put(url)
        .body(reqwest::Body::wrap_stream(FramedRead::new(
            chunk,
            BytesCodec::new(),
        )))
        .send()
        .await?;
    let status_code = response.status().as_u16();
    if !(status_code >= 200 && status_code < 300) {
        return Err(InternalError::ChunkUploadError(status_code).into());
    }
    #[cfg(feature = "file_tracing")]
    let mut log_file_writer = {
        let mut options = OpenOptions::new();
        let log_file = options.append(true).open("/tmp/lfs-cta-rs.log").await?;
        let mut log_file_writer = FramedWrite::new(log_file, LinesCodec::new());
        log_file_writer
    };

    Ok(())
}

async fn upload_file(mut request: Request) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());
    let client = reqwest::Client::new();

    #[cfg(feature = "file_tracing")]
    let mut log_file_writer = {
        let mut options = OpenOptions::new();
        let log_file = options.append(true).open("/tmp/lfs-cta-rs.log").await?;
        let mut log_file_writer = FramedWrite::new(log_file, LinesCodec::new());
        log_file_writer
    };

    let init_progress = serde_json::to_string(&Event::Progress(Progress::new(&request.oid, 0, 0)))?;
    writer.send(&init_progress).await?;
    #[cfg(feature = "file_tracing")]
    log_file_writer.send(init_progress).await?;

    let mut handles = vec![];
    let semaphore = Arc::new(Semaphore::new(100));

    let chunk_size = request
        .action
        .header
        .remove("chunk_size")
        .ok_or(InternalError::MissingChunkSizeHeader)?
        .parse::<u64>()?;
    let presigned_urls: Vec<&String> = request.action.header.values().collect();

    for (i, presigned_url) in presigned_urls.iter().enumerate() {
        let path = request.path.to_owned();
        let client = client.clone();
        let url = presigned_url.to_string();

        let start = i as u64 * chunk_size;
        let stop = std::cmp::min(start + chunk_size - 1, request.size);
        let permit = semaphore.clone().acquire_owned().await?;
        handles.push(tokio::spawn(async move {
            let chunk = upload_chunk(client, url, path, start, stop).await;
            drop(permit);
            chunk
        }));
    }

    let results: Vec<Result<eyre::Result<()>, tokio::task::JoinError>> =
        futures::future::join_all(handles).await;
    let results: eyre::Result<()> = results.into_iter().flatten().collect();
    results?;

    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let stdin = io::stdin();
    let mut reader = FramedRead::new(stdin, LinesCodec::new());
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());

    #[cfg(feature = "file_tracing")]
    let mut file_writer = {
        let mut options = OpenOptions::new();
        let file = options
            .create(true)
            .append(true)
            .open("/tmp/lfs-cta-rs.log")
            .await?;
        let mut file_writer = FramedWrite::new(file, LinesCodec::new());
        file_writer.send("------------").await?;
        file_writer
    };

    // handle init event
    let init_line = reader
        .next()
        .await
        .transpose()?
        .ok_or_else(|| InternalError::FramedReaderError)?;
    if let Err(err) = serde_json::from_str::<Init>(&init_line) {
        let err_msg = serde_json::to_string(&Error::new(32, err.to_string()))?;
        writer.send(&err_msg).await?;
        #[cfg(feature = "file_tracing")]
        file_writer.send(err_msg).await?;
        return Ok(());
    } else {
        #[cfg(feature = "file_tracing")]
        file_writer.send(init_line).await?;
    }
    writer.send("{}").await?;
    #[cfg(feature = "file_tracing")]
    file_writer.send("{}").await?;

    // main loop
    while let Some(line) = reader.next().await {
        let line = line?;
        let event: Event = serde_json::from_str(&line)?;

        #[cfg(feature = "file_tracing")]
        file_writer.send(&line).await?;

        match event {
            Event::Download(DownloadRequest { request }) => {
                let complete = serde_json::to_string(&Event::Complete(Complete::new(
                    &request.oid,
                    None,
                    Some(ErrorInner::new(
                        2,
                        "Agent does not support download".to_string(),
                    )),
                )))?;
                writer.send(&complete).await?;
                #[cfg(feature = "file_tracing")]
                file_writer.send(complete).await?;
            }
            Event::Upload(UploadRequest { request }) => {
                tokio::spawn(async { upload_file(request).await });
            }
            Event::Terminate => (),
            _ => {
                #[cfg(feature = "file_tracing")]
                file_writer
                    .send(format!("Unexpected event received in main loop : {}", line))
                    .await?;
                eprintln!("Unexpected event received in main loop : {}", line)
            }
        }
    }

    #[cfg(feature = "file_tracing")]
    file_writer.send("------------").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_upload() {
        assert!(true);
    }
}
