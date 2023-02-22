use futures::{stream::StreamExt, SinkExt};
use reqwest::header::CONTENT_LENGTH;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use thiserror::Error;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::{fs::OpenOptions, sync::Semaphore};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite, LinesCodec};

#[derive(Debug, Error)]
enum InternalError {
    #[error("framed reader received None instead of line")]
    FramedReaderError,
    #[error("chunk_size header is missing")]
    MissingChunkSizeHeader,
    #[error("etag header is missing")]
    MissingEtagHeader,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EtagWithPart {
    etag: String,
    part_number: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadCompletionPayload {
    oid: String,
    parts: Vec<EtagWithPart>,
}

impl UploadCompletionPayload {
    fn new(oid: &str, parts: Vec<EtagWithPart>) -> Self {
        Self {
            oid: oid.to_owned(),
            parts,
        }
    }
}

#[cfg(feature = "file_tracing")]
async fn create_log_file_writer() -> eyre::Result<FramedWrite<tokio::fs::File, LinesCodec>> {
    let mut options = OpenOptions::new();
    let file = options
        .create(true)
        .append(true)
        .open("/tmp/lfs-cta-rs.log")
        .await?;
    Ok(FramedWrite::new(file, LinesCodec::new()))
}

async fn upload_chunk(
    client: reqwest::Client,
    progress_tx: Sender<(String, u64)>,
    oid: String,
    url: String,
    path: String,
    file_size: u64,
    start: u64,
    chunk_size: u64,
    part_number: usize,
) -> eyre::Result<EtagWithPart> {
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;
    let bytes_transfered = std::cmp::min(file_size - start, chunk_size);

    file.seek(SeekFrom::Start(start as u64)).await?;
    let chunk = file.take(chunk_size);

    let response = client
        .put(url)
        .header(CONTENT_LENGTH, bytes_transfered)
        .body(reqwest::Body::wrap_stream(FramedRead::new(
            chunk,
            BytesCodec::new(),
        )))
        .send()
        .await?;
    let response = response.error_for_status()?;

    let etag_part = EtagWithPart {
        etag: response
            .headers()
            .get("etag")
            .ok_or(InternalError::MissingEtagHeader)?
            .to_str()?
            .to_owned()
            .replace("\\", "")
            .replace("\"", ""),
        part_number,
    };

    progress_tx.send((oid, bytes_transfered)).await?;
    Ok(etag_part)
}

async fn upload_file(mut request: Request, progress_tx: Sender<(String, u64)>) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());
    let client = reqwest::Client::new();

    #[cfg(feature = "file_tracing")]
    let mut log_file_writer = create_log_file_writer().await?;

    let mut handles = vec![];
    let semaphore = Arc::new(Semaphore::new(64));

    let chunk_size = request
        .action
        .header
        .remove("chunk_size")
        .ok_or(InternalError::MissingChunkSizeHeader)?
        .parse::<u64>()?;

    for (i, presigned_url) in &request.action.header {
        let progress_tx = progress_tx.clone();
        let oid = request.oid.clone();
        let url = presigned_url.to_string();
        let path = request.path.to_owned();
        let client = client.clone();
        let i = i.parse::<usize>()?;

        let start = (i as u64 - 1) * chunk_size;
        let permit = semaphore.clone().acquire_owned().await?;
        handles.push(tokio::spawn(async move {
            let chunk = upload_chunk(
                client,
                progress_tx,
                oid,
                url,
                path,
                request.size,
                start,
                chunk_size,
                i,
            )
            .await;
            drop(permit);
            chunk
        }));
    }

    let results: Vec<Result<eyre::Result<EtagWithPart>, tokio::task::JoinError>> =
        futures::future::join_all(handles).await;

    let results: eyre::Result<Vec<EtagWithPart>> =
        results
            .into_iter()
            .try_fold(vec![], |mut acc, res| match res {
                Ok(Ok(etag_part)) => {
                    acc.push(etag_part);
                    Ok(acc)
                }
                Ok(Err(err)) => Err(err),
                Err(err) => Err(err.into()),
            });

    match results {
        Ok(mut parts) => {
            parts.sort_by_key(|p| p.part_number);
            let res = client
                .post(request.action.href)
                .json(&UploadCompletionPayload::new(&request.oid, parts))
                .send()
                .await?;
            res.error_for_status()?;
            let complete =
                serde_json::to_string(&Event::Complete(Complete::new(&request.oid, None, None)))?;
            writer.send(&complete).await?;
            #[cfg(feature = "file_tracing")]
            log_file_writer.send(complete).await?;
        }
        Err(err) => {
            let upload_err = serde_json::to_string(&Event::Complete(Complete::new(
                &request.oid,
                None,
                Some(ErrorInner::new(32, err.to_string())),
            )))?;
            writer.send(&upload_err).await?;
            #[cfg(feature = "file_tracing")]
            log_file_writer.send(upload_err).await?;
        }
    }
    Ok(())
}

async fn send_progress_messages(mut progress_rx: Receiver<(String, u64)>) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());
    let mut bytes_written: HashMap<String, u64> = HashMap::new();

    #[cfg(feature = "file_tracing")]
    let mut file_writer = create_log_file_writer().await?;

    while let Some(bytes) = progress_rx.recv().await {
        let bytes_since_last = if let Some(bytes_since_last) = bytes_written.get(&bytes.0) {
            *bytes_since_last
        } else {
            0
        };
        let bytes_so_far = bytes_since_last + bytes.1;
        let progress_message = serde_json::to_string(&Event::Progress(Progress::new(
            &bytes.0,
            bytes_so_far,
            bytes_since_last,
        )))?;
        *bytes_written.entry(bytes.0).or_insert(bytes_since_last) = bytes_so_far;
        writer.send(&progress_message).await?;
        #[cfg(feature = "file_tracing")]
        file_writer.send(progress_message).await?;
    }

    Ok(())
}

async fn send_error_messages(mut error_rx: Receiver<(String, eyre::Report)>) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());

    #[cfg(feature = "file_tracing")]
    let mut file_writer = create_log_file_writer().await?;

    while let Some(error) = error_rx.recv().await {
        let complete_err = serde_json::to_string(&Event::Complete(Complete::new(
            &error.0,
            None,
            Some(ErrorInner::new(32, error.1.to_string())),
        )))?;
        writer.send(&complete_err).await?;
        #[cfg(feature = "file_tracing")]
        file_writer.send(complete_err).await?;
    }
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
        let mut file_writer = create_log_file_writer().await?;
        file_writer.send("------------").await?;
        file_writer
    };

    // handle init event
    let init_line = reader
        .next()
        .await
        .transpose()?
        .ok_or_else(|| InternalError::FramedReaderError)?;
    let init = match serde_json::from_str::<Init>(&init_line) {
        Ok(init) => init,
        Err(err) => {
            let err_msg = serde_json::to_string(&Error::new(32, err.to_string()))?;
            writer.send(&err_msg).await?;
            #[cfg(feature = "file_tracing")]
            file_writer.send(err_msg).await?;
            return Ok(());
        }
    };
    #[cfg(feature = "file_tracing")]
    file_writer.send(init_line).await?;
    writer.send("{}").await?;
    #[cfg(feature = "file_tracing")]
    file_writer.send("{}").await?;

    let (progress_tx, progress_rx) = mpsc::channel((init.concurrenttransfers * 64u32) as usize);
    tokio::spawn(send_progress_messages(progress_rx));

    let (error_tx, error_rx) = mpsc::channel(init.concurrenttransfers as usize);
    tokio::spawn(send_error_messages(error_rx));

    // main loop
    while let Some(line) = reader.next().await {
        let progress_tx = progress_tx.clone();
        let error_tx = error_tx.clone();
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
                tokio::spawn(async move {
                    let oid = request.oid.clone();
                    if let Err(err) = upload_file(request, progress_tx).await {
                        if let Err(_) = error_tx.send((oid, err)).await {
                            // rx channel closed
                        }
                    }
                });
            }
            Event::Terminate => (),
            _ => {
                #[cfg(feature = "file_tracing")]
                file_writer
                    .send(format!("Unexpected event received in main loop : {}", line))
                    .await?;
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
