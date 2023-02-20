use futures::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use tokio::io;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Debug, Error)]
enum InternalError {
    #[error("framed reader received None instead of line")]
    FramedReaderError,
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
    size: u32,
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
    bytes_so_far: u32,
    bytes_since_last: u32,
}

impl Progress {
    fn new(oid: &str, bytes_so_far: u32, bytes_since_last: u32) -> Self {
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

async fn upload_file(request: Request) -> eyre::Result<()> {
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());

    // init progress
    writer
        .send(serde_json::to_string(&Event::Progress(Progress::new(
            &request.oid,
            0,
            0,
        )))?)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let stdin = io::stdin();
    let mut reader = FramedRead::new(stdin, LinesCodec::new());
    let stdout = io::stdout();
    let mut writer = FramedWrite::new(stdout, LinesCodec::new());

    // handle init event
    let init_line = reader
        .next()
        .await
        .transpose()?
        .ok_or_else(|| InternalError::FramedReaderError)?;
    if let Err(err) = serde_json::from_str::<Init>(&init_line) {
        writer
            .send(serde_json::to_string(&Error::new(32, err.to_string()))?)
            .await?;
        return Ok(());
    };
    writer.send("{}").await?;

    // main loop
    while let Some(line) = reader.next().await {
        let line = line?;
        let event: Event = serde_json::from_str(&line)?;

        match event {
            Event::Download(DownloadRequest { request }) => {
                writer
                    .send(serde_json::to_string(&Event::Complete(Complete::new(
                        &request.oid,
                        None,
                        Some(ErrorInner::new(
                            2,
                            "Agent does not support download".to_string(),
                        )),
                    )))?)
                    .await?;
            }
            Event::Upload(UploadRequest { request }) => {
                tokio::spawn(async { upload_file(request).await });
            }
            Event::Terminate => (),
            _ => eprintln!("Unexpected event received in main loop : {}", line),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_upload() {
        assert!(true);
    }
}
