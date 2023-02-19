use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{FramedRead, LinesCodec};

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

#[derive(Debug, Deserialize, Serialize)]
struct Complete {
    oid: String,
    path: Option<String>,
    error: Option<ErrorInner>,
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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut reader = FramedRead::new(stdin, LinesCodec::new());
    if let Some(init) = reader.next().await.transpose()? {
        match serde_json::from_str::<Init>(&init) {
            Ok(init) => println!("{:?}", init),
            Err(err) => {
                stdout
                    .write_all(serde_json::to_string(&Error::new(32, err.to_string()))?.as_bytes())
                    .await?;
            }
        }
    }
    stdout.write_all("{}".as_bytes()).await?;
    while let Some(line) = reader.next().await {
        let event: Event = serde_json::from_str(&line?)?;
        match event {
            _ => println!("received event : {:?}", event),
        }
        let terminate = Event::Terminate;
        println!("{}", serde_json::to_string(&terminate)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_parsing() {
        assert!(true);
    }
}
