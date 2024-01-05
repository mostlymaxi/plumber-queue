use futures::StreamExt;
use std::fs;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::watch;
use tokio::{select, time};

use flume::Receiver;

use crate::server::QueueMessage;

pub enum CurrentSyncPage {
    A,
    B,
}

impl ToString for CurrentSyncPage {
    fn to_string(&self) -> String {
        match self {
            CurrentSyncPage::A => "A".to_owned(),
            CurrentSyncPage::B => "B".to_owned(),
        }
    }
}

pub struct QueueSyncer {
    queue_size: usize,
    sync_page: CurrentSyncPage,
    producer_sync_rx: Receiver<QueueMessage>,
    stop_rx: watch::Receiver<()>,
    path: PathBuf,
}

pub struct ConsumerOffsetSyncer {
    queue_size: usize,
    consumer_sync_offset: Arc<AtomicUsize>,
    stop_rx: watch::Receiver<()>,
    path: PathBuf,
}

impl ConsumerOffsetSyncer {
    pub fn new(
        queue_size: usize,
        consumer_sync_offset: Arc<AtomicUsize>,
        stop_rx: watch::Receiver<()>,
        path: PathBuf,
    ) -> ConsumerOffsetSyncer {
        let path = path.join("qsync");
        fs::create_dir_all(&path).unwrap();

        Self {
            queue_size,
            consumer_sync_offset,
            stop_rx,
            path,
        }
    }

    pub async fn run(&mut self) {
        let file = File::create(self.path.join("consumer.offset"))
            .await
            .unwrap();
        let mut f = BufWriter::new(file);

        let padding = format!("{:X}", self.queue_size).len() + 1;

        loop {
            let offset = self.consumer_sync_offset.load(Ordering::Relaxed);
            let offset = format!("{:0p$X}", offset, p = padding);
            f.write_all(offset.as_bytes()).await.unwrap();
            f.seek(SeekFrom::Start(0)).await.unwrap();
            f.flush().await.unwrap();
            select! {
                _ = time::sleep(Duration::from_millis(500)) => {},
                _ = self.stop_rx.changed() => break,
            }
        }
        f.flush().await.unwrap();
    }
}

impl QueueSyncer {
    pub fn new(
        queue_size: usize,
        producer_sync_rx: Receiver<QueueMessage>,
        stop_rx: watch::Receiver<()>,
        path: PathBuf,
    ) -> Self {
        let sync_page = CurrentSyncPage::A;
        let path = path.join("qsync");
        fs::create_dir_all(&path).unwrap();

        Self {
            queue_size,
            sync_page,
            producer_sync_rx,
            stop_rx,
            path,
        }
    }

    pub async fn run(&mut self) {
        let file: File = File::options()
            .append(true)
            .create(true)
            .open(
                self.path
                    .join("producer")
                    .with_extension(self.sync_page.to_string()),
            )
            .await
            .unwrap();

        let mut f = BufWriter::new(file);

        let mut rx = self.producer_sync_rx.stream();
        let mut i: usize = 0;

        loop {
            select! {
                _ = self.stop_rx.changed() => break,
                _ = time::sleep(Duration::from_millis(500)) => f.flush().await.unwrap(),
                Some(qm) = rx.next() => {
                    if i >= self.queue_size {
                        match self.sync_page {
                            CurrentSyncPage::A => self.sync_page = CurrentSyncPage::B,
                            CurrentSyncPage::B => self.sync_page = CurrentSyncPage::A,
                        };

                        let file = File::create(
                            self.path.join("producer")
                            .with_extension(self.sync_page.to_string())
                        ).await.unwrap();

                        f = BufWriter::new(file);
                        i = 0;
                    }

                    f.write_all(qm.to_string().as_bytes()).await.unwrap();
                    f.write_all(&[b'\n']).await.unwrap();
                    i += 1;
                }
            }
        }
        f.flush().await.unwrap();
    }
}
