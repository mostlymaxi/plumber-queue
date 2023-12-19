use std::path::PathBuf;
use std::io::{Write, Seek, SeekFrom};
use futures::StreamExt;
use tokio::io::{BufWriter, AsyncWriteExt, AsyncSeekExt};
use tokio::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{thread, fs};
use tokio::time;
use std::time::Duration;

use flume::Receiver;

use crate::server::QueueMessage;

pub enum CurrentSyncPage {
    A,
    B
}

pub struct QueueSyncer {
    queue_size: usize,
    sync_page: CurrentSyncPage,
    producer_sync_rx: Receiver<QueueMessage>,
    path: PathBuf
}

pub struct ConsumerOffsetSyncer {
    queue_size: usize,
    consumer_sync_offset: Arc<AtomicUsize>,
    path: PathBuf
}

impl ConsumerOffsetSyncer {
    pub fn new(queue_size: usize, consumer_sync_offset: Arc<AtomicUsize>, path: PathBuf) -> ConsumerOffsetSyncer {
        let path = path.join("qsync");
        fs::create_dir_all(&path).unwrap();

        Self {
            queue_size,
            consumer_sync_offset,
            path,
        }
    }

    pub async fn run(&self) {
        let file = File::create(self.path.join("consumer.offset")).await.unwrap();
        let mut f = BufWriter::new(file);

        let padding = format!("{:X}", self.queue_size).len() + 1;

        loop {
            let offset = self.consumer_sync_offset.load(Ordering::Relaxed);
            let offset = format!("{:0p$X}", offset, p = padding);
            f.write_all(offset.as_bytes()).await.unwrap();
            f.seek(SeekFrom::Start(0)).await.unwrap();
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl QueueSyncer {
    pub fn new(
            queue_size: usize,
            producer_sync_rx: Receiver<QueueMessage>,
            path: PathBuf) -> Self {

        let sync_page = CurrentSyncPage::A;
        let path = path.join("qsync");
        fs::create_dir_all(&path).unwrap();

        Self {
            queue_size,
            sync_page,
            producer_sync_rx,
            path
        }

    }

    pub async fn run(&mut self) {
        let file: File = File::create(self.path.join("producer.A")).await.unwrap();
        let mut f = BufWriter::new(file);

        let mut rx = self.producer_sync_rx.stream();
        let mut i: usize = 0;

        while let Some(qm) = rx.next().await {
            if i >= self.queue_size {
                let file_name = match self.sync_page {
                    CurrentSyncPage::A => {
                        self.sync_page = CurrentSyncPage::B;
                        "producer.B"
                    },
                    CurrentSyncPage::B => {
                        self.sync_page = CurrentSyncPage::A;
                        "producer.A"
                    },
                };
                let file = File::create(self.path.join(file_name)).await.unwrap();
                f = BufWriter::new(file);
                i = 0;
            }

            f.write_all(qm.to_string().as_bytes()).await.unwrap();
            f.write_all(&[b'\n']).await.unwrap();
            i += 1;
        }
        f.flush().await.unwrap();
    }
}
