use std::path::PathBuf;
use std::io::{BufWriter, Write, Seek, SeekFrom};
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam::channel::Receiver;

use crate::server::QueueMessage;

pub enum CurrentSyncPage {
    A,
    B
}

pub struct QueueSyncer {
    queue_size: usize,
    sync_page: CurrentSyncPage,
    sync_producer: Receiver<QueueMessage>,
    sync_consumer: Arc<AtomicUsize>,
    path: PathBuf
}

impl QueueSyncer {
    pub fn new(queue_size: usize,
        sync_producer: Receiver<QueueMessage>,
        sync_consumer: Arc<AtomicUsize>,
        path: PathBuf) -> Self {

        let sync_page = CurrentSyncPage::A;

        Self {
            queue_size,
            sync_page,
            sync_producer,
            sync_consumer,
            path,
        }

    }

    pub fn run(&self) {
        let f = File::create("/tmp/test_sync_v3_A").unwrap();
        let mut f2 = File::create("/tmp/test_sync_v3_A").unwrap();
        let mut f = BufWriter::new(f);
        let mut i: usize = 0;

        let sync_producer = self.sync_producer.clone();
        let t1 = thread::spawn(move || {
            for qm in sync_producer {

            }
        });

        let sync_consumer = self.sync_consumer.clone();
        let padding = format!("{:X}", self.queue_size).len() + 1;
        let t2 = thread::spawn(move || {
            loop {
                let offset = sync_consumer.load(Ordering::Relaxed);
                let offset = format!("{:0p$X}", offset, p = padding);
                f2.write_all(offset.as_bytes()).unwrap();
                f2.seek(SeekFrom::Start(0)).unwrap();
                thread::sleep(Duration::from_secs(1));
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
