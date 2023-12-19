use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::SocketAddr;
use tokio::select;
use tokio::net::TcpStream;
use tokio_util::codec::{LinesCodec, Framed};
use tokio_stream::StreamExt;
use futures::sink::SinkExt;

use crate::server::QueueMessage;


pub struct ProducerClient {
    main_tx: flume::Sender<QueueMessage>,
    main_rx: flume::Receiver<QueueMessage>,
    producer_sync_tx: flume::Sender<QueueMessage>,
    producer_sync_offset: Arc<AtomicUsize>,
    stream: TcpStream,
    addr: SocketAddr,
}

impl ProducerClient {
    pub fn new(
            main_tx: flume::Sender<QueueMessage>,
            main_rx: flume::Receiver<QueueMessage>,
            producer_sync_tx: flume::Sender<QueueMessage>,
            producer_sync_offset: Arc<AtomicUsize>,
            stream: TcpStream,
            addr: SocketAddr) -> Self {
        Self {
            main_tx,
            main_rx,
            producer_sync_tx,
            producer_sync_offset,
            stream,
            addr
        }
    }

    pub async fn run(self) {
        let mut stream = Framed::new(
            self.stream, LinesCodec::new_with_max_length(2048)
        );

        while let Some(line) = stream.next().await {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                },
            };

            if self.main_tx.is_full() {
                let _ = self.main_rx.recv_async().await;
            }

            log::trace!("({})) {line}", self.addr);
            let qm = QueueMessage::new(
                self.producer_sync_offset.fetch_add(1, Ordering::Relaxed),
                line
            );
            self.main_tx.send_async(qm.clone()).await.unwrap();
            self.producer_sync_tx.send_async(qm).await.unwrap();
        }
    }
}

pub struct ConsumerClient {
    main_rx: flume::Receiver<QueueMessage>,
    consumer_sync_offset: Arc<AtomicUsize>,
    stream: TcpStream,
    addr: SocketAddr,
}

impl ConsumerClient {
    pub fn new(
            main_rx: flume::Receiver<QueueMessage>,
            consumer_sync_offset: Arc<AtomicUsize>,
            stream: TcpStream,
            addr: SocketAddr) -> Self {
        Self {
            main_rx,
            consumer_sync_offset,
            stream,
            addr
        }
    }

    pub async fn run(self) {
        let mut stream = Framed::new(
            self.stream, LinesCodec::new_with_max_length(2048)
        );

        let mut rx = self.main_rx.stream();
        loop {
            select! {
                biased;

                Some(qm) = rx.next() => {
                    log::trace!("({}) {}", self.addr, qm.to_string());
                    stream.send(qm.get_msg()).await.unwrap();
                    self.consumer_sync_offset.store(qm.get_offset(), Ordering::Relaxed);
                }
                None = stream.next() => break,
            };
        }
    }
}
