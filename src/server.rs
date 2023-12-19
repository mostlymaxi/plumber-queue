use std::fs;
use std::io::{self, BufWriter, Write};
use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use tokio::net::{TcpListener, TcpStream};
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
use std::{thread, sync::Arc, time::Duration};

use crate::handlers::{ProducerClient, ConsumerClient};
use tokio::{signal, select};
use tokio::sync::{watch, mpsc};
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct QueueMessage {
    offset: usize,
    msg: String,
}

#[derive(Debug)]
struct QueueMessageError;

impl QueueMessage {
    pub fn new(offset: usize, msg: String) -> QueueMessage {
        QueueMessage {
            offset,
            msg,
        }
    }

    pub fn get_msg(&self) -> String {
        self.msg.clone()
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn offset_from_str(&self, value: &str) -> Result<usize, QueueMessageError> {
        let value = value.trim();
        let mut value_iter = value.split(' ');
        let mut offset = value_iter.next().ok_or(QueueMessageError)?;

        offset = offset.trim_start_matches('[');
        offset = offset.trim_end_matches(']');

        Ok(usize::from_str_radix(offset, 16)
            .map_err(|_| QueueMessageError)?)
    }
}

impl ToString for QueueMessage {
    fn to_string(&self) -> String {
        format!("[{:X}] {}", self.offset, self.msg)
    }
}

#[derive(Clone)]
pub struct QueueServer {
    addr_producer: SocketAddr,
    addr_consumer: SocketAddr,
    tx: broadcast::Sender<QueueMessage>,
    stop_rx: watch::Receiver<()>,
    producer_offset: Arc<AtomicUsize>,
    heartbeat: u64
}

enum CurrentPage {
    A,
    B
}

// maybe send copy of data down channel that gets written to disk????
// idk
impl QueueServer {
    pub const DEFAULT_QUEUE_SIZE: usize = 1_000_000;
    pub const DEFAULT_PRODUCER_PORT: u16 = 8084;
    pub const DEFAULT_CONSUMER_PORT: u16 = 8085;
    pub const DEFAULT_IPV4: (u8, u8, u8, u8) = (127, 0, 0, 1);
    pub const DEFAULT_HEARTBEAT_MS: u64 = 10_000;

    #[allow(dead_code)]
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(Self::DEFAULT_QUEUE_SIZE);
        let (stop_tx, stop_rx) = watch::channel(());
        let producer_offset = Arc::new(AtomicUsize::new(0));

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        let (a, b, c, d) = Self::DEFAULT_IPV4;

        let addr_producer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_PRODUCER_PORT);
        let addr_consumer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_CONSUMER_PORT);

        let heartbeat = Self::DEFAULT_HEARTBEAT_MS;

        Self {
            addr_producer,
            addr_consumer,
            tx,
            stop_rx,
            producer_offset,
            heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn new_with_size(n: usize) -> Self {
        let (tx, _) = broadcast::channel(n);
        let (stop_tx, stop_rx) = watch::channel(());
        let producer_offset = Arc::new(AtomicUsize::new(0));

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        let (a, b, c, d) = Self::DEFAULT_IPV4;

        let addr_producer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_PRODUCER_PORT);
        let addr_consumer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_CONSUMER_PORT);

        let heartbeat = Self::DEFAULT_HEARTBEAT_MS;

        Self {
            addr_producer,
            addr_consumer,
            tx,
            stop_rx,
            producer_offset,
            heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn with_size(mut self, n: usize) -> Self {
        let (tx, _) = broadcast::channel(n);

        self.tx = tx;
        self
    }

    #[allow(dead_code)]
    pub fn with_producer_port(mut self, port: u16) -> Self {
        let (a, b, c, d) = Self::DEFAULT_IPV4;
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            port);

        self.addr_producer = addr;
        self
    }

    #[allow(dead_code)]
    pub fn with_consumer_port(mut self, port: u16) -> Self {
        let (a, b, c, d) = Self::DEFAULT_IPV4;
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            port);

        self.addr_consumer = addr;
        self
    }

    #[allow(dead_code)]
    pub fn with_producer_address(mut self, addr: &SocketAddr) -> Self {
        self.addr_producer = *addr;
        self
    }

    #[allow(dead_code)]
    pub fn with_consumer_address(mut self, addr: &SocketAddr) -> Self {
        self.addr_consumer = *addr;
        self
    }

    async fn producer_client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.addr_producer).await?;

        loop {
            let (socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            log::info!("({}) accepted a connection", &addr);

            let producer_client = ProducerClient::new(
                self.tx.clone(),
                self.producer_offset.clone(),
                socket,
                addr
            );

            tokio::spawn(async move {
                producer_client.run().await;
                log::info!("({}) disconnected", &addr);
            });

        }
    }

    async fn consumer_client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.addr_consumer).await?;

        loop {
            let (socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            log::info!("({}) accepted a connection", &addr);

            let consumer_client = ConsumerClient::new(
                self.tx.subscribe(),
                socket,
                addr
            );

            tokio::spawn(async move {
                consumer_client.run().await;
            });

        }
    }

    // fn disk_syncer(queue_size: usize, sync_receiver: Receiver<QueueMessage>, sync_consumer: Arc<AtomicUsize>) {
    //     let mut current_page = CurrentPage::A;
    //     let f = fs::File::create("/tmp/test_sync_v2_A").unwrap();
    //     let mut f = BufWriter::new(f);
    //     let mut i: usize = 0;

    //     for qm in sync_receiver {
    //         if i == queue_size {
    //             f.flush().unwrap();
    //             f = match current_page {
    //                 CurrentPage::A => {
    //                     current_page = CurrentPage::B;
    //                     let tmp = fs::File::create("/tmp/test_sync_v2_B").unwrap();
    //                     BufWriter::new(tmp)
    //                 },
    //                 CurrentPage::B => {
    //                     current_page = CurrentPage::A;
    //                     let tmp = fs::File::create("/tmp/test_sync_v2_A").unwrap();
    //                     BufWriter::new(tmp)
    //                 },
    //             };
    //             i = 0;
    //         }

    //         f.write_all(qm.to_string().as_bytes()).unwrap();
    //         f.write_all(&[b'\n']).unwrap();
    //         f.write_all(sync_consumer
    //             .load(Ordering::Relaxed)
    //             .to_string()
    //             .as_bytes()
    //         ).unwrap();
    //         f.write_all(&[b'\n']).unwrap();
    //         i += 1;
    //     }

    //     f.flush().unwrap();
    // }

    pub async fn run(self) {
        log::debug!("starting queue server...");

        let sync_rx = self.tx.subscribe();
        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone  = self.clone();
        let producer_task = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.producer_client_handler() => {}
            }
        });


        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone  = self.clone();
        let consumer_task = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.consumer_client_handler() => {}
            }
        });

        log::info!("queue server ready");
        producer_task.await.unwrap();
        consumer_task.await.unwrap();

    }

}
