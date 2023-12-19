use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use tokio::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use std::io;
use crate::handlers::{ProducerClient, ConsumerClient};
use crate::syncer::{QueueSyncer, ConsumerOffsetSyncer};
use tokio::{signal, select};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct QueueMessage {
    offset: usize,
    msg: String,
}

#[derive(Debug)]
pub struct QueueMessageError;

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

    #[allow(dead_code)]
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
    channels: QueueChannels,
    stop_rx: watch::Receiver<()>,
    _heartbeat: u64
}

#[derive(Clone)]
pub struct QueueChannels {
    pub main_tx: flume::Sender<QueueMessage>,
    pub main_rx: flume::Receiver<QueueMessage>,
    pub producer_sync_tx: flume::Sender<QueueMessage>,
    pub producer_sync_rx: flume::Receiver<QueueMessage>,
    pub consumer_sync_offset: Arc<AtomicUsize>,
    pub producer_sync_offset: Arc<AtomicUsize>,
}

impl QueueChannels {
    fn new(n: usize) -> Self {
        let (main_tx, main_rx) = flume::bounded(n);
        let (producer_sync_tx, producer_sync_rx) = flume::bounded(n);
        let consumer_sync_offset = Arc::new(AtomicUsize::new(0));
        let producer_sync_offset = Arc::new(AtomicUsize::new(0));

        Self {
            main_tx,
            main_rx,
            producer_sync_tx,
            producer_sync_rx,
            consumer_sync_offset,
            producer_sync_offset,
        }
    }
}

impl QueueServer {
    pub const DEFAULT_QUEUE_SIZE: usize = 1_000_000;
    pub const DEFAULT_PRODUCER_PORT: u16 = 8084;
    pub const DEFAULT_CONSUMER_PORT: u16 = 8085;
    pub const DEFAULT_IPV4: (u8, u8, u8, u8) = (127, 0, 0, 1);
    pub const DEFAULT_HEARTBEAT_MS: u64 = 10_000;

    #[allow(dead_code)]
    pub fn new() -> Self {
        let channels = QueueChannels::new(Self::DEFAULT_QUEUE_SIZE);
        let (stop_tx, stop_rx) = watch::channel(());

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

        let _heartbeat = Self::DEFAULT_HEARTBEAT_MS;

        Self {
            addr_producer,
            addr_consumer,
            channels,
            stop_rx,
            _heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn new_with_size(n: usize) -> Self {
        let channels = QueueChannels::new(n);
        let (stop_tx, stop_rx) = watch::channel(());

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

        let _heartbeat = Self::DEFAULT_HEARTBEAT_MS;

        Self {
            addr_producer,
            addr_consumer,
            channels,
            stop_rx,
            _heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn with_size(mut self, n: usize) -> Self {
        let channels = QueueChannels::new(n);

        self.channels = channels;
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

    fn new_producer_client(&self, socket: TcpStream, addr: SocketAddr) -> ProducerClient {
        ProducerClient::new(
            self.channels.main_tx.clone(),
            self.channels.main_rx.clone(),
            self.channels.producer_sync_tx.clone(),
            self.channels.producer_sync_offset.clone(),
            socket,
            addr
        )
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

            log::info!("({}) accepted a producer client", &addr);

            let producer_client = self.new_producer_client(socket, addr);

            tokio::spawn(async move {
                producer_client.run().await;
                log::info!("({}) disconnected", &addr);
            });

        }
    }

    fn new_consumer_client(&self, socket: TcpStream, addr: SocketAddr) -> ConsumerClient {
        ConsumerClient::new(
            self.channels.main_rx.clone(),
            self.channels.consumer_sync_offset.clone(),
            socket,
            addr
        )
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

            log::info!("({}) accepted a consumer client", &addr);

            let consumer_client = self.new_consumer_client(socket, addr);

            tokio::spawn(async move {
                consumer_client.run().await;
                log::info!("({}) disconnected", &addr);
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

        let mut producer_sync = QueueSyncer::new(
            self.channels.main_tx.capacity().unwrap_or(Self::DEFAULT_QUEUE_SIZE),
            self.channels.producer_sync_rx.clone(),
            "/tmp/qtest".into()
        );
        let _producer_sync_task = tokio::spawn(async move {
            producer_sync.run().await;
        });

        let consumer_sync = ConsumerOffsetSyncer::new(
            self.channels.main_tx.capacity().unwrap_or(Self::DEFAULT_QUEUE_SIZE),
            self.channels.consumer_sync_offset.clone(),
            "/tmp/qtest".into()
        );
        let _consumer_sync_task = tokio::spawn(async move {
            consumer_sync.run().await;
        });

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
