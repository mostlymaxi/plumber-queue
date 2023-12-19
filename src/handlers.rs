use std::{sync::{Arc, atomic::{AtomicBool, Ordering, AtomicUsize}}, net::SocketAddr, time::Duration, thread};
use tokio::{sync::{broadcast::error::RecvError}, io::AsyncWriteExt, select};
use tokio::sync::{broadcast, mpsc};
use tokio::net::TcpStream;
use tokio_util::codec::{LinesCodec, Framed};
use tokio_stream::{Stream, StreamExt, wrappers::{BroadcastStream, errors::BroadcastStreamRecvError}};
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
                    log::trace!("{}", qm.to_string());
                    stream.send(qm.get_msg()).await.unwrap();
                    self.consumer_sync_offset.store(qm.get_offset(), Ordering::Relaxed);
                }
                None = stream.next() => break,
            };
        }
    }
}

// trait KeepAlive {
//     fn keepalive(&self, heartbeat: Duration, alive: Arc<AtomicBool>);
// }

// impl KeepAlive for TcpStream {
//     fn keepalive(&self, heartbeat: Duration, alive: Arc<AtomicBool>) {
//         self.set_read_timeout(Some(heartbeat)).unwrap();
//         let stream = BufReader::new(self);
//         let mut stream = stream.lines();

//         while alive.load(Ordering::Relaxed) {
//             let Some(line) = stream.next() else {
//                 alive.store(false, Ordering::Relaxed);
//                 continue;
//             };

//             match line {
//                 Ok(line) => { log::trace!("{line}") },
//                 Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
//                     // alive.store(false, Ordering::Relaxed);
//                 },
//                 Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
//                     // alive.store(false, Ordering::Relaxed);
//                 },
//                 Err(_) => {
//                     // alive.store(false, Ordering::Relaxed);
//                 }
//             };
//         }
//     }
// }

// impl Client for ConsumerClient {
//     fn run(self) {
//         log::info!("({}) accepted new consumer client", self.addr);
//         let alive = Arc::new(AtomicBool::new(true));
//         let a = alive.clone();

//         let stream_clone = self.stream.try_clone().unwrap();
//         let heartbeat = self.heartbeat;
//         thread::spawn(move || stream_clone.keepalive(heartbeat, a));

//         let mut stream = BufWriter::new(&self.stream);


//         while self.running.load(Ordering::Relaxed) {
//             if !alive.load(Ordering::Relaxed) {
//                 log::warn!("({}) dead heartbeat", self.addr);
//                 break;
//             }

//             let qm = match self.ringbuf.pop() {
//                 Some(qm) => qm,
//                 None => {
//                     stream.flush().unwrap();
//                     log::trace!("({}) waiting for data...", self.addr);
//                     thread::sleep(Duration::from_millis(5));
//                     continue;
//                 }
//             };

//             let line = qm.get_msg();
//             log::trace!("{:#?}", line);

//             stream.write_all(line.as_bytes()).unwrap();
//             stream.write_all(&[b'\n']).unwrap();
//             self.sync_consumer.store(qm.get_offset(), Ordering::Relaxed);
//         }

//         log::info!("({}) closing consumer client", self.addr)
//     }
// }
