use std::{sync::{Arc, atomic::{AtomicBool, Ordering, AtomicUsize}}, net::SocketAddr, time::Duration, thread};
use tokio::{sync::{broadcast::error::RecvError}, io::AsyncWriteExt, select};
use tokio::sync::{broadcast, mpsc};
use tokio::net::TcpStream;
use tokio_util::codec::{LinesCodec, Framed};
use tokio_stream::{Stream, StreamExt, wrappers::{BroadcastStream, errors::BroadcastStreamRecvError}};
use futures::sink::SinkExt;

use crate::server::QueueMessage;


pub struct ProducerClient {
    tx: flume::Sender<QueueMessage>,
    rx: flume::Receiver<QueueMessage>,
    // tx_sync: mpsc::Sender<QueueMessage>,
    offset: Arc<AtomicUsize>,
    stream: TcpStream,
    addr: SocketAddr,
}

impl ProducerClient {
    pub fn new(tx: flume::Sender<QueueMessage>,
            rx: flume::Receiver<QueueMessage>,
            // tx_sync: mpsc::Sender<QueueMessage>,
            offset: Arc<AtomicUsize>,
            stream: TcpStream,
            addr: SocketAddr) -> Self {
        Self {
            tx,
            rx,
            // tx_sync,
            offset,
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

            if self.tx.is_full() {
                let _ = self.rx.recv_async().await;
            }

            log::trace!("({})) {line}", self.addr);
            let qm = QueueMessage::new(
                self.offset.fetch_add(1, Ordering::Relaxed),
                line
            );
            self.tx.send_async(qm).await.unwrap();
        }
    }
}

pub struct ConsumerClient {
    rx: flume::Receiver<QueueMessage>,
    stream: TcpStream,
    addr: SocketAddr,
}

impl ConsumerClient {
    pub fn new(rx: flume::Receiver<QueueMessage>, stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            rx,
            stream,
            addr
        }
    }

    pub async fn run(self) {
        let mut stream = Framed::new(
            self.stream, LinesCodec::new_with_max_length(2048)
        );

        let mut rx = self.rx.stream();
        loop {
            select! {
                biased;

                Some(qm) = rx.next() => {
                    log::trace!("{}", qm.to_string());
                    stream.send(qm.get_msg()).await.unwrap();
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
