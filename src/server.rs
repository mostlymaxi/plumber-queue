use std::fs;
use std::io::{self, BufWriter, Write};
use std::net::{TcpListener, SocketAddr, IpAddr, Ipv4Addr, TcpStream};
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
use std::{thread, sync::Arc, time::Duration};

use crate::handlers::{GeneralClient, Client, ProducerClient, ConsumerClient};
use crossbeam::channel::{Sender, Receiver};
use crossbeam::queue::ArrayQueue;

#[derive(Clone)]
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

pub struct QueueServer {
    addr_producer: SocketAddr,
    addr_consumer: SocketAddr,
    ringbuf: Arc<ArrayQueue<QueueMessage>>,
    running: Arc<AtomicBool>,
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
        let ringbuf: ArrayQueue<QueueMessage> = ArrayQueue::new(Self::DEFAULT_QUEUE_SIZE);
        let ringbuf = Arc::new(ringbuf);
        let running = Arc::new(AtomicBool::new(true));

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
            ringbuf,
            running,
            heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn new_with_size(n: usize) -> Self {
        let ringbuf: ArrayQueue<QueueMessage> = ArrayQueue::new(n);
        let ringbuf = Arc::new(ringbuf);
        let running = Arc::new(AtomicBool::new(true));

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
            ringbuf,
            running,
            heartbeat
        }
    }

    #[allow(dead_code)]
    pub fn with_size(mut self, n: usize) -> Self {
        let ringbuf: ArrayQueue<QueueMessage> = ArrayQueue::new(n);
        let ringbuf = Arc::new(ringbuf);

        self.ringbuf = ringbuf;
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

    fn client_handler<C>(
        listener: TcpListener,
        heartbeat: Duration,
        ringbuf: Arc<ArrayQueue<QueueMessage>>,
        sync_sender: Sender<QueueMessage>,
        sync_consumer: Arc<AtomicUsize>,
        running: Arc<AtomicBool>) where C: From<GeneralClient> + Client + Send + 'static {
        // this is necessary for timeouts and things
        listener.set_nonblocking(false).unwrap();

        while running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    if !running.load(Ordering::Relaxed) { break; }

                    let ringbuf_clone = ringbuf.clone();
                    let running_clone = running.clone();
                    let sync_sender_clone = sync_sender.clone();
                    let sync_consumer_clone = sync_consumer.clone();

                    let client = GeneralClient::new(
                        ringbuf_clone,
                        sync_sender_clone,
                        sync_consumer_clone,
                        stream,
                        heartbeat,
                        addr,
                        running_clone
                    );

                    let client = C::from(client);
                    let _ = thread::spawn(move || client.run());
                },
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    log::trace!("waiting...");
                    thread::sleep(Duration::from_secs(1));
                    continue;
                },
                Err(e) => { log::error!("connection failed: {:#?}", e) }
            }
            log::debug!("closing client handler");
        }
    }


    fn disk_syncer(queue_size: usize, sync_receiver: Receiver<QueueMessage>, sync_consumer: Arc<AtomicUsize>) {
        let mut current_page = CurrentPage::A;
        let f = fs::File::create("/tmp/test_sync_v2_A").unwrap();
        let mut f = BufWriter::new(f);
        let mut i: usize = 0;

        for qm in sync_receiver {
            if i == queue_size {
                f.flush().unwrap();
                f = match current_page {
                    CurrentPage::A => {
                        current_page = CurrentPage::B;
                        let tmp = fs::File::create("/tmp/test_sync_v2_B").unwrap();
                        BufWriter::new(tmp)
                    },
                    CurrentPage::B => {
                        current_page = CurrentPage::A;
                        let tmp = fs::File::create("/tmp/test_sync_v2_A").unwrap();
                        BufWriter::new(tmp)
                    },
                };
                i = 0;
            }

            f.write_all(qm.to_string().as_bytes()).unwrap();
            f.write_all(&[b'\n']).unwrap();
            f.write_all(sync_consumer
                .load(Ordering::Relaxed)
                .to_string()
                .as_bytes()
            ).unwrap();
            f.write_all(&[b'\n']).unwrap();
            i += 1;
        }

        f.flush().unwrap();
    }

    pub fn run(self) {
        log::debug!("starting queue server...");

        let running_clone = self.running.clone();
        ctrlc::set_handler(move || {
            running_clone.store(false, Ordering::Relaxed);

            let _ = TcpStream::connect_timeout(&self.addr_producer, Duration::from_secs(2));
            let _ = TcpStream::connect_timeout(&self.addr_consumer, Duration::from_secs(2));
        }).unwrap();

        let p_listener = TcpListener::bind(self.addr_producer).unwrap();
        let c_listener = TcpListener::bind(self.addr_consumer).unwrap();
        let heartbeat = Duration::from_millis(self.heartbeat);

        let (sync_sender, sync_receiver) = crossbeam::channel::bounded(1000);
        let sync_consumer = Arc::new(AtomicUsize::new(0));
        //let (sync_sender, sync_receiver) = crossbeam::channel::bounded(self.ringbuf.capacity());

        log::debug!("spawning producer handler");
        let ringbuf_clone = self.ringbuf.clone();
        let running_clone = self.running.clone();
        let sync_sender_clone = sync_sender.clone();
        let sync_consumer_clone = sync_consumer.clone();
        let p_thread = thread::spawn(move || QueueServer::client_handler::<ProducerClient>(
            p_listener,
            heartbeat,
            ringbuf_clone,
            sync_sender_clone,
            sync_consumer_clone,
            running_clone
        ));

        log::debug!("spawning consumer handler");
        let ringbuf_clone = self.ringbuf.clone();
        let running_clone = self.running.clone();
        let sync_sender_clone = sync_sender.clone();
        let sync_consumer_clone = sync_consumer.clone();
        let c_thread = thread::spawn(move || QueueServer::client_handler::<ConsumerClient>(
            c_listener,
            heartbeat,
            ringbuf_clone,
            sync_sender_clone,
            sync_consumer_clone,
            running_clone
        ));

        let queue_size = self.ringbuf.capacity();
        let s_thread = thread::spawn(move || Self::disk_syncer(
            queue_size, sync_receiver, sync_consumer)
        );
        drop(sync_sender);

        log::info!("ready to accept connections!");
        c_thread.join().unwrap();
        p_thread.join().unwrap();
        s_thread.join().unwrap();

    }

}
