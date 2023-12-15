use std::io::{Write, BufReader, BufRead, BufWriter, self};
use std::net::{TcpStream, TcpListener, SocketAddr, IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, sync::Arc, time::Duration};

use crossbeam::queue::ArrayQueue;

trait Client: Sized {
    fn run(self) {}
}

struct GeneralClient {
    ringbuf: Arc<ArrayQueue<String>>,
    stream: TcpStream,
    heartbeat: Duration,
    addr: SocketAddr,
    running: Arc<AtomicBool>,
}

impl GeneralClient {
    fn new(ringbuf: Arc<ArrayQueue<String>>,
           stream: TcpStream,
           heartbeat: Duration,
           addr: SocketAddr,
           running: Arc<AtomicBool>,) -> Self {
        Self {
            ringbuf,
            stream,
            heartbeat,
            addr,
            running,
        }
    }
}

impl Client for GeneralClient {}

struct ProducerClient {
    ringbuf: Arc<ArrayQueue<String>>,
    stream: TcpStream,
    _heartbeat: Duration,
    addr: SocketAddr,
    running: Arc<AtomicBool>,
}

impl From<GeneralClient> for ProducerClient {
    fn from(c: GeneralClient) -> Self {
        Self {
            ringbuf: c.ringbuf,
            stream: c.stream,
            _heartbeat: c.heartbeat,
            addr: c.addr,
            running: c.running
        }
    }
}

impl Client for ProducerClient {
    fn run(self) {
        log::debug!("({}) accepted new producer client", self.addr);

        self.stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let stream = BufReader::new(&self.stream);
        let mut stream = stream.lines();

        while self.running.load(Ordering::Relaxed) {
            let Some(line) = stream.next() else { break };

            let line = match line {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    log::trace!("({}) waiting for data...", self.addr);
                    continue;
                },
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
                    log::trace!("({}) waiting for data...", self.addr);
                    continue;
                },
                Err(e) => {
                    log::error!("({}) connection failed: {:#?}", self.addr, e);
                    break;
                }
            };

            log::trace!("({}) {:#?}", self.addr, line);
            self.ringbuf.force_push(line);
        }

        log::debug!("({}) closing producer client", self.addr)
    }
}

struct ConsumerClient {
    ringbuf: Arc<ArrayQueue<String>>,
    stream: TcpStream,
    heartbeat: Duration,
    addr: SocketAddr,
    running: Arc<AtomicBool>,
}

impl Clone for ConsumerClient {
    fn clone(&self) -> Self {
        Self {
            ringbuf: self.ringbuf.clone(),
            stream: self.stream.try_clone().unwrap(),
            heartbeat: self.heartbeat.clone(),
            addr: self.addr.clone(),
            running: self.running.clone() }
    }
}

impl From<GeneralClient> for ConsumerClient {
    fn from(c: GeneralClient) -> Self {
        Self {
            ringbuf: c.ringbuf,
            stream: c.stream,
            heartbeat: c.heartbeat,
            addr: c.addr,
            running: c.running
        }
    }
}

trait KeepAlive {
    fn keepalive(&self, heartbeat: Duration, alive: Arc<AtomicBool>);
}

impl KeepAlive for TcpStream {
    fn keepalive(&self, heartbeat: Duration, alive: Arc<AtomicBool>) {
        self.set_read_timeout(Some(heartbeat)).unwrap();
        let stream = BufReader::new(self);
        let mut stream = stream.lines();

        while alive.load(Ordering::Relaxed) {
            let Some(line) = stream.next() else {
                alive.store(false, Ordering::Relaxed);
                continue;
            };

            match line {
                Ok(line) => { log::trace!("{line}") },
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    alive.store(false, Ordering::Relaxed);
                },
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
                    alive.store(false, Ordering::Relaxed);
                },
                Err(_) => {
                    alive.store(false, Ordering::Relaxed);
                }
            };
        }
    }
}

impl Client for ConsumerClient {
    fn run(self) {
        log::debug!("({}) accepted new consumer client", self.addr);
        let alive = Arc::new(AtomicBool::new(true));
        let a = alive.clone();

        let stream_clone = self.stream.try_clone().unwrap();
        let heartbeat = self.heartbeat.clone();
        thread::spawn(move || stream_clone.keepalive(heartbeat, a));

        let mut stream = BufWriter::new(&self.stream);


        while self.running.load(Ordering::Relaxed) {
            if !alive.load(Ordering::Relaxed) {
                log::warn!("({}) dead heartbeat", self.addr);
                break;
            }

            let line = match self.ringbuf.pop() {
                Some(l) => l,
                None => {
                    stream.flush().unwrap();
                    log::trace!("({}) waiting for data...", self.addr);
                    thread::sleep(Duration::from_millis(1000));
                    continue;
                }
            };

            log::trace!("{:#?}", line);
            stream.write_all(line.as_bytes()).unwrap();
            stream.write_all(&[b'\n']).unwrap();
        }

        log::debug!("({}) closing consumer client", self.addr)
    }
}

struct QueueServer {
    addr_producer: SocketAddr,
    addr_consumer: SocketAddr,
    ringbuf: Arc<ArrayQueue<String>>,
    running: Arc<AtomicBool>,
    heartbeat: u64
}

impl QueueServer {
    const DEFAULT_QUEUE_SIZE: usize = 100_000_000;
    const DEFAULT_PRODUCER_PORT: u16 = 8084;
    const DEFAULT_CONSUMER_PORT: u16 = 8085;
    const DEFAULT_IPV4: (u8, u8, u8, u8) = (127, 0, 0, 1);
    const DEFAULT_HEARTBEAT_MS: u64 = 10_000;

    pub fn new() -> Self {
        let ringbuf: ArrayQueue<String> = ArrayQueue::new(Self::DEFAULT_QUEUE_SIZE);
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
        let ringbuf: ArrayQueue<String> = ArrayQueue::new(n);
        let ringbuf = Arc::new(ringbuf);

        self.ringbuf = ringbuf;
        self
    }

    #[allow(dead_code)]
    pub fn with_producer_address(mut self, addr: &SocketAddr) -> Self {
        self.addr_producer = addr.clone();
        self
    }

    #[allow(dead_code)]
    pub fn with_consumer_address(mut self, addr: &SocketAddr) -> Self {
        self.addr_consumer = addr.clone();
        self
    }

    fn client_handler<C>(listener: TcpListener, heartbeat: Duration, ringbuf: Arc<ArrayQueue<String>>, running: Arc<AtomicBool>)
            where C: From<GeneralClient> + Client + Send + 'static {

        // this is necessary for timeouts and things
        listener.set_nonblocking(false).unwrap();

        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    let ringbuf_clone = ringbuf.clone();
                    let r = running.clone();

                    let client = GeneralClient::new(
                        ringbuf_clone,
                        stream,
                        heartbeat,
                        addr,
                        r
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
        }
    }

    fn run(self) {
        let p_listener = TcpListener::bind(&self.addr_producer).unwrap();
        let c_listener = TcpListener::bind(&self.addr_consumer).unwrap();
        let heartbeat = Duration::from_millis(self.heartbeat);

        let ringbuf_clone = self.ringbuf.clone();
        let running_clone = self.running.clone();
        let p_thread = thread::spawn(move || QueueServer::client_handler::<ProducerClient>(
            p_listener,
            heartbeat,
            ringbuf_clone,
            running_clone
        ));

        let ringbuf_clone = self.ringbuf.clone();
        let running_clone = self.running.clone();
        let c_thread = thread::spawn(move || QueueServer::client_handler::<ConsumerClient>(
            c_listener,
            heartbeat,
            ringbuf_clone,
            running_clone
        ));

        let _ = c_thread.join().unwrap();
        let _ = p_thread.join().unwrap();

    }

}


fn main() {
    env_logger::init();

    let qs = QueueServer::new();
    qs.run();
}
