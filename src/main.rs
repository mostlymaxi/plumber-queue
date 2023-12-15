use std::io::{Write, BufReader, BufRead, BufWriter, self};
use std::net::{TcpStream, TcpListener, SocketAddr, IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, sync::Arc, time::Duration};

use clients::{GeneralClient, Client, ProducerClient, ConsumerClient};
use crossbeam::queue::ArrayQueue;
mod clients;


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
