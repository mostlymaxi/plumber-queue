use clap::Parser;
use server::QueueServer;

mod handlers;
mod syncer;
mod server;

/// the simplest queue around
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// length of the queue buffer - overflowing will cause old values to be dropped
    #[arg(short, long, default_value_t=server::QueueServer::DEFAULT_QUEUE_SIZE)]
    queue_size: usize,
    /// port to use to listen for consumers
    #[arg(short, long, default_value_t=server::QueueServer::DEFAULT_CONSUMER_PORT)]
    consumer_port: u16,
    /// port to use to listen for producers
    #[arg(short, long, default_value_t=server::QueueServer::DEFAULT_PRODUCER_PORT)]
    producer_port: u16,
    /// heartbeat interval in milliseconds for consumers
    #[arg(long, default_value_t=server::QueueServer::DEFAULT_HEARTBEAT_MS)]
    heartbeat: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    log::info!("building queue server with queue size {}...", args.queue_size);
    let qs = QueueServer::new_with_size(args.queue_size)
        .with_consumer_port(args.consumer_port)
        .with_producer_port(args.producer_port);

    qs.run().await;
}
