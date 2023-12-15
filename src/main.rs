use server::QueueServer;

mod handlers;
mod server;


fn main() {
    env_logger::init();

    let qs = QueueServer::new();
    qs.run();
}
