use async_trait::async_trait;
use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use hyper::{
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body,
    Request,
    Response,
    Server,
};
use log::{Level, Log};
use std::{cell::Cell, convert::Infallible, io::Stdout, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{oneshot::Sender, Mutex},
    time::sleep,
};

use crate::{
    connection::{init_pool_maybe, Pool},
    logger::{configure_logger, create_trace_id, json::Logger, with_trace_id, NewJsonLogger},
};

#[cfg(unix)]
fn setup_signal_handler(rt: &Runtime, tx: Sender<()>) {
    use tokio::signal::unix::{signal, SignalKind};

    let tx_mut = Arc::new(Mutex::new(Cell::new(Some(tx))));
    for signal_kind in [SignalKind::terminate(), SignalKind::interrupt()] {
        let tx_mut_clone = tx_mut.clone();
        rt.spawn(async move {
            let _ = signal(signal_kind).unwrap().recv().await;
            warn!("Received signal {:?}, starting shutdown", signal_kind);
            let lck = tx_mut_clone.lock().await;
            if let Some(tx) = lck.take() {
                let _ = tx.send(());
            }
        });
    }
}

#[cfg(windows)]
fn setup_signal_handler(rt: &Runtime, tx: Sender<()>) {
    use tokio::signal::ctrl_c;

    rt.spawn(async move {
        let _ = ctrl_c().await;
        warn!("Received signal {:?}, starting shutdown", signal_kind);
        let _ = tx.send(());
    });
}

/// A ServerHandler handles requests of any kind, this is the main entry point for each request your
/// server receives.
#[async_trait]
pub trait ServerHandler: Sync + Send {
    /// Handle a single request and produce a response.
    async fn handle(&self, req: Request<Body>) -> Response<Body>;
}

/// Run a HTTP server on the given port with the given `ServerHandler`. The constructed `ServerHandler`
/// is called for every request and has to produce a response.
///
/// This function is intended to serve as your main function and therefore also sets up logging and
/// shuts down the server after receiving a SIGTERM or SIGINT.
pub fn run<F: FnOnce(Pool) -> S, S: ServerHandler + 'static>(mk_service: F, port: u16) {
    static LOGGER: Lazy<Logger<Stdout>, NewJsonLogger> = Lazy::new(NewJsonLogger::new(Level::Info));

    dotenv().ok();
    configure_logger(&*LOGGER);

    let (pool, pool_size) = init_pool_maybe().expect("Failed to initialize database pool");
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(pool_size as usize)
        .build()
        .unwrap();
    let service = Arc::new(mk_service(pool));

    rt.spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
            LOGGER.flush();
        }
    });

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    setup_signal_handler(&rt, tx);

    rt.block_on(async {
        // Setup and configure server...
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let make_service = make_service_fn(move |conn: &AddrStream| {
            let remote_addr = conn.remote_addr();
            info!("New connection from {}", remote_addr);
            let conn_service = Arc::clone(&service);
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let req_service = Arc::clone(&conn_service);
                    let id = create_trace_id(&req);
                    async move { Ok::<_, Infallible>(with_trace_id(id, req_service.handle(req)).await) }
                }))
            }
        });

        let server = Server::bind(&addr).http1_keepalive(true).serve(make_service);

        info!("Started server on {} with a pool of size {}", addr, pool_size);

        let graceful = server.with_graceful_shutdown(async {
            rx.await.ok();
        });

        // Run the server until we are told to shutdown
        if let Err(e) = graceful.await {
            error!("Server terminated with error: {}", e);
        } else {
            info!("Completed server shutdown");
        }

        LOGGER.flush();
    });
}
