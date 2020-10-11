//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `mini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.

use lrucache::{server, DEFAULT_PORT};

use clap::Clap;
use tokio::net::TcpListener;
use tokio::signal;

pub fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // enable logging
    // see https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::parse();
    let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);
    let root = cli.root_path.as_deref().unwrap_or("");
    let mut use_basic = false;
    let num_cpus = num_cpus::get();
    let core_threads = cli.worker_threads.unwrap_or(num_cpus);
    if core_threads == 1 {
        use_basic = true;
    }
    let max_threads = cli.max_threads.unwrap_or(32768);
    if max_threads < core_threads {
        return Err(format!(
            "max_threads({}) cannot be less than worker_threads({})",
            max_threads, core_threads
        )
        .into());
    }
    let max_connections = cli.max_conns;
    let mut rt = if use_basic {
        tokio::runtime::Builder::new()
            .enable_all()
            .basic_scheduler()
            .max_threads(max_threads)
            .build()?
    } else {
        tokio::runtime::Builder::new()
            .enable_all()
            .threaded_scheduler()
            .max_threads(max_threads)
            .core_threads(core_threads)
            .build()?
    };

    rt.block_on(async {
        // Bind a TCP listener
        let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;

        server::run(
            listener,
            signal::ctrl_c(),
            cli.capacity,
            root,
            max_connections,
        )
        .await
    })
}

#[derive(Clap, Debug)]
#[clap(name = "cache-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A cache server that accepts a subnet of Redis protocol")]
struct Cli {
    #[clap(short, long)]
    port: Option<String>,
    #[clap(short, long)]
    capacity: Option<u64>,
    #[clap(short, long)]
    root_path: Option<String>,
    #[clap(short, long)]
    worker_threads: Option<usize>,
    #[clap(short, long)]
    max_threads: Option<usize>,
    /// Maximum number of concurrent connections the redis server will accept.
    ///
    /// When this limit is reached, the server will stop accepting connections until
    /// an active connection terminates.
    #[clap(long, default_value = "250")]
    max_conns: usize,
}
