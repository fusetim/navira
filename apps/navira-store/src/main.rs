use clap::Parser;
use std::path::PathBuf;
use tracing::info;

/// `navira-store` serves your static content over /ipfs/bitswap
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the directory containing the CAR files
    #[arg(short, long)]
    datastore: PathBuf,

    /// Unix socket path to listen on
    /// If not provided, it will not listen on a Unix socket
    #[arg(short, long)]
    socket: Option<PathBuf>,

    /// UDP port to listen for Bitswap connections
    /// Default: 4001
    #[arg(short, long, default_value_t = 4001)]
    port: u16,

    /// UDP address to bind to for Bitswap connections
    /// Default: 0.0.0.0 (all interfaces)
    ///
    /// Important: UDP socket is disabled when a Unix socket is provided
    #[arg(short, long, default_value = "0.0.0.0")]
    address: String,
}

fn main() {
    let args = Args::parse();
    setup_logging();

    info!("Datastore path: {:?}", args.datastore);
    if let Some(socket_path) = args.socket {
        info!("Listening on Unix socket: {:?}", socket_path);
    } else {
        info!("Listening on UDP {}:{}", args.address, args.port);
    }
}

fn setup_logging() {
    use tracing_subscriber::FmtSubscriber;

    const DEFAULT_LOGGING: &str = "navira_store=info,warn,debug";

    let rust_log = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| if s.is_empty() { None } else { Some(s) })
        .unwrap_or_else(|| DEFAULT_LOGGING.to_owned());

    tracing::subscriber::set_global_default(
        FmtSubscriber::builder().with_env_filter(rust_log).finish(),
    )
    .expect("tracing setup failed");
}
