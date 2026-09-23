use cid::Cid;
use clap::Parser;
use multiaddr::Multiaddr;
use navira_ipni::announce::Announce;
use navira_ipni::serde_json;
use navira_peer::identity::{IntoPeerId as _, ed25519::Ed25519Identity};
use navira_store::{
    datastore::DataStore,
    index::{self, IndexProvider},
    start_server,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use std::{str::FromStr, time::Duration};
use tokio::sync::RwLock;
use tracing::info;
use tracing_log::LogTracer;

/// `navira-store` serves your static content HTTP (IPNI + Trustless Gateway)
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the directory containing the CAR files
    #[arg(short, long)]
    datastore: PathBuf,

    /// HTTP port to listen on
    /// If not provided, it will use the default port (8080)
    #[arg(short, long)]
    port: Option<u16>,

    /// IP addresses to listen to
    /// If none are provided, it will listen on all interfaces (0.0.0.0, ::)
    #[arg(short, long)]
    bind: Vec<String>,

    /// Config directory path for the navira-store server
    /// If not provided, it will use the default config directory (./config)
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging();

    info!("Datastore path: {:?}", args.datastore);
    if !args.datastore.exists() {
        eprintln!("Datastore path does not exist: {:?}", args.datastore);
        std::process::exit(1);
    }

    let config = args
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from("./config"));
    info!("Config path: {:?}", args.config);
    if !config.exists() {
        std::fs::create_dir_all(&config).expect("Failed to create config directory");
        info!("Created config directory: {:?}", config);
    }

    let identity = match load_peer_keys(&config) {
        Some(id) => {
            info!("Loaded existing peer identity from config");
            id
        }
        None => {
            info!("No existing peer identity found, generating a new one");
            let id = Ed25519Identity::generate();
            if let Err(e) = save_peer_keys(&config, &id) {
                eprintln!("Failed to save peer identity: {:?}", e);
                std::process::exit(1);
            }
            info!("Saved new peer identity to config");
            id
        }
    };
    info!("Peer ID: {}", identity.into_peerid().to_legacy());

    let addresses = vec![
        "/dns6/navira-test.fusetim.space/tcp/443/https",
        "/dns/navira-test.fusetim.space/tcp/443/https",
    ];
    let addresses = addresses
        .into_iter()
        .map(|addr| format!("{}/p2p/{}", addr, identity.into_peerid().to_legacy()))
        .map(|addr| Multiaddr::from_str(&addr).expect("Invalid multiaddr"))
        .collect::<Vec<_>>();

    let port = args.port.unwrap_or(8080);
    info!("HTTP server will listen on port: {}", port);

    let mut store = DataStore::new();
    let Ok(count) = store.scan_directory(&args.datastore).await else {
        eprintln!("Error scanning directory: {:?}", args.datastore);
        std::process::exit(1);
    };

    info!("Discovered and tracked {} CAR files", count);

    let adcar_path = config.join("adcar.car");
    let mut index_provider = load_index(&adcar_path, &identity)
        .await
        .expect("Failed to load or create index provider");

    info!(
        "Opened index provider with head: {:?}",
        index_provider.head()
    );

    let mut hashes = Vec::new();

    match store.index().await {
        Ok(cids) => {
            info!("Indexing completed successfully, found {} CIDs", cids.len());
            for cid in cids {
                if let Ok(cid) = Cid::try_from(cid).and_then(|c| c.into_v1()) {
                    hashes.push(cid.hash().to_owned());
                }
            }
        }
        Err(e) => eprintln!("Error during indexing: {:?}", e),
    }

    // Announce them to the index provider
    let n = index_provider
        .advertise_hashes(&identity, &addresses, &hashes)
        .await
        .unwrap();
    info!("Successfully announced {n} CIDs to the index provider");

    // Announce to indexer via HTTP (launch the task in the background)
    let head = index_provider.head().unwrap().clone();
    let provider_addrs = addresses.clone();
    tokio::spawn(async move {
        announce_http(&head, provider_addrs).await;
    });

    let index_provider = Arc::new(RwLock::new(index_provider));
    let store = Arc::new(RwLock::new(store));
    if let Err(err) = start_server(store.clone(), index_provider.clone(), port).await {
        eprintln!("Error starting server: {:?}", err);
        std::process::exit(1);
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

    LogTracer::init().expect("Failed to set up log tracer");
}

fn load_peer_keys(config: &Path) -> Option<Ed25519Identity> {
    let path = config.join("navira.keys");

    let raw = std::fs::read(&path).ok()?;
    Ed25519Identity::from_private_key(&raw).ok()
}

fn save_peer_keys(config: &Path, identity: &Ed25519Identity) -> std::io::Result<()> {
    let path = config.join("navira.keys");
    let raw = identity.private_key();
    std::fs::write(&path, raw)
}

async fn load_index(
    adcar_path: &Path,
    identity: &Ed25519Identity,
) -> Result<IndexProvider, index::LoadError> {
    match tokio::fs::try_exists(adcar_path).await {
        Ok(true) => IndexProvider::open(adcar_path.to_owned(), identity).await,
        Ok(false) => IndexProvider::create(adcar_path.to_owned()).await,
        Err(e) => Err(index::LoadError::Io(e)),
    }
}

async fn announce_http(head: &Cid, addresses: Vec<Multiaddr>) {
    const ANNOUNCE_URLS: &[&str] = &["https://cid.contact"];
    let client = reqwest::Client::new();

    // Before advertising, check that at least one of the multiaddrs given is
    // reachable
    if addresses.is_empty() {
        eprintln!("No valid multiaddrs provided for announcement. Skipping announcement.");
        return;
    }
    let mut reachable = false;
    for _ in 0..10 {
        for addr in &addresses {
            let Some(base) = maddr_to_url(addr) else {
                continue;
            };
            let url = format!("{}/ipni/v1/ad/head", base);
            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        info!("Successfully reached index provider at {}", &url);
                        reachable = true;
                        break;
                    } else {
                        eprintln!(
                            "Failed to reach index provider at {}: HTTP {}",
                            &url,
                            response.status()
                        );
                    }
                }
                Err(e) => eprintln!("Error reaching index provider at {}: {:?}", &url, e),
            }
        }
        if reachable {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !reachable {
        eprintln!("Could not reach any index provider after 10 attempts. Skipping announcement.");
        return;
    }

    let announce = Announce::new(head.clone(), addresses, vec![], None);
    let announce_json = serde_json::to_string(&announce).unwrap();

    for url in ANNOUNCE_URLS {
        match client
            .put(format!("{}/announce", url))
            .header("User-Agent", "navira-store/0.1.0")
            .header("Content-Type", "application/json")
            .body(announce_json.clone())
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    info!("Successfully announced to {}", url);
                } else {
                    eprintln!("Failed to announce to {}: HTTP {}", url, response.status());
                }
            }
            Err(e) => eprintln!("Error announcing to {}: {:?}", url, e),
        }
    }
}

fn maddr_to_url(maddr: &Multiaddr) -> Option<String> {
    let mut iter = maddr.iter();
    let protocol = iter.next()?;
    let host = match protocol {
        multiaddr::Protocol::Dns(host) => host.to_string(),
        multiaddr::Protocol::Dns4(host) => host.to_string(),
        multiaddr::Protocol::Dns6(host) => host.to_string(),
        multiaddr::Protocol::Ip4(ip) => ip.to_string(),
        multiaddr::Protocol::Ip6(ip) => ip.to_string(),
        _ => return None,
    };
    let port = match iter.next() {
        Some(multiaddr::Protocol::Tcp(port)) => port,
        _ => return None,
    };
    let scheme = match iter.next() {
        Some(multiaddr::Protocol::Https) => "https",
        Some(multiaddr::Protocol::Http) => "http",
        _ => return None,
    };
    if (scheme == "https" && port == 443) || (scheme == "http" && port == 80) {
        return Some(format!("{}://{}", scheme, host));
    }
    Some(format!("{}://{}:{}", scheme, host, port))
}
