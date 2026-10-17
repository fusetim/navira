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
use std::{mem::drop, str::FromStr, time::Duration};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{debug, error, error_span, info, warn};
use tracing_log::LogTracer;

/// `navira-store` serves your static content HTTP (IPNI + Trustless Gateway)
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Config directory path for the navira-store server
    /// If not provided, it will use the default config directory (./config)
    #[arg(short, long, default_value = "./config")]
    config: PathBuf,

    /// Path to the directory containing the CAR files
    #[arg(short, long)]
    datastore: PathBuf,

    /// Save the combined config file to disk
    ///
    /// If any configuration values were provided via the environment variables, they will override
    /// the values in the config file. The resulting configuration will be saved to disk.
    #[arg(long)]
    save: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging();
    info!("hello, world!");

    info!("datastore path: {:?}", args.datastore);
    if !args.datastore.exists() {
        error!("datastore path does not exist");
        std::process::exit(1);
    }

    info!(
        "config path: {:?}",
        std::fs::canonicalize(&args.config).unwrap_or(args.config.clone())
    );
    let config_span = error_span!("config");
    let sg = config_span.enter();
    if !args.config.exists() {
        warn!("config directory does not exist, creating it");
        std::fs::create_dir_all(&args.config).expect("Failed to create config directory");
    }

    info!("loading config...");
    debug!(
        "note: if any configuration values were provided via the environment variables, they will override the values in the config file"
    );
    let config = load_config(&args.config);

    if args.save {
        // Save the config to disk and exit
        save_config(&args.config, &config).expect("Failed to save config");
        info!(
            "saved combined config to: {}",
            args.config.to_string_lossy()
        );
    }
    if let Err(err) = config.validate() {
        error!("invalid configuration: {:?}", err);
        std::process::exit(1);
    }
    drop(sg);

    let key_path = {
        let conf_path = PathBuf::from(&config.identity_key_path);
        if conf_path.is_relative() {
            args.config.join(&config.identity_key_path)
        } else {
            conf_path
        }
    };
    let identity_span = error_span!("identity");
    let sg = identity_span.enter();
    let identity = match load_peer_keys(&key_path) {
        Some(id) => {
            info!("loaded existing peer identity from file: {:?}", &key_path);
            id
        }
        None => {
            info!("no existing peer identity found, generating a new one");
            let id = Ed25519Identity::generate();
            if let Err(e) = save_peer_keys(&key_path, &id) {
                error!("failed to save peer identity: {:?}", e);
                std::process::exit(1);
            }
            info!("saved new peer identity to disk");
            id
        }
    };
    info!("peer ID: {}", identity.into_peerid().to_legacy());
    drop(sg);

    let addresses = config
        .annouce_address
        .into_iter()
        .map(|addr| format!("{}/p2p/{}", addr, identity.into_peerid().to_legacy()))
        .map(|addr| Multiaddr::from_str(&addr).expect("Invalid multiaddr"))
        .collect::<Vec<_>>();

    info!("HTTP server will listen on port: {}", config.http_port);

    let datastore_span = error_span!("datastore");
    let sg = datastore_span.enter();
    let mut store = DataStore::new();
    let Ok(count) = store.scan_directory(&args.datastore).await else {
        error!("Error scanning directory: {:?}", args.datastore);
        std::process::exit(1);
    };

    info!("Discovered and tracked {} CAR files", count);
    drop(sg);

    let index_span = error_span!("index");
    let sg = index_span.enter();
    let adcar_path = args.config.join("adcar.car");
    let mut index_provider = load_index(&adcar_path, &identity)
        .await
        .expect("Failed to load or create index provider");
    info!(
        "Opened index provider with head: {:?}",
        index_provider.head()
    );
    drop(sg);

    let sg = datastore_span.enter();
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
        Err(e) => error!("Error during indexing: {:?}", e),
    }
    drop(sg);

    // Announce them to the index provider
    let sg = index_span.enter();
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
    drop(sg);

    let index_provider = Arc::new(RwLock::new(index_provider));
    let store = Arc::new(RwLock::new(store));
    if let Err(err) = start_server(store.clone(), index_provider.clone(), config.http_port).await {
        error!("Error starting server: {:?}", err);
        std::process::exit(1);
    }
}

fn load_config(config_path: &Path) -> navira_store::Config {
    let settings = config::Config::builder()
        .add_source(config::File::from(config_path.join("config.toml")).required(false))
        .add_source(config::Environment::with_prefix("NAVIRA_STORE").separator("_"))
        .build()
        .expect("Failed to load configuration");
    settings
        .try_deserialize()
        .expect("Failed to deserialize configuration")
}

fn save_config(config_path: &Path, config: &navira_store::Config) -> std::io::Result<()> {
    let toml_string = toml::to_string(config).expect("Failed to serialize configuration");
    std::fs::write(config_path.join("config.toml"), toml_string)
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

fn load_peer_keys(key_path: &Path) -> Option<Ed25519Identity> {
    let raw = std::fs::read(&key_path).ok()?;
    Ed25519Identity::from_private_key(&raw).ok()
}

fn save_peer_keys(key_path: &Path, identity: &Ed25519Identity) -> std::io::Result<()> {
    let raw = identity.private_key();
    std::fs::write(&key_path, raw)
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
        error!("No valid multiaddrs provided for announcement. Skipping announcement.");
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
                        warn!(
                            "Failed to reach index provider at {}: HTTP {}",
                            &url,
                            response.status()
                        );
                    }
                }
                Err(e) => error!("Error reaching index provider at {}: {:?}", &url, e),
            }
        }
        if reachable {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !reachable {
        warn!("Could not reach any index provider after 10 attempts. Skipping announcement.");
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
                    error!("Failed to announce to {}: HTTP {}", url, response.status());
                }
            }
            Err(e) => error!("Error announcing to {}: {:?}", url, e),
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
