mod l10n;

mod room;
pub use room::*;

mod server;
pub use server::*;

mod session;
pub use session::*;

use anyhow::Result;
use clap::Parser;
use std::{
    collections::{
        HashMap,
        hash_map::{Entry, VacantEntry},
    },
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    time::Duration,
};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{metadata::LevelFilter, warn};
use tracing_appender::non_blocking::WorkerGuard;
use uuid::Uuid;

pub type SafeMap<K, V> = RwLock<HashMap<K, V>>;
pub type IdMap<V> = SafeMap<Uuid, V>;

fn vacant_entry<V>(map: &mut HashMap<Uuid, V>) -> VacantEntry<'_, Uuid, V> {
    let mut id = Uuid::new_v4();
    while map.contains_key(&id) {
        id = Uuid::new_v4();
    }
    match map.entry(id) {
        Entry::Vacant(entry) => entry,
        _ => unreachable!(),
    }
}

/// `max_files == 0` keeps every rotated log file.
pub fn init_log(file: &str, level: LevelFilter, max_files: usize) -> Result<WorkerGuard> {
    use tracing::Level;
    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    use tracing_log::LogTracer;
    use tracing_subscriber::{EnvFilter, filter, fmt, prelude::*};

    let log_dir = Path::new("log");
    if log_dir.exists() {
        if !log_dir.is_dir() {
            panic!("log exists and is not a folder");
        }
    } else {
        std::fs::create_dir(log_dir).expect("failed to create log folder");
    }

    LogTracer::init()?;

    let mut appender = RollingFileAppender::builder()
        .rotation(Rotation::HOURLY)
        .filename_prefix(file);
    if max_files > 0 {
        appender = appender.max_log_files(max_files);
    }
    let (non_blocking, guard) = tracing_appender::non_blocking(appender.build(log_dir)?);

    let subscriber = tracing_subscriber::registry()
        .with(fmt::layer().with_writer(non_blocking).with_filter(level))
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_filter(EnvFilter::from_default_env()),
        )
        .with(
            filter::Targets::new()
                .with_target("hyper", Level::INFO)
                .with_target("rustls", Level::INFO)
                .with_default(Level::TRACE),
        );

    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");
    Ok(guard)
}

/// Command line arguments
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(
        short,
        long,
        default_value_t = 12346,
        help = "Specify the port number to use for the server"
    )]
    port: u16,

    #[clap(
        long,
        help = "Bind IP address (defaults to :: with fallback to 0.0.0.0)"
    )]
    bind: Option<IpAddr>,

    #[clap(
        long,
        default_value_t = LevelFilter::DEBUG,
        help = "Maximum level written to the log file (off, error, warn, info, debug, trace)"
    )]
    log_level: LevelFilter,

    #[clap(
        long,
        default_value_t = 168,
        help = "Number of hourly log files to keep (0 = keep all)"
    )]
    log_max_files: usize,
}

async fn bind_listener(bind: Option<IpAddr>, port: u16) -> Result<TcpListener> {
    if let Some(ip) = bind {
        return Ok(TcpListener::bind(SocketAddr::new(ip, port)).await?);
    }
    let v6 = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port);
    match TcpListener::bind(v6).await {
        Ok(listener) => Ok(listener),
        Err(err) => {
            let v4 = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
            warn!("failed to bind {v6} ({err}); falling back to {v4}");
            Ok(TcpListener::bind(v4).await?)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let _guard = init_log("phira-mp", args.log_level, args.log_max_files)?;
    let config = load_server_config(Path::new("server_config.yml"))?;

    let tcp_listener = bind_listener(args.bind, args.port).await?;
    println!("Local Address: {}", tcp_listener.local_addr()?);

    let server = Server::from_config(tcp_listener, config);

    loop {
        if let Err(err) = server.accept().await {
            warn!("failed to accept: {err:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_args_defaults_keep_previous_behaviour() {
        let args = Args::try_parse_from(["phira-mp-server"]).unwrap();
        assert_eq!(args.port, 12346);
        assert!(args.bind.is_none());
        assert_eq!(args.log_level, LevelFilter::DEBUG);
        assert_eq!(args.log_max_files, 168);
    }

    #[test]
    fn log_args_are_configurable() {
        let args = Args::try_parse_from([
            "phira-mp-server",
            "--bind",
            "127.0.0.1",
            "--log-level",
            "warn",
            "--log-max-files",
            "0",
        ])
        .unwrap();
        assert_eq!(args.bind, Some("127.0.0.1".parse().unwrap()));
        assert_eq!(args.log_level, LevelFilter::WARN);
        assert_eq!(args.log_max_files, 0);
    }

    #[test]
    fn invalid_log_level_is_rejected() {
        assert!(Args::try_parse_from(["phira-mp-server", "--log-level", "loud"]).is_err());
    }

    #[tokio::test]
    async fn bind_listener_binds_explicit_and_default_addresses() {
        let l1 = bind_listener(Some("127.0.0.1".parse().unwrap()), 0)
            .await
            .unwrap();
        assert_eq!(
            l1.local_addr().unwrap().ip(),
            "127.0.0.1".parse::<IpAddr>().unwrap()
        );
        drop(l1);

        let l2 = bind_listener(None, 0).await.unwrap();
        assert!(l2.local_addr().unwrap().port() > 0);
    }
}
