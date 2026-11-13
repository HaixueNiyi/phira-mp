use crate::{IdMap, Room, SafeMap, Session, User, vacant_id};
use anyhow::{Context, Result};
use phira_mp_common::RoomId;
use serde::Deserialize;
use std::{fs::File, io, path::Path, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use tracing::{info, warn};
use uuid::Uuid;

pub const MAX_UNAUTHENTICATED_CONNS: usize = 256;
pub const AUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
pub struct Chart {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub monitors: Vec<i32>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self { monitors: vec![2] }
    }
}

#[derive(Debug, Deserialize)]
pub struct Record {
    pub id: i32,
    pub player: i32,
    pub score: i32,
    pub perfect: i32,
    pub good: i32,
    pub bad: i32,
    pub miss: i32,
    pub max_combo: i32,
    pub accuracy: f32,
    pub full_combo: bool,
    pub std: f32,
    pub std_score: f32,
}

pub struct ServerState {
    pub config: ServerConfig,
    pub http_client: reqwest::Client,
    pub unauth_sem: Arc<Semaphore>,
    pub sessions: IdMap<Arc<Session>>,
    pub users: SafeMap<i32, Arc<User>>,

    pub rooms: SafeMap<RoomId, Arc<Room>>,

    pub lost_con_tx: mpsc::Sender<Uuid>,
}

pub struct Server {
    pub(crate) state: Arc<ServerState>,
    listener: TcpListener,
    lost_con_handle: JoinHandle<()>,
}

pub fn load_server_config(path: &Path) -> Result<ServerConfig> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            warn!("{} not found; using default configuration", path.display());
            return Ok(ServerConfig::default());
        }
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {}", path.display()));
        }
    };
    serde_yaml::from_reader(file).with_context(|| format!("failed to parse {}", path.display()))
}

impl From<TcpListener> for Server {
    fn from(listener: TcpListener) -> Self {
        let config =
            load_server_config(Path::new("server_config.yml")).expect("invalid server_config.yml");
        Self::from_config(listener, config)
    }
}

impl Server {
    pub fn from_config(listener: TcpListener, config: ServerConfig) -> Self {
        let (lost_con_tx, mut lost_con_rx) = mpsc::channel(256);
        let http_client = reqwest::Client::builder()
            .timeout(AUTH_HTTP_TIMEOUT)
            .connect_timeout(AUTH_HTTP_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");
        let state = Arc::new(ServerState {
            config,
            http_client,
            unauth_sem: Arc::new(Semaphore::new(MAX_UNAUTHENTICATED_CONNS)),
            sessions: IdMap::default(),
            users: SafeMap::default(),

            rooms: SafeMap::default(),

            lost_con_tx,
        });
        let lost_con_handle = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Some(id) = lost_con_rx.recv().await {
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        warn!("lost connection with {id}");
                        let session = state.sessions.write().await.remove(&id);
                        if let Some(session) = session {
                            session.mark_lost();
                            let is_current = session
                                .user
                                .session
                                .read()
                                .await
                                .as_ref()
                                .is_some_and(|it| it.ptr_eq(&Arc::downgrade(&session)));
                            if is_current {
                                Arc::clone(&session.user)
                                    .dangle_for_session(session.id)
                                    .await;
                            }
                        }
                    });
                }
            }
        });

        Self {
            listener,
            state,

            lost_con_handle,
        }
    }
}

impl Server {
    pub async fn accept(&self) -> Result<()> {
        let (stream, addr) = self.listener.accept().await?;
        let Ok(permit) = Arc::clone(&self.state.unauth_sem).try_acquire_owned() else {
            warn!("rejecting connection from {addr}: too many unauthenticated connections");
            return Ok(());
        };
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let _permit = permit;
            let id = {
                let guard = state.sessions.read().await;
                vacant_id(&guard)
            };
            match Session::new(id, stream, Arc::clone(&state)).await {
                Ok(session) => {
                    info!(
                        "received connections from {addr} ({}), version: {}",
                        session.id,
                        session.version()
                    );
                }
                Err(err) => {
                    warn!("failed to accept connection from {addr} ({id}): {err:?}");
                }
            }
        });
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.lost_con_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::SocketAddr, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::timeout,
    };

    /// Starts a `Server` on an ephemeral localhost port, driving `accept()` in a loop like `main`.
    async fn start_server() -> (SocketAddr, Arc<Server>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Arc::new(Server::from(listener));
        let server_clone = Arc::clone(&server);
        tokio::spawn(async move {
            loop {
                let _ = server_clone.accept().await;
            }
        });
        (addr, server)
    }

    /// Sends protocol version 1 + framed `ClientCommand::Ping` (`[1, 0]`) and expects
    /// framed `ServerCommand::Pong` (`[1, 0]`) within `wait`.
    async fn ping_roundtrip(addr: SocketAddr, wait: Duration) -> bool {
        let Ok(Ok(mut conn)) = timeout(wait, TcpStream::connect(addr)).await else {
            return false;
        };
        if conn.write_all(&[1, 1, 0]).await.is_err() {
            return false;
        }
        let mut resp = [0u8; 2];
        matches!(timeout(wait, conn.read_exact(&mut resp)).await, Ok(Ok(_)) if resp == [1, 0])
    }

    /// Reproduction test for C-1 (ported from `scratch/poc_accept_block.py`):
    /// an idle unauthenticated TCP connection (either before or after sending the version byte)
    /// must not block `accept()` or hold the global `sessions` write lock.
    #[tokio::test]
    async fn idle_unauthenticated_connection_does_not_block_accept() {
        let (addr, server) = start_server().await;

        // Attacker 1: connects and sends nothing at all (no version byte).
        let _attacker_no_version = TcpStream::connect(addr).await.unwrap();
        // Attacker 2: connects, sends only the version byte, and never authenticates.
        let mut attacker_no_auth = TcpStream::connect(addr).await.unwrap();
        attacker_no_auth.write_all(&[1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Subsequent client must still be accepted and served promptly.
        assert!(
            ping_roundtrip(addr, Duration::from_millis(500)).await,
            "victim client timed out waiting for Pong while idle connections were open"
        );

        // Global `sessions` write lock must not be held by unauthenticated connections.
        assert!(
            server.state.sessions.try_write().is_ok(),
            "sessions write lock is held while waiting for unauthenticated peer"
        );
    }

    #[tokio::test]
    async fn unauthenticated_connection_cap_rejects_excess_and_recovers() {
        let (addr, server) = start_server().await;

        // Exhaust all unauthenticated slots via the semaphore.
        let permits = Arc::clone(&server.state.unauth_sem)
            .acquire_many_owned(MAX_UNAUTHENTICATED_CONNS as u32)
            .await
            .unwrap();

        // Any new connection while at the cap must be rejected (dropped -> EOF).
        let mut excess = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let n = timeout(Duration::from_millis(500), excess.read(&mut buf))
            .await
            .expect("excess connection was not closed promptly")
            .unwrap();
        assert_eq!(n, 0, "expected immediate EOF when unauth cap is reached");

        // Releasing the permits allows subsequent clients to connect and be served.
        drop(permits);
        assert!(
            ping_roundtrip(addr, Duration::from_millis(500)).await,
            "client was not served after unauthenticated slots were freed"
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_protocol_version_promptly() {
        let (addr, _server) = start_server().await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        // Send unsupported protocol version 99.
        conn.write_all(&[99]).await.unwrap();
        let mut buf = [0u8; 1];
        let n = timeout(Duration::from_millis(500), conn.read(&mut buf))
            .await
            .expect("unsupported version connection was not closed promptly")
            .unwrap();
        assert_eq!(n, 0, "expected EOF for unsupported protocol version");
    }

    #[test]
    fn load_server_config_rejects_malformed_yaml_and_falls_back_on_missing() {
        let missing = std::env::temp_dir().join(format!("phira_mp_missing_{}.yml", Uuid::new_v4()));
        let cfg = load_server_config(&missing).unwrap();
        assert_eq!(cfg.monitors, vec![2]);

        let malformed = std::env::temp_dir().join(format!("phira_mp_bad_{}.yml", Uuid::new_v4()));
        std::fs::write(&malformed, "monitors: [not_an_int").unwrap();
        let res = load_server_config(&malformed);
        let _ = std::fs::remove_file(&malformed);
        assert!(
            res.is_err(),
            "malformed YAML must return Err instead of silently granting defaults"
        );
    }
}
