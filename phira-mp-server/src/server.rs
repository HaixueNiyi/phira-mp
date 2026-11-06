use crate::{IdMap, Room, SafeMap, Session, User, vacant_entry};
use anyhow::Result;
use phira_mp_common::RoomId;
use serde::Deserialize;
use std::{fs::File, sync::Arc};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tracing::{info, warn};
use uuid::Uuid;

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
    pub sessions: IdMap<Arc<Session>>,
    pub users: SafeMap<i32, Arc<User>>,

    pub rooms: SafeMap<RoomId, Arc<Room>>,

    pub lost_con_tx: mpsc::Sender<Uuid>,
}

pub struct Server {
    state: Arc<ServerState>,
    listener: TcpListener,
    lost_con_handle: JoinHandle<()>,
}

impl From<TcpListener> for Server {
    fn from(listener: TcpListener) -> Self {
        let (lost_con_tx, mut lost_con_rx) = mpsc::channel(16);
        let config: ServerConfig = File::open("server_config.yml")
            .ok()
            .and_then(|f| serde_yaml::from_reader(f).ok())
            .unwrap_or_default();
        let state = Arc::new(ServerState {
            config,
            sessions: IdMap::default(),
            users: SafeMap::default(),

            rooms: SafeMap::default(),

            lost_con_tx,
        });
        let lost_con_handle = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Some(id) = lost_con_rx.recv().await {
                    warn!("lost connection with {id}");
                    if let Some(session) = state.sessions.write().await.remove(&id)
                        && session
                            .user
                            .session
                            .read()
                            .await
                            .as_ref()
                            .is_some_and(|it| it.ptr_eq(&Arc::downgrade(&session)))
                    {
                        Arc::clone(&session.user).dangle().await;
                    }
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
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let id = {
                let mut guard = state.sessions.write().await;
                *vacant_entry(&mut guard).key()
            };
            match Session::new(id, stream, Arc::clone(&state)).await {
                Ok(session) => {
                    info!(
                        "received connections from {addr} ({}), version: {}",
                        session.id,
                        session.version()
                    );
                    state.sessions.write().await.insert(session.id, session);
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
}
