use crate::{Chart, Record, User};
use anyhow::{Result, bail};
use phira_mp_common::{ClientRoomState, Message, RoomId, RoomState, ServerCommand};
use rand::seq::IndexedRandom;
use std::{
    collections::{HashMap, HashSet},
    ops::DerefMut,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::RwLock;
use tracing::{debug, info};

const ROOM_MAX_USERS: usize = 8;

#[derive(Default, Debug)]
pub enum InternalRoomState {
    #[default]
    SelectChart,
    WaitForReady {
        started: HashSet<i32>,
    },
    Playing {
        results: HashMap<i32, Record>,
        aborted: HashSet<i32>,
    },
}

impl InternalRoomState {
    pub fn to_client(&self, chart: Option<i32>) -> RoomState {
        match self {
            Self::SelectChart => RoomState::SelectChart(chart),
            Self::WaitForReady { .. } => RoomState::WaitingForReady,
            Self::Playing { .. } => RoomState::Playing,
        }
    }
}

pub struct Room {
    pub id: RoomId,
    pub host: RwLock<Weak<User>>,
    pub state: RwLock<InternalRoomState>,

    pub live: AtomicBool,
    pub locked: AtomicBool,
    pub cycle: AtomicBool,

    users: RwLock<Vec<Weak<User>>>,
    monitors: RwLock<Vec<Weak<User>>>,
    pub chart: RwLock<Option<Chart>>,
}

impl Room {
    pub fn new(id: RoomId, host: Weak<User>) -> Self {
        if let Some(user) = host.upgrade() {
            user.monitor.store(false, Ordering::SeqCst);
        }
        Self {
            id,
            host: host.clone().into(),
            state: RwLock::default(),

            live: AtomicBool::new(false),
            locked: AtomicBool::new(false),
            cycle: AtomicBool::new(false),

            users: vec![host].into(),
            monitors: Vec::new().into(),
            chart: RwLock::default(),
        }
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::SeqCst)
    }

    pub fn is_cycle(&self) -> bool {
        self.cycle.load(Ordering::SeqCst)
    }

    pub async fn client_room_state(&self) -> RoomState {
        self.state
            .read()
            .await
            .to_client(self.chart.read().await.as_ref().map(|it| it.id))
    }

    pub async fn client_state(&self, user: &User) -> ClientRoomState {
        ClientRoomState {
            id: self.id.clone(),
            state: self.client_room_state().await,
            live: self.is_live(),
            locked: self.is_locked(),
            cycle: self.is_cycle(),
            is_host: self.check_host(user).await.is_ok(),
            is_ready: matches!(&*self.state.read().await, InternalRoomState::WaitForReady { started } if started.contains(&user.id)),
            users: self
                .users
                .read()
                .await
                .iter()
                .chain(self.monitors.read().await.iter())
                .filter_map(|it| it.upgrade().map(|it| (it.id, it.to_info())))
                .collect(),
        }
    }

    pub async fn on_state_change(&self) {
        self.broadcast(ServerCommand::ChangeState(self.client_room_state().await))
            .await;
    }

    pub async fn add_user(&self, user: Weak<User>, monitor: bool) -> bool {
        if monitor {
            let mut guard = self.monitors.write().await;
            guard.retain(|it| it.strong_count() > 0);
            guard.push(user);
            true
        } else {
            let mut guard = self.users.write().await;
            guard.retain(|it| it.strong_count() > 0);
            if guard.len() >= ROOM_MAX_USERS {
                false
            } else {
                guard.push(user);
                true
            }
        }
    }

    pub async fn users(&self) -> Vec<Arc<User>> {
        self.users
            .read()
            .await
            .iter()
            .filter_map(|it| it.upgrade())
            .collect()
    }

    pub async fn monitors(&self) -> Vec<Arc<User>> {
        self.monitors
            .read()
            .await
            .iter()
            .filter_map(|it| it.upgrade())
            .collect()
    }

    pub async fn check_host(&self, user: &User) -> Result<()> {
        if self.host.read().await.upgrade().map(|it| it.id) != Some(user.id) {
            bail!("only host can do this");
        }
        Ok(())
    }

    #[inline]
    pub async fn send(&self, msg: Message) {
        self.broadcast(ServerCommand::Message(msg)).await;
    }

    pub async fn broadcast(&self, cmd: ServerCommand) {
        debug!("broadcast {cmd:?}");
        for session in self.users().await.into_iter().chain(self.monitors().await) {
            session.try_send(cmd.clone()).await;
        }
    }

    pub async fn broadcast_monitors(&self, cmd: ServerCommand) {
        for session in self.monitors().await {
            session.try_send(cmd.clone()).await;
        }
    }

    #[inline]
    pub async fn send_as(&self, user: &User, content: String) {
        self.send(Message::Chat {
            user: user.id,
            content,
        })
        .await;
    }

    /// Return: should the room be dropped
    #[must_use]
    pub async fn on_user_leave(&self, user: &User) -> bool {
        self.send(Message::LeaveRoom {
            user: user.id,
            name: user.name.clone(),
        })
        .await;
        *user.room.write().await = None;
        (if user.monitor.swap(false, Ordering::SeqCst) {
            &self.monitors
        } else {
            &self.users
        })
        .write()
        .await
        .retain(|it| it.upgrade().is_some_and(|it| it.id != user.id));
        if self.check_host(user).await.is_ok() {
            info!("host disconnected!");
            let users = self.users().await;
            if users.is_empty() {
                info!("room users all disconnected, dropping room");
                return true;
            } else {
                let user = users.choose(&mut rand::rng()).unwrap();
                debug!("selected {} as host", user.id);
                *self.host.write().await = Arc::downgrade(user);
                self.send(Message::NewHost { user: user.id }).await;
                user.try_send(ServerCommand::ChangeHost(true)).await;
            }
        }
        self.check_all_ready().await;
        false
    }

    pub async fn reset_game_time(&self) {
        for user in self.users().await {
            user.game_time
                .store(f32::NEG_INFINITY.to_bits(), Ordering::SeqCst);
        }
    }

    pub async fn check_all_ready(&self) {
        let mut guard = self.state.write().await;
        match guard.deref_mut() {
            InternalRoomState::WaitForReady { started } => {
                if self
                    .users()
                    .await
                    .into_iter()
                    .chain(self.monitors().await)
                    .all(|it| started.contains(&it.id))
                {
                    *guard = InternalRoomState::Playing {
                        results: HashMap::new(),
                        aborted: HashSet::new(),
                    };
                    drop(guard);
                    info!(room = self.id.to_string(), "game start");
                    self.send(Message::StartPlaying).await;
                    self.reset_game_time().await;
                    self.on_state_change().await;
                }
            }
            // clippy suggests folding the `if` into a match guard, but a guard cannot `.await`.
            #[allow(clippy::collapsible_match)]
            InternalRoomState::Playing { results, aborted } => {
                if self
                    .users()
                    .await
                    .into_iter()
                    .all(|it| results.contains_key(&it.id) || aborted.contains(&it.id))
                {
                    *guard = InternalRoomState::SelectChart;
                    drop(guard);
                    // TODO print results
                    self.send(Message::GameEnd).await;
                    // dbg!(2);
                    // dbg!(3);
                    if self.is_cycle() {
                        debug!(room = self.id.to_string(), "cycling");
                        let host = Weak::clone(&*self.host.read().await);
                        let new_host = {
                            let users = self.users().await;
                            let index = users
                                .iter()
                                .position(|it| host.ptr_eq(&Arc::downgrade(it)))
                                .map(|it| (it + 1) % users.len())
                                .unwrap_or_default();
                            users.into_iter().nth(index).unwrap()
                        };
                        *self.host.write().await = Arc::downgrade(&new_host);
                        self.send(Message::NewHost { user: new_host.id }).await;
                        if let Some(old) = host.upgrade() {
                            old.try_send(ServerCommand::ChangeHost(false)).await;
                        }
                        new_host.try_send(ServerCommand::ChangeHost(true)).await;
                    }
                    self.on_state_change().await;
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MAX_UNAUTHENTICATED_CONNS, ServerConfig, ServerState, l10n::Language};
    use tokio::sync::{Semaphore, mpsc};

    fn test_server_state() -> Arc<ServerState> {
        let (tx, _rx) = mpsc::channel(16);
        std::mem::forget(_rx);
        Arc::new(ServerState {
            config: ServerConfig::default(),
            http_client: reqwest::Client::new(),
            unauth_sem: Arc::new(Semaphore::new(MAX_UNAUTHENTICATED_CONNS)),
            sessions: Default::default(),
            users: Default::default(),
            rooms: Default::default(),
            lost_con_tx: tx,
        })
    }

    fn test_user(state: &Arc<ServerState>, id: i32) -> Arc<User> {
        Arc::new(User::new(
            id,
            format!("u{id}"),
            Language::default(),
            Arc::clone(state),
        ))
    }

    fn test_record(player: i32) -> Record {
        Record {
            id: player,
            player,
            score: 1,
            perfect: 1,
            good: 0,
            bad: 0,
            miss: 0,
            max_combo: 1,
            accuracy: 1.0,
            full_combo: true,
            std: 0.0,
            std_score: 0.0,
        }
    }

    fn room_id(s: &str) -> RoomId {
        s.to_owned().try_into().unwrap()
    }

    /// Regression test for M-1 (ported from `scratch/audit`): concurrent `check_all_ready()`
    /// calls must not perform duplicate `Playing -> SelectChart` transitions (which previously
    /// rotated the host more than once when `cycle` was enabled).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn check_all_ready_does_not_double_transition() {
        let state = test_server_state();
        for i in 0..300 {
            let (a, b, c) = (
                test_user(&state, 1),
                test_user(&state, 2),
                test_user(&state, 3),
            );
            let room = Arc::new(Room::new(room_id(&format!("r{i}")), Arc::downgrade(&a)));
            assert!(room.add_user(Arc::downgrade(&b), false).await);
            assert!(room.add_user(Arc::downgrade(&c), false).await);
            room.cycle.store(true, Ordering::SeqCst);
            *room.state.write().await = InternalRoomState::Playing {
                results: [1, 2, 3]
                    .into_iter()
                    .map(|id| (id, test_record(id)))
                    .collect(),
                aborted: HashSet::new(),
            };
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let r = Arc::clone(&room);
                    tokio::spawn(async move { r.check_all_ready().await })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
            let host = room.host.read().await.upgrade().unwrap().id;
            assert_eq!(
                host, 2,
                "iteration {i}: host rotated more than once (host={host})"
            );
        }
    }

    /// Regression test for M-3 (ported from `scratch/audit`): a user who previously joined a
    /// room as a monitor must not retain `monitor = true` when later creating/leaving a room.
    #[tokio::test]
    async fn stale_monitor_flag_prevents_room_cleanup() {
        let state = test_server_state();
        let u = test_user(&state, 2);
        u.monitor.store(true, Ordering::SeqCst);
        let room = Arc::new(Room::new(room_id("r"), Arc::downgrade(&u)));
        *u.room.write().await = Some(Arc::clone(&room));
        assert!(
            !u.monitor.load(Ordering::SeqCst),
            "host monitor flag must be reset on Room::new"
        );
        let drop_room = room.on_user_leave(&u).await;
        assert!(
            drop_room,
            "room should be dropped after its only user leaves"
        );
        assert!(room.users().await.is_empty());
    }
}
