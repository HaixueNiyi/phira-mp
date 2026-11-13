pub mod resolver;

use anyhow::{Context, Error, Result};
use dashmap::DashMap;
use phira_mp_common::{
    ClientCommand, ClientRoomState, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, JoinRoomResponse,
    JudgeEvent, Message, RoomId, RoomState, ServerCommand, Stream, TouchFrame, UserInfo,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock, oneshot, watch},
    task::JoinHandle,
    time,
};
use tracing::{trace, warn};

const MAX_QUEUED_MESSAGES: usize = 1024;

struct CallbackSlot<T> {
    call_lock: Mutex<()>,
    tx: Mutex<Option<oneshot::Sender<T>>>,
}

impl<T> Default for CallbackSlot<T> {
    fn default() -> Self {
        Self {
            call_lock: Mutex::default(),
            tx: Mutex::default(),
        }
    }
}

type RCallback<T, E = String> = CallbackSlot<Result<T, E>>;

pub const TIMEOUT: Duration = Duration::from_secs(7);

pub struct LivePlayer {
    pub touch_frames: Mutex<Vec<TouchFrame>>,
    pub judge_events: Mutex<Vec<JudgeEvent>>,
}

impl Default for LivePlayer {
    fn default() -> Self {
        Self::new()
    }
}

impl LivePlayer {
    pub fn new() -> Self {
        Self {
            touch_frames: Mutex::default(),
            judge_events: Mutex::default(),
        }
    }
}

struct State {
    delay: Mutex<Option<Duration>>,
    ping_lock: Mutex<()>,
    pong_tx: Mutex<Option<oneshot::Sender<()>>>,

    me: RwLock<Option<UserInfo>>,
    room: RwLock<Option<ClientRoomState>>,

    cb_authenticate: RCallback<(UserInfo, Option<ClientRoomState>)>,
    cb_chat: RCallback<()>,
    cb_create_room: RCallback<()>,
    cb_join_room: RCallback<JoinRoomResponse>,
    cb_leave_room: RCallback<()>,
    cb_lock_room: RCallback<()>,
    cb_cycle_room: RCallback<()>,
    cb_select_chart: RCallback<()>,
    cb_request_start: RCallback<()>,
    cb_ready: RCallback<()>,
    cb_cancel_ready: RCallback<()>,
    cb_played: RCallback<()>,
    cb_abort: RCallback<()>,

    live_players: DashMap<i32, Arc<LivePlayer>>,
    messages: Mutex<Vec<Message>>,
    pending_create_id: Mutex<Option<RoomId>>,
    pending_join_id: Mutex<Option<RoomId>>,
}

impl State {
    pub fn live_player(&self, player: i32) -> Arc<LivePlayer> {
        Arc::clone(
            &self
                .live_players
                .entry(player)
                .or_insert_with(|| Arc::new(LivePlayer::new())),
        )
    }
}

pub struct Client {
    state: Arc<State>,

    stream: Arc<Stream<ClientCommand, ServerCommand>>,

    ping_fail_count: Arc<AtomicU8>,
    ping_task_handle: JoinHandle<()>,
}

impl Client {
    pub async fn new(stream: TcpStream) -> Result<Self> {
        stream.set_nodelay(true)?;

        let state = Arc::new(State {
            delay: Mutex::default(),
            ping_lock: Mutex::default(),
            pong_tx: Mutex::default(),

            me: RwLock::default(),
            room: RwLock::default(),

            cb_authenticate: CallbackSlot::default(),
            cb_chat: CallbackSlot::default(),
            cb_create_room: CallbackSlot::default(),
            cb_join_room: CallbackSlot::default(),
            cb_leave_room: CallbackSlot::default(),
            cb_lock_room: CallbackSlot::default(),
            cb_cycle_room: CallbackSlot::default(),
            cb_select_chart: CallbackSlot::default(),
            cb_request_start: CallbackSlot::default(),
            cb_ready: CallbackSlot::default(),
            cb_cancel_ready: CallbackSlot::default(),
            cb_played: CallbackSlot::default(),
            cb_abort: CallbackSlot::default(),

            live_players: DashMap::new(),
            messages: Mutex::default(),
            pending_create_id: Mutex::default(),
            pending_join_id: Mutex::default(),
        });
        let stream = Arc::new(
            Stream::new(
                Some(1),
                stream,
                Box::new({
                    let state = Arc::clone(&state);
                    move |_send_tx, cmd| process(Arc::clone(&state), cmd)
                }),
            )
            .await?,
        );

        let ping_fail_count = Arc::new(AtomicU8::default());
        let ping_task_handle = tokio::spawn({
            let ping_fail_count = Arc::clone(&ping_fail_count);
            let state = Arc::clone(&state);
            let stream = Arc::clone(&stream);
            let mut closed_rx = stream.closed();
            async move {
                loop {
                    tokio::select! {
                        _ = time::sleep(HEARTBEAT_INTERVAL) => {}
                        _ = wait_until_closed(&mut closed_rx) => break,
                    }
                    if stream.is_closed() {
                        break;
                    }
                    match send_ping(&state, &stream).await {
                        Ok(delay) => {
                            ping_fail_count.store(0, Ordering::SeqCst);
                            trace!("sent heartbeat, delay: {delay:?}");
                        }
                        Err(err) => {
                            if stream.is_closed() {
                                break;
                            }
                            warn!("heartbeat timeout or error: {err:?}");
                            let _ = ping_fail_count.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |cur| Some(cur.saturating_add(1)),
                            );
                        }
                    }
                }
            }
        });

        Ok(Self {
            state,

            stream,

            ping_fail_count,
            ping_task_handle,
        })
    }

    pub async fn from_address(addr: &str) -> Result<Self> {
        let auth: http::uri::Authority = addr.parse().context("Invalid server address")?;
        let resolved_addr = resolver::resolve(&auth).await?;
        let stream = TcpStream::connect(resolved_addr).await?;
        Self::new(stream).await
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn me(&self) -> Option<UserInfo> {
        self.state.me.blocking_read().clone()
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn user_name(&self, id: i32) -> String {
        self.user_name_opt(id).unwrap_or_else(|| "?".to_owned())
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn user_name_opt(&self, id: i32) -> Option<String> {
        self.state
            .room
            .blocking_read()
            .as_ref()
            .and_then(|it| it.users.get(&id).map(|it| it.name.clone()))
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_take_messages(&self) -> Vec<Message> {
        self.state.messages.blocking_lock().drain(..).collect()
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_state(&self) -> Option<ClientRoomState> {
        self.state.room.blocking_read().clone()
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_room_id(&self) -> Option<RoomId> {
        self.state
            .room
            .blocking_read()
            .as_ref()
            .map(|it| it.id.clone())
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_room_state(&self) -> Option<RoomState> {
        self.state.room.blocking_read().as_ref().map(|it| it.state)
    }

    pub async fn room_state(&self) -> Option<RoomState> {
        self.state.room.read().await.as_ref().map(|it| it.state)
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_is_host(&self) -> Option<bool> {
        self.state
            .room
            .blocking_read()
            .as_ref()
            .map(|it| it.is_host)
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn blocking_is_ready(&self) -> Option<bool> {
        self.state
            .room
            .blocking_read()
            .as_ref()
            .map(|it| it.is_ready)
    }

    pub async fn ping(&self) -> Result<Duration> {
        send_ping(&self.state, &self.stream).await
    }

    /// Must not be called from within a Tokio runtime worker thread.
    pub fn delay(&self) -> Option<Duration> {
        *self.state.delay.blocking_lock()
    }

    async fn rcall_locked<R>(&self, payload: ClientCommand, cb: &RCallback<R>) -> Result<R> {
        let (tx, rx) = oneshot::channel();
        *cb.tx.lock().await = Some(tx);
        if let Err(err) = self.stream.send(payload).await {
            *cb.tx.lock().await = None;
            return Err(err);
        }
        match time::timeout(TIMEOUT, rx).await {
            Ok(Ok(res)) => res.map_err(Error::msg),
            Ok(Err(_)) => Err(Error::msg("response channel closed")),
            Err(_) => {
                *cb.tx.lock().await = None;
                anyhow::bail!("timeout");
            }
        }
    }

    async fn rcall<R>(&self, payload: ClientCommand, cb: &RCallback<R>) -> Result<R> {
        let _call_guard = cb.call_lock.lock().await;
        self.rcall_locked(payload, cb).await
    }

    #[inline]
    pub async fn authenticate(&self, token: impl Into<String>) -> Result<()> {
        let _ = self
            .rcall(
                ClientCommand::Authenticate {
                    token: token.into().try_into()?,
                },
                &self.state.cb_authenticate,
            )
            .await?;
        Ok(())
    }

    #[inline]
    pub async fn chat(&self, message: String) -> Result<()> {
        self.rcall(
            ClientCommand::Chat {
                message: message.try_into()?,
            },
            &self.state.cb_chat,
        )
        .await
    }

    #[inline]
    pub async fn create_room(&self, id: RoomId) -> Result<()> {
        let _call_guard = self.state.cb_create_room.call_lock.lock().await;
        *self.state.pending_create_id.lock().await = Some(id.clone());
        let res = self
            .rcall_locked(ClientCommand::CreateRoom { id }, &self.state.cb_create_room)
            .await;
        if res.is_err() {
            *self.state.pending_create_id.lock().await = None;
        }
        res
    }

    #[inline]
    pub async fn join_room(&self, id: RoomId, monitor: bool) -> Result<()> {
        let _call_guard = self.state.cb_join_room.call_lock.lock().await;
        *self.state.pending_join_id.lock().await = Some(id.clone());
        let res = self
            .rcall_locked(
                ClientCommand::JoinRoom { id, monitor },
                &self.state.cb_join_room,
            )
            .await;
        if res.is_err() {
            *self.state.pending_join_id.lock().await = None;
        }
        res.map(|_| ())
    }

    #[inline]
    pub async fn leave_room(&self) -> Result<()> {
        self.rcall(ClientCommand::LeaveRoom, &self.state.cb_leave_room)
            .await
    }

    #[inline]
    pub async fn lock_room(&self, lock: bool) -> Result<()> {
        self.rcall(ClientCommand::LockRoom { lock }, &self.state.cb_lock_room)
            .await
    }

    #[inline]
    pub async fn cycle_room(&self, cycle: bool) -> Result<()> {
        self.rcall(
            ClientCommand::CycleRoom { cycle },
            &self.state.cb_cycle_room,
        )
        .await
    }

    #[inline]
    pub async fn select_chart(&self, id: i32) -> Result<()> {
        self.rcall(
            ClientCommand::SelectChart { id },
            &self.state.cb_select_chart,
        )
        .await
    }

    #[inline]
    pub async fn request_start(&self) -> Result<()> {
        self.rcall(ClientCommand::RequestStart, &self.state.cb_request_start)
            .await
    }

    #[inline]
    pub async fn ready(&self) -> Result<()> {
        self.rcall(ClientCommand::Ready, &self.state.cb_ready).await
    }

    #[inline]
    pub async fn cancel_ready(&self) -> Result<()> {
        self.rcall(ClientCommand::CancelReady, &self.state.cb_cancel_ready)
            .await
    }

    #[inline]
    pub async fn played(&self, id: i32) -> Result<()> {
        self.rcall(ClientCommand::Played { id }, &self.state.cb_played)
            .await
    }

    #[inline]
    pub async fn abort(&self) -> Result<()> {
        self.rcall(ClientCommand::Abort, &self.state.cb_abort).await
    }

    pub fn ping_fail_count(&self) -> u8 {
        self.ping_fail_count.load(Ordering::Relaxed)
    }

    pub async fn send(&self, payload: ClientCommand) -> Result<()> {
        self.stream.send(payload).await
    }

    pub fn blocking_send(&self, payload: ClientCommand) -> Result<()> {
        self.stream.blocking_send(payload)
    }

    #[inline]
    pub fn live_player(&self, player: i32) -> Arc<LivePlayer> {
        self.state.live_player(player)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.ping_task_handle.abort();
    }
}

async fn wait_until_closed(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow_and_update() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow_and_update() {
            return;
        }
    }
}

async fn send_ping(
    state: &State,
    stream: &Stream<ClientCommand, ServerCommand>,
) -> Result<Duration> {
    let _ping_guard = state.ping_lock.lock().await;
    let (tx, rx) = oneshot::channel();
    *state.pong_tx.lock().await = Some(tx);
    let start = Instant::now();
    if let Err(err) = stream.send(ClientCommand::Ping).await {
        *state.pong_tx.lock().await = None;
        return Err(err);
    }
    match time::timeout(HEARTBEAT_TIMEOUT, rx).await {
        Ok(Ok(())) => {
            let delay = start.elapsed();
            *state.delay.lock().await = Some(delay);
            Ok(delay)
        }
        Ok(Err(_)) => Err(Error::msg("ping response channel closed")),
        Err(_) => {
            *state.pong_tx.lock().await = None;
            anyhow::bail!("heartbeat timeout");
        }
    }
}

async fn process(state: Arc<State>, cmd: ServerCommand) {
    async fn cb<T>(cb: &CallbackSlot<T>, res: T) {
        if let Some(tx) = cb.tx.lock().await.take() {
            let _ = tx.send(res);
        } else {
            warn!("received unexpected callback response");
        }
    }
    match cmd {
        ServerCommand::Pong => {
            if let Some(tx) = state.pong_tx.lock().await.take() {
                let _ = tx.send(());
            }
        }
        ServerCommand::Authenticate(res) => {
            let res = match res {
                Ok((me, room)) => {
                    *state.me.write().await = Some(me.clone());
                    *state.room.write().await = room.clone();
                    state.live_players.clear();
                    Ok((me, room))
                }
                Err(err) => Err(err),
            };
            cb(&state.cb_authenticate, res).await;
        }
        ServerCommand::Chat(res) => {
            cb(&state.cb_chat, res).await;
        }
        ServerCommand::Touches { player, frames } => {
            state
                .live_player(player)
                .touch_frames
                .lock()
                .await
                .extend(frames.iter().cloned());
        }
        ServerCommand::Judges { player, judges } => {
            state
                .live_player(player)
                .judge_events
                .lock()
                .await
                .extend(judges.iter().cloned());
        }
        ServerCommand::Message(msg) => {
            match msg {
                Message::LockRoom { lock } => {
                    if let Some(room) = state.room.write().await.as_mut() {
                        room.locked = lock;
                    }
                }
                Message::CycleRoom { cycle } => {
                    if let Some(room) = state.room.write().await.as_mut() {
                        room.cycle = cycle;
                    }
                }
                Message::LeaveRoom { user, .. } => {
                    state.live_players.remove(&user);
                    if let Some(room) = state.room.write().await.as_mut() {
                        room.users.remove(&user);
                    }
                }
                _ => {}
            }
            let mut messages = state.messages.lock().await;
            if messages.len() >= MAX_QUEUED_MESSAGES {
                let drain_count = messages.len() - (MAX_QUEUED_MESSAGES - 1);
                messages.drain(..drain_count);
            }
            messages.push(msg);
        }
        ServerCommand::ChangeState(room) => {
            state.live_players.clear();
            if let Some(room_state) = state.room.write().await.as_mut() {
                room_state.state = room;
                room_state.is_ready = room_state.is_host;
            }
        }
        ServerCommand::ChangeHost(me_is_host) => {
            if let Some(room) = state.room.write().await.as_mut() {
                room.is_host = me_is_host;
            }
        }

        ServerCommand::CreateRoom(res) => {
            let res = match res {
                Ok(()) => {
                    let id = state.pending_create_id.lock().await.take();
                    let me = state.me.read().await.clone();
                    match (id, me) {
                        (Some(id), Some(me)) => {
                            state.live_players.clear();
                            *state.room.write().await = Some(ClientRoomState {
                                id,
                                state: RoomState::default(),
                                live: false,
                                locked: false,
                                cycle: false,
                                is_host: true,
                                is_ready: false,
                                users: std::iter::once((me.id, me)).collect(),
                            });
                            Ok(())
                        }
                        (None, _) => Err("missing pending room id".to_owned()),
                        (_, None) => Err("not authenticated".to_owned()),
                    }
                }
                Err(err) => {
                    *state.pending_create_id.lock().await = None;
                    Err(err)
                }
            };
            cb(&state.cb_create_room, res).await;
        }
        ServerCommand::JoinRoom(res) => {
            let res = match res {
                Ok(resp) => {
                    if let Some(id) = state.pending_join_id.lock().await.take() {
                        state.live_players.clear();
                        *state.room.write().await = Some(ClientRoomState {
                            id,
                            state: resp.state,
                            live: resp.live,
                            locked: false,
                            cycle: false,
                            is_host: false,
                            is_ready: false,
                            users: resp.users.iter().cloned().map(|it| (it.id, it)).collect(),
                        });
                        Ok(resp)
                    } else {
                        Err("missing pending room id".to_owned())
                    }
                }
                Err(err) => {
                    *state.pending_join_id.lock().await = None;
                    Err(err)
                }
            };
            cb(&state.cb_join_room, res).await;
        }
        ServerCommand::OnJoinRoom(user) => {
            if let Some(room) = state.room.write().await.as_mut() {
                room.live |= user.monitor;
                room.users.insert(user.id, user);
            }
        }
        ServerCommand::LeaveRoom(res) => {
            if res.is_ok() {
                *state.room.write().await = None;
                state.live_players.clear();
            }
            cb(&state.cb_leave_room, res).await;
        }
        ServerCommand::LockRoom(res) => {
            cb(&state.cb_lock_room, res).await;
        }
        ServerCommand::CycleRoom(res) => {
            cb(&state.cb_cycle_room, res).await;
        }
        ServerCommand::SelectChart(res) => {
            cb(&state.cb_select_chart, res).await;
        }
        ServerCommand::RequestStart(res) => {
            if res.is_ok()
                && let Some(room) = state.room.write().await.as_mut()
            {
                room.is_ready = true;
            }
            cb(&state.cb_request_start, res).await;
        }
        ServerCommand::Ready(res) => {
            if res.is_ok()
                && let Some(room) = state.room.write().await.as_mut()
            {
                room.is_ready = true;
            }
            cb(&state.cb_ready, res).await;
        }
        ServerCommand::CancelReady(res) => {
            if res.is_ok()
                && let Some(room) = state.room.write().await.as_mut()
            {
                room.is_ready = false;
            }
            cb(&state.cb_cancel_ready, res).await;
        }
        ServerCommand::Played(res) => {
            cb(&state.cb_played, res).await;
        }
        ServerCommand::Abort(res) => {
            cb(&state.cb_abort, res).await;
        }
    }
}
