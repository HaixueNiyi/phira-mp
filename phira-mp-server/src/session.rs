use crate::{
    AddUserResult, Chart, InternalRoomState, Record, Room, ServerState,
    l10n::{LANGUAGE, Language},
    tl,
};
use anyhow::{Result, anyhow, bail};
use phira_mp_common::{
    ClientCommand, HEARTBEAT_DISCONNECT_TIMEOUT, JoinRoomResponse, JudgeEvent, MAX_PACKET_SIZE,
    Message, ServerCommand, Stream, TouchFrame, UserInfo,
};
use serde::Deserialize;
use std::{
    collections::{HashSet, hash_map::Entry},
    future::Future,
    ops::DerefMut,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, OnceCell, RwLock, oneshot, watch},
    task::JoinHandle,
    time,
};
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};
use uuid::Uuid;

const HOST: &str = "https://phira.5wyxi.com";

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    fn take(&mut self, capacity: f64, refill_per_sec: f64) -> bool {
        let now = Instant::now();
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * refill_per_sec).min(capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct User {
    pub id: i32,
    pub name: String,
    pub lang: Language,

    pub server: Arc<ServerState>,
    pub session: RwLock<Option<Weak<Session>>>,
    pub room: RwLock<Option<Arc<Room>>>,

    pub monitor: AtomicBool,
    pub game_time: AtomicU32,

    pub dangle_mark: Mutex<Option<Arc<()>>>,

    chat_limit: std::sync::Mutex<TokenBucket>,
    room_op_limit: std::sync::Mutex<TokenBucket>,
    live_frame_limit: std::sync::Mutex<TokenBucket>,
}

impl User {
    pub fn new(id: i32, name: String, lang: Language, server: Arc<ServerState>) -> Self {
        Self {
            id,
            name,
            lang,

            server,
            session: RwLock::default(),
            room: RwLock::default(),

            monitor: AtomicBool::default(),
            game_time: AtomicU32::default(),

            dangle_mark: Mutex::default(),

            chat_limit: std::sync::Mutex::new(TokenBucket::new(10.0)),
            room_op_limit: std::sync::Mutex::new(TokenBucket::new(15.0)),
            live_frame_limit: std::sync::Mutex::new(TokenBucket::new(300.0)),
        }
    }

    pub fn to_info(&self) -> UserInfo {
        UserInfo {
            id: self.id,
            name: self.name.clone(),
            monitor: self.monitor.load(Ordering::SeqCst),
        }
    }

    pub fn can_monitor(&self) -> bool {
        self.server.config.monitors.contains(&self.id)
    }

    pub async fn set_session(&self, session: Weak<Session>) -> Option<Weak<Session>> {
        let mut mark = self.dangle_mark.lock().await;
        *mark = None;
        self.session.write().await.replace(session)
    }

    pub async fn try_send(&self, cmd: ServerCommand) {
        let session = self.session.read().await.as_ref().and_then(Weak::upgrade);
        if let Some(session) = session {
            session.try_send(cmd).await;
        } else {
            warn!("sending {cmd:?} to dangling user {}", self.id);
        }
    }

    pub async fn dangle(self: Arc<Self>) {
        self.dangle_with_delay(None, Duration::from_secs(10)).await;
    }

    pub async fn dangle_for_session(self: Arc<Self>, session_id: Uuid) {
        self.dangle_with_delay(Some(session_id), Duration::from_secs(10))
            .await;
    }

    pub(crate) async fn dangle_with_delay(
        self: Arc<Self>,
        expected_session: Option<Uuid>,
        delay: Duration,
    ) {
        warn!(user = self.id, "user dangling");
        let guard = self.room.read().await;
        let room = guard.as_ref().map(Arc::clone);
        drop(guard);
        if let Some(room) = room {
            let is_playing_unfinished = {
                let guard = room.state.read().await;
                if let InternalRoomState::Playing { results, aborted } = &*guard {
                    !self.monitor.load(Ordering::SeqCst)
                        && !results.contains_key(&self.id)
                        && !aborted.contains(&self.id)
                } else {
                    false
                }
            };
            if is_playing_unfinished {
                warn!(user = self.id, "lost connection on playing, aborting");
                let aborted = {
                    let mut users_guard = self.server.users.write().await;
                    let mut mark_guard = self.dangle_mark.lock().await;
                    let mut session_guard = self.session.write().await;
                    let matches_session = match expected_session {
                        Some(sid) => session_guard
                            .as_ref()
                            .and_then(Weak::upgrade)
                            .is_none_or(|s| s.id == sid),
                        None => true,
                    };
                    if matches_session {
                        *mark_guard = None;
                        *session_guard = None;
                        if users_guard
                            .get(&self.id)
                            .is_some_and(|u| Arc::ptr_eq(u, &self))
                        {
                            users_guard.remove(&self.id);
                        }
                        true
                    } else {
                        false
                    }
                };
                if aborted && room.on_user_leave(&self).await {
                    let mut rooms = self.server.rooms.write().await;
                    if rooms.get(&room.id).is_some_and(|r| Arc::ptr_eq(r, &room)) {
                        rooms.remove(&room.id);
                    }
                }
                return;
            }
        }
        let dangle_mark = Arc::new(());
        {
            let mut mark_guard = self.dangle_mark.lock().await;
            let session_guard = self.session.read().await;
            let matches_session = match expected_session {
                Some(sid) => session_guard
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .is_none_or(|s| s.id == sid),
                None => true,
            };
            if !matches_session {
                return;
            }
            *mark_guard = Some(Arc::clone(&dangle_mark));
        }
        tokio::spawn(async move {
            time::sleep(delay).await;
            let evicted = {
                let mut users_guard = self.server.users.write().await;
                let mut mark_guard = self.dangle_mark.lock().await;
                let matches_mark = mark_guard
                    .as_ref()
                    .is_some_and(|cur| Arc::ptr_eq(cur, &dangle_mark));
                if !matches_mark {
                    false
                } else {
                    *mark_guard = None;
                    *self.session.write().await = None;
                    if users_guard
                        .get(&self.id)
                        .is_some_and(|u| Arc::ptr_eq(u, &self))
                    {
                        users_guard.remove(&self.id);
                    }
                    true
                }
            };
            if evicted {
                let guard = self.room.read().await;
                let room = guard.as_ref().map(Arc::clone);
                drop(guard);
                if let Some(room) = room
                    && room.on_user_leave(&self).await
                {
                    let mut rooms = self.server.rooms.write().await;
                    if rooms.get(&room.id).is_some_and(|r| Arc::ptr_eq(r, &room)) {
                        rooms.remove(&room.id);
                    }
                }
            }
        });
    }
}

pub struct Session {
    pub id: Uuid,
    pub stream: Stream<ServerCommand, ClientCommand>,
    pub user: Arc<User>,

    panicked: Arc<AtomicBool>,
    monitor_task_handle: JoinHandle<()>,
}

#[derive(Debug, Deserialize)]
struct AuthUserInfo {
    id: i32,
    name: String,
    language: String,
}

async fn authenticate_token(server: &ServerState, id: Uuid, token: String) -> Result<AuthUserInfo> {
    if token.len() > 32 {
        bail!("invalid token");
    }
    debug!("session {id}: authenticate");
    let resp: AuthUserInfo = server
        .http_client
        .get(format!("{HOST}/me"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(|err| {
            warn!("failed to fetch info: {err:?}");
            anyhow!("failed to fetch info")
        })?
        .error_for_status()
        .map_err(|err| {
            warn!("failed to fetch info: {err:?}");
            anyhow!("failed to fetch info")
        })?
        .json()
        .await
        .map_err(|err| {
            warn!("failed to fetch info: {err:?}");
            anyhow!("failed to fetch info")
        })?;
    debug!("session {id} <- {resp:?}");
    Ok(resp)
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

fn spawn_monitor_task(
    id: Uuid,
    heartbeat_timeout: Duration,
    last_recv: Arc<std::sync::Mutex<Instant>>,
    mut stream_closed: watch::Receiver<bool>,
    session: Weak<Session>,
    server: Arc<ServerState>,
    panicked: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let recv = *last_recv.lock().unwrap();
            let deadline = (recv + heartbeat_timeout).into();
            tokio::select! {
                _ = time::sleep_until(deadline) => {
                    let latest = *last_recv.lock().unwrap();
                    if latest + heartbeat_timeout > Instant::now() {
                        continue;
                    }
                }
                _ = wait_until_closed(&mut stream_closed) => {}
            }
            if let Some(session) = session.upgrade() {
                session.mark_lost();
            } else if !panicked.swap(true, Ordering::SeqCst)
                && let Err(err) = server.lost_con_tx.send(id).await
            {
                error!("failed to mark lost connection ({id}): {err:?}");
            }
            break;
        }
    })
}

type AuthDoneTx = oneshot::Sender<Option<Weak<Session>>>;
type AuthTx = oneshot::Sender<(String, AuthDoneTx)>;

type StreamHandler = Box<
    dyn FnMut(
            Arc<tokio::sync::mpsc::Sender<ServerCommand>>,
            ClientCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

fn build_stream_handler(
    id: Uuid,
    server: Arc<ServerState>,
    this: Arc<OnceCell<Weak<Session>>>,
    last_recv: Arc<std::sync::Mutex<Instant>>,
    panicked: Arc<AtomicBool>,
    mut auth_tx: Option<AuthTx>,
    waiting_for_authenticate: Arc<AtomicBool>,
) -> StreamHandler {
    Box::new(move |send_tx, cmd| {
        let this = Arc::clone(&this);
        let auth_slice = if matches!(cmd, ClientCommand::Authenticate { .. }) {
            auth_tx.take()
        } else {
            None
        };
        let server = Arc::clone(&server);
        let last_recv = Arc::clone(&last_recv);
        let waiting_for_authenticate = Arc::clone(&waiting_for_authenticate);
        let panicked = Arc::clone(&panicked);
        Box::pin(async move {
            *last_recv.lock().unwrap() = Instant::now();
            if panicked.load(Ordering::SeqCst) {
                return;
            }
            if matches!(cmd, ClientCommand::Ping) {
                let _ = send_tx.try_send(ServerCommand::Pong);
                return;
            }
            if waiting_for_authenticate.load(Ordering::SeqCst) {
                if let ClientCommand::Authenticate { token } = cmd {
                    let Some(auth_tx) = auth_slice else {
                        warn!("duplicate authenticate attempt on session {id}");
                        return;
                    };
                    let (done_tx, done_rx) = oneshot::channel();
                    if auth_tx.send((token.into_inner(), done_tx)).is_err() {
                        return;
                    }
                    if let Ok(Some(weak)) = done_rx.await {
                        let _ = this.set(weak);
                        waiting_for_authenticate.store(false, Ordering::SeqCst);
                    }
                } else {
                    warn!("packet before authentication, ignoring: {cmd:?}");
                }
                return;
            }
            let Some(session) = this.get().and_then(Weak::upgrade) else {
                return;
            };
            let user = Arc::clone(&session.user);
            drop(session);
            if let Some(resp) = LANGUAGE
                .scope(Arc::new(user.lang.clone()), process(user, cmd))
                .await
                && let Err(err) = send_tx.try_send(resp)
            {
                error!("failed to handle message, aborting connection {id}: {err:?}");
                if let Some(session) = this.get().and_then(Weak::upgrade) {
                    session.mark_lost();
                } else if !panicked.swap(true, Ordering::SeqCst)
                    && let Err(err) = server.lost_con_tx.send(id).await
                {
                    error!("failed to mark lost connection ({id}): {err:?}");
                }
            }
        })
    })
}

impl Session {
    async fn install(
        id: Uuid,
        stream: Stream<ServerCommand, ClientCommand>,
        user: Arc<User>,
        last_recv: Arc<std::sync::Mutex<Instant>>,
        panicked: Arc<AtomicBool>,
        this: &Arc<OnceCell<Weak<Session>>>,
        heartbeat_timeout: Duration,
    ) -> Arc<Self> {
        let server = Arc::clone(&user.server);
        let stream_close_rx = stream.closed();
        let res = Arc::new_cyclic(|weak_self| {
            let monitor_task_handle = spawn_monitor_task(
                id,
                heartbeat_timeout,
                last_recv,
                stream_close_rx,
                Weak::clone(weak_self),
                Arc::clone(&server),
                Arc::clone(&panicked),
            );
            Self {
                id,
                stream,
                user: Arc::clone(&user),
                panicked,
                monitor_task_handle,
            }
        });
        let _ = this.set(Arc::downgrade(&res));
        let old_session = res.user.set_session(Arc::downgrade(&res)).await;
        server
            .sessions
            .write()
            .await
            .insert(res.id, Arc::clone(&res));
        if let Some(old) = old_session.and_then(|w| w.upgrade())
            && !Arc::ptr_eq(&old, &res)
        {
            info!(
                "kicking previous session {} for user {}",
                old.id, res.user.id
            );
            old.close_replaced(&server).await;
        }
        res
    }

    pub async fn new(id: Uuid, stream: TcpStream, server: Arc<ServerState>) -> Result<Arc<Self>> {
        stream.set_nodelay(true)?;
        let this = Arc::new(OnceCell::<Weak<Session>>::new());
        let (auth_tx, auth_rx) = oneshot::channel();
        let last_recv = Arc::new(std::sync::Mutex::new(Instant::now()));
        let panicked = Arc::new(AtomicBool::new(false));
        let waiting_for_authenticate = Arc::new(AtomicBool::new(true));
        let handler = build_stream_handler(
            id,
            Arc::clone(&server),
            Arc::clone(&this),
            Arc::clone(&last_recv),
            Arc::clone(&panicked),
            Some(auth_tx),
            waiting_for_authenticate,
        );
        let mut stream = time::timeout(
            HEARTBEAT_DISCONNECT_TIMEOUT,
            Stream::<ServerCommand, ClientCommand>::new(None, stream, handler),
        )
        .await
        .map_err(|_| anyhow!("handshake timed out"))??;

        if stream.version() != 1 {
            stream.close();
            bail!("unsupported protocol version: {}", stream.version());
        }

        let mut closed_rx = stream.closed();
        let (token, done_tx) = tokio::select! {
            res = time::timeout(HEARTBEAT_DISCONNECT_TIMEOUT, auth_rx) => match res {
                Ok(Ok(pair)) => pair,
                Ok(Err(_)) => {
                    stream.close();
                    bail!("connection closed before authentication");
                }
                Err(_) => {
                    stream.close();
                    bail!("authentication timed out");
                }
            },
            _ = wait_until_closed(&mut closed_rx) => {
                stream.close();
                bail!("connection closed before authentication");
            }
        };

        let auth_res = tokio::select! {
            res = authenticate_token(&server, id, token) => res,
            _ = wait_until_closed(&mut closed_rx) => {
                let _ = done_tx.send(None);
                stream.close();
                bail!("connection closed during authentication");
            }
        };

        let resp = match auth_res {
            Ok(resp) => resp,
            Err(err) => {
                warn!("failed to authenticate session {id}: {err:?}");
                let _ = done_tx.send(None);
                let _ = stream.try_send(ServerCommand::Authenticate(Err(err.to_string())));
                stream.close_after_flush(Duration::from_millis(500)).await;
                return Err(err);
            }
        };

        let user = {
            let mut users_guard = server.users.write().await;
            match users_guard.entry(resp.id) {
                Entry::Occupied(e) => {
                    let u = Arc::clone(e.get());
                    *u.dangle_mark.lock().await = None;
                    u
                }
                Entry::Vacant(e) => {
                    let u = Arc::new(User::new(
                        resp.id,
                        resp.name,
                        resp.language.parse().map(Language).unwrap_or_default(),
                        Arc::clone(&server),
                    ));
                    e.insert(Arc::clone(&u));
                    u
                }
            }
        };

        let res = Self::install(
            id,
            stream,
            user,
            last_recv,
            panicked,
            &this,
            HEARTBEAT_DISCONNECT_TIMEOUT,
        )
        .await;

        let reconnected_room = res.user.room.read().await.as_ref().map(Arc::clone);
        let cancel_wait = match &reconnected_room {
            Some(room) => room.cancel_wait_for_ready_on_reconnect().await,
            None => None,
        };
        let room_state = match &reconnected_room {
            Some(room) => Some(room.client_state(&res.user).await),
            None => None,
        };
        let _ = res.stream.try_send(ServerCommand::Authenticate(Ok((
            res.user.to_info(),
            room_state,
        ))));
        if let (Some(room), Some((new_state, host_id))) = (reconnected_room, cancel_wait) {
            for target in room.users().await.into_iter().chain(room.monitors().await) {
                if target.id != res.user.id {
                    if let Some(host_id) = host_id {
                        target
                            .try_send(ServerCommand::Message(Message::CancelGame {
                                user: host_id,
                            }))
                            .await;
                    }
                    target.try_send(ServerCommand::ChangeState(new_state)).await;
                }
            }
        }
        if done_tx.send(Some(Arc::downgrade(&res))).is_err() || res.stream.is_closed() {
            res.mark_lost();
        }
        Ok(res)
    }

    pub fn version(&self) -> u8 {
        self.stream.version()
    }

    pub fn name(&self) -> &str {
        &self.user.name
    }

    pub fn mark_lost(&self) {
        if !self.panicked.swap(true, Ordering::SeqCst) {
            self.stream.close();
            self.monitor_task_handle.abort();
            let lost_con_tx = self.user.server.lost_con_tx.clone();
            let id = self.id;
            if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = lost_con_tx.try_send(id) {
                tokio::spawn(async move {
                    if let Err(err) = lost_con_tx.send(id).await {
                        error!("failed to mark lost connection ({id}): {err:?}");
                    }
                });
            }
        }
    }

    pub async fn close_replaced(&self, server: &ServerState) {
        self.panicked.store(true, Ordering::SeqCst);
        self.monitor_task_handle.abort();
        self.stream.close();
        server.sessions.write().await.remove(&self.id);
    }

    pub async fn try_send(&self, cmd: ServerCommand) {
        if let Err(err) = self.stream.try_send(cmd) {
            error!(
                "failed to deliver command to {}, disconnecting: {err:?}",
                self.id
            );
            self.mark_lost();
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.panicked.store(true, Ordering::SeqCst);
        self.monitor_task_handle.abort();
        self.stream.close();
    }
}

fn estimate_touches_bytes(frames: &[TouchFrame]) -> usize {
    let mut total = 16usize;
    for frame in frames {
        total = total
            .saturating_add(9)
            .saturating_add(frame.points.len().saturating_mul(5));
        if total > MAX_PACKET_SIZE {
            return total;
        }
    }
    total
}

fn estimate_judges_bytes(judges: &[JudgeEvent]) -> usize {
    16usize.saturating_add(judges.len().saturating_mul(13))
}

async fn apply_played_record(user: &User, room: &Room, res: Record) -> Result<()> {
    if user.monitor.load(Ordering::SeqCst) {
        bail!("invalid state");
    }
    if res.player != user.id {
        bail!("invalid record");
    }
    let (score, accuracy, full_combo) = {
        let mut guard = room.state.write().await;
        let InternalRoomState::Playing { results, aborted } = guard.deref_mut() else {
            bail!("invalid state");
        };
        if !room.contains_user(user.id).await {
            bail!("no room");
        }
        if aborted.contains(&user.id) {
            bail!("aborted");
        }
        if results.contains_key(&user.id) {
            bail!("already uploaded");
        }
        if !room.used_records.lock().await.insert(res.id) {
            bail!("duplicate record");
        }
        let summary = (res.score, res.accuracy, res.full_combo);
        results.insert(user.id, res);
        summary
    };
    debug!(
        room = room.id.to_string(),
        user = user.id,
        score,
        accuracy,
        full_combo,
        "user played"
    );
    room.send(Message::Played {
        user: user.id,
        score,
        accuracy,
        full_combo,
    })
    .await;
    room.check_all_ready().await;
    Ok(())
}

async fn process(user: Arc<User>, cmd: ClientCommand) -> Option<ServerCommand> {
    #[inline]
    fn err_to_str<T>(result: Result<T>) -> Result<T, String> {
        result.map_err(|it| it.to_string())
    }

    macro_rules! get_room {
        (~ $d:ident) => {
            let $d = match user.room.read().await.as_ref().map(Arc::clone) {
                Some(room) => room,
                None => {
                    warn!("no room");
                    return None;
                }
            };
        };
        ($d:ident) => {
            let $d = user
                .room
                .read()
                .await
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| anyhow!("no room"))?;
        };
    }
    match cmd {
        ClientCommand::Ping => unreachable!(),
        ClientCommand::Authenticate { .. } => Some(ServerCommand::Authenticate(Err(
            "repeated authenticate".to_owned(),
        ))),
        ClientCommand::Chat { message } => {
            let res: Result<()> = async move {
                get_room!(room);
                if !user.chat_limit.lock().unwrap().take(10.0, 5.0) {
                    bail!("rate limited");
                }
                room.send_as(&user, message.into_inner()).await;
                Ok(())
            }
            .await;
            Some(ServerCommand::Chat(err_to_str(res)))
        }
        ClientCommand::Touches { frames } => {
            get_room!(~ room);
            if user.monitor.load(Ordering::SeqCst) {
                warn!("monitor {} attempted to send touch events", user.id);
                return None;
            }
            if !user.live_frame_limit.lock().unwrap().take(300.0, 200.0) {
                warn!("rate limiting touch events from {}", user.id);
                return None;
            }
            if estimate_touches_bytes(&frames) > MAX_PACKET_SIZE - 16 {
                warn!("dropping oversized touch frame batch from {}", user.id);
                return None;
            }
            if room.is_live()
                && matches!(*room.state.read().await, InternalRoomState::Playing { .. })
            {
                debug!("received {} touch events from {}", frames.len(), user.id);
                if let Some(frame) = frames.last() {
                    user.game_time.store(frame.time.to_bits(), Ordering::SeqCst);
                }
                room.queue_monitor(ServerCommand::Touches {
                    player: user.id,
                    frames,
                });
            } else {
                warn!("received touch events in non-live or non-playing mode");
            }
            None
        }
        ClientCommand::Judges { judges } => {
            get_room!(~ room);
            if user.monitor.load(Ordering::SeqCst) {
                warn!("monitor {} attempted to send judge events", user.id);
                return None;
            }
            if !user.live_frame_limit.lock().unwrap().take(300.0, 200.0) {
                warn!("rate limiting judge events from {}", user.id);
                return None;
            }
            if estimate_judges_bytes(&judges) > MAX_PACKET_SIZE - 16 {
                warn!("dropping oversized judge batch from {}", user.id);
                return None;
            }
            if room.is_live()
                && matches!(*room.state.read().await, InternalRoomState::Playing { .. })
            {
                debug!("received {} judge events from {}", judges.len(), user.id);
                room.queue_monitor(ServerCommand::Judges {
                    player: user.id,
                    judges,
                });
            } else {
                warn!("received judge events in non-live or non-playing mode");
            }
            None
        }
        ClientCommand::CreateRoom { id } => {
            let res: Result<()> = async move {
                if !user.room_op_limit.lock().unwrap().take(15.0, 5.0) {
                    bail!("rate limited");
                }
                let mut room_guard = user.room.write().await;
                if room_guard.is_some() {
                    bail!("already in room");
                }

                let room = {
                    let mut map_guard = user.server.rooms.write().await;
                    if map_guard.contains_key(&id) {
                        bail!(tl!("create-id-occupied"));
                    }
                    user.monitor.store(false, Ordering::SeqCst);
                    let room = Arc::new(Room::new(id.clone(), Arc::downgrade(&user)));
                    map_guard.insert(id.clone(), Arc::clone(&room));
                    room
                };
                *room_guard = Some(Arc::clone(&room));
                drop(room_guard);
                room.send(Message::CreateRoom { user: user.id }).await;

                info!(user = user.id, room = id.to_string(), "user create room");
                Ok(())
            }
            .await;
            Some(ServerCommand::CreateRoom(err_to_str(res)))
        }
        ClientCommand::JoinRoom { id, monitor } => {
            let res: Result<()> = async move {
                if !user.room_op_limit.lock().unwrap().take(15.0, 5.0) {
                    bail!("rate limited");
                }
                let mut room_guard = user.room.write().await;
                if room_guard.is_some() {
                    bail!("already in room");
                }
                let room = user.server.rooms.read().await.get(&id).map(Arc::clone);
                let Some(room) = room else {
                    bail!("room not found");
                };
                let member_guard = room.membership_lock.lock().await;
                let still_active = user
                    .server
                    .rooms
                    .read()
                    .await
                    .get(&id)
                    .is_some_and(|r| Arc::ptr_eq(r, &room));
                if !still_active || room.is_dropped() {
                    bail!("room not found");
                }
                if room.locked.load(Ordering::SeqCst) {
                    bail!(tl!("join-room-locked"));
                }
                if monitor && !user.can_monitor() {
                    bail!(tl!("join-cant-monitor"));
                }
                let state_guard = room.state.read().await;
                if !matches!(*state_guard, InternalRoomState::SelectChart) {
                    bail!(tl!("join-game-ongoing"));
                }
                if room.contains_user(user.id).await {
                    bail!("already in room");
                }
                match room.add_user(Arc::downgrade(&user), monitor).await {
                    AddUserResult::Added => {}
                    AddUserResult::Full => bail!(tl!("join-room-full")),
                    AddUserResult::Dropped => bail!("room not found"),
                }
                info!(
                    user = user.id,
                    room = id.to_string(),
                    monitor,
                    "user join room"
                );
                user.monitor.store(monitor, Ordering::SeqCst);
                if monitor && !room.live.fetch_or(true, Ordering::SeqCst) {
                    info!(room = id.to_string(), "room goes live");
                }
                *room_guard = Some(Arc::clone(&room));
                let join_resp = JoinRoomResponse {
                    state: state_guard.to_client(room.chart.read().await.as_ref().map(|c| c.id)),
                    users: room
                        .users()
                        .await
                        .into_iter()
                        .chain(room.monitors().await)
                        .map(|it| it.to_info())
                        .collect(),
                    live: room.is_live(),
                };
                // Deliver JoinRoom(Ok) to the joiner before broadcasting OnJoinRoom/Message::JoinRoom
                // so the client initializes `room` state prior to receiving room broadcasts.
                user.try_send(ServerCommand::JoinRoom(Ok(join_resp))).await;
                drop(state_guard);
                drop(member_guard);
                drop(room_guard);
                room.broadcast(ServerCommand::OnJoinRoom(user.to_info()))
                    .await;
                room.send(Message::JoinRoom {
                    user: user.id,
                    name: user.name.clone(),
                })
                .await;
                Ok(())
            }
            .await;
            match res {
                Ok(()) => None,
                Err(err) => Some(ServerCommand::JoinRoom(Err(err.to_string()))),
            }
        }
        ClientCommand::LeaveRoom => {
            let res: Result<()> = async move {
                get_room!(room);
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    "user leave room"
                );
                if room.on_user_leave(&user).await {
                    let mut rooms = user.server.rooms.write().await;
                    if rooms.get(&room.id).is_some_and(|r| Arc::ptr_eq(r, &room)) {
                        rooms.remove(&room.id);
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::LeaveRoom(err_to_str(res)))
        }
        ClientCommand::LockRoom { lock } => {
            let res: Result<()> = async move {
                get_room!(room);
                room.check_host(&user).await?;
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    lock,
                    "lock room"
                );
                room.locked.store(lock, Ordering::SeqCst);
                room.send(Message::LockRoom { lock }).await;
                Ok(())
            }
            .await;
            Some(ServerCommand::LockRoom(err_to_str(res)))
        }
        ClientCommand::CycleRoom { cycle } => {
            let res: Result<()> = async move {
                get_room!(room);
                room.check_host(&user).await?;
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    cycle,
                    "cycle room"
                );
                room.cycle.store(cycle, Ordering::SeqCst);
                room.send(Message::CycleRoom { cycle }).await;
                Ok(())
            }
            .await;
            Some(ServerCommand::CycleRoom(err_to_str(res)))
        }
        ClientCommand::SelectChart { id } => {
            let res: Result<()> = async move {
                if !user.room_op_limit.lock().unwrap().take(15.0, 5.0) {
                    bail!("rate limited");
                }
                get_room!(room);
                if !matches!(*room.state.read().await, InternalRoomState::SelectChart) {
                    bail!("invalid state");
                }
                room.check_host(&user).await?;
                let span = debug_span!(
                    "select chart",
                    user = user.id,
                    room = room.id.to_string(),
                    chart = id,
                );
                async move {
                    trace!("fetch");
                    let res: Chart = user
                        .server
                        .http_client
                        .get(format!("{HOST}/chart/{id}"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    debug!("chart is {res:?}");
                    let (chart_name, chart_id, room_state) = {
                        let state_guard = room.state.write().await;
                        if !matches!(*state_guard, InternalRoomState::SelectChart) {
                            bail!("invalid state");
                        }
                        if !room.contains_user(user.id).await {
                            bail!("no room");
                        }
                        room.check_host(&user).await?;
                        let name = res.name.clone();
                        let cid = res.id;
                        *room.chart.write().await = Some(res);
                        let room_state = state_guard.to_client(Some(cid));
                        (name, cid, room_state)
                    };
                    room.send(Message::SelectChart {
                        user: user.id,
                        name: chart_name,
                        id: chart_id,
                    })
                    .await;
                    room.broadcast(ServerCommand::ChangeState(room_state)).await;
                    Ok(())
                }
                .instrument(span)
                .await
            }
            .await;
            Some(ServerCommand::SelectChart(err_to_str(res)))
        }

        ClientCommand::RequestStart => {
            let res: Result<()> = async move {
                get_room!(room);
                let client_state = {
                    let mut state_guard = room.state.write().await;
                    if !matches!(*state_guard, InternalRoomState::SelectChart) {
                        bail!("invalid state");
                    }
                    room.check_host(&user).await?;
                    let chart_id = room
                        .chart
                        .read()
                        .await
                        .as_ref()
                        .map(|c| c.id)
                        .ok_or_else(|| anyhow!(tl!("start-no-chart-selected")))?;
                    debug!(room = room.id.to_string(), "room wait for ready");
                    *state_guard = InternalRoomState::WaitForReady {
                        started: std::iter::once(user.id).collect::<HashSet<_>>(),
                    };
                    state_guard.to_client(Some(chart_id))
                };
                room.reset_game_time().await;
                room.send(Message::GameStart { user: user.id }).await;
                room.broadcast(ServerCommand::ChangeState(client_state))
                    .await;
                room.check_all_ready().await;
                Ok(())
            }
            .await;
            Some(ServerCommand::RequestStart(err_to_str(res)))
        }
        ClientCommand::Ready => {
            let res: Result<()> = async move {
                get_room!(room);
                {
                    let mut guard = room.state.write().await;
                    let InternalRoomState::WaitForReady { started } = guard.deref_mut() else {
                        bail!("invalid state");
                    };
                    if !room.contains_user(user.id).await {
                        bail!("no room");
                    }
                    if !started.insert(user.id) {
                        bail!("already ready");
                    }
                }
                room.send(Message::Ready { user: user.id }).await;
                room.check_all_ready().await;
                Ok(())
            }
            .await;
            Some(ServerCommand::Ready(err_to_str(res)))
        }
        ClientCommand::CancelReady => {
            let res: Result<()> = async move {
                get_room!(room);
                enum CancelOutcome {
                    CancelGame(phira_mp_common::RoomState),
                    CancelReady,
                }
                let outcome = {
                    let mut guard = room.state.write().await;
                    let InternalRoomState::WaitForReady { started } = guard.deref_mut() else {
                        bail!("invalid state");
                    };
                    if room.check_host(&user).await.is_ok() {
                        *guard = InternalRoomState::SelectChart;
                        let chart_id = room.chart.read().await.as_ref().map(|c| c.id);
                        CancelOutcome::CancelGame(guard.to_client(chart_id))
                    } else {
                        if !started.remove(&user.id) {
                            bail!("not ready");
                        }
                        CancelOutcome::CancelReady
                    }
                };
                match outcome {
                    CancelOutcome::CancelGame(room_state) => {
                        room.send(Message::CancelGame { user: user.id }).await;
                        room.broadcast(ServerCommand::ChangeState(room_state)).await;
                    }
                    CancelOutcome::CancelReady => {
                        room.send(Message::CancelReady { user: user.id }).await;
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::CancelReady(err_to_str(res)))
        }
        ClientCommand::Played { id } => {
            let res: Result<()> = async move {
                if !user.room_op_limit.lock().unwrap().take(15.0, 5.0) {
                    bail!("rate limited");
                }
                get_room!(room);
                if user.monitor.load(Ordering::SeqCst) {
                    bail!("invalid state");
                }
                {
                    let guard = room.state.read().await;
                    let InternalRoomState::Playing { results, aborted } = &*guard else {
                        bail!("invalid state");
                    };
                    if aborted.contains(&user.id) {
                        bail!("aborted");
                    }
                    if results.contains_key(&user.id) {
                        bail!("already uploaded");
                    }
                }
                let res: Record = user
                    .server
                    .http_client
                    .get(format!("{HOST}/record/{id}"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if res.id != id {
                    bail!("invalid record");
                }
                apply_played_record(&user, &room, res).await?;
                Ok(())
            }
            .await;
            Some(ServerCommand::Played(err_to_str(res)))
        }
        ClientCommand::Abort => {
            let res: Result<()> = async move {
                get_room!(room);
                if user.monitor.load(Ordering::SeqCst) {
                    bail!("invalid state");
                }
                {
                    let mut guard = room.state.write().await;
                    let InternalRoomState::Playing { results, aborted } = guard.deref_mut() else {
                        bail!("invalid state");
                    };
                    if results.contains_key(&user.id) {
                        bail!("already uploaded");
                    }
                    if !aborted.insert(user.id) {
                        bail!("aborted");
                    }
                }
                room.send(Message::Abort { user: user.id }).await;
                room.check_all_ready().await;
                Ok(())
            }
            .await;
            Some(ServerCommand::Abort(err_to_str(res)))
        }
    }
}

#[cfg(test)]
impl Session {
    pub(crate) async fn connect_test_user(
        id: Uuid,
        tcp_stream: TcpStream,
        user: Arc<User>,
        heartbeat_timeout: Duration,
    ) -> Arc<Self> {
        let this = Arc::new(OnceCell::<Weak<Session>>::new());
        let last_recv = Arc::new(std::sync::Mutex::new(Instant::now()));
        let panicked = Arc::new(AtomicBool::new(false));
        let waiting_for_authenticate = Arc::new(AtomicBool::new(false));
        let handler = build_stream_handler(
            id,
            Arc::clone(&user.server),
            Arc::clone(&this),
            Arc::clone(&last_recv),
            Arc::clone(&panicked),
            None,
            waiting_for_authenticate,
        );
        let stream = Stream::<ServerCommand, ClientCommand>::new(None, tcp_stream, handler)
            .await
            .unwrap();
        Self::install(
            id,
            stream,
            user,
            last_recv,
            panicked,
            &this,
            heartbeat_timeout,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MAX_UNAUTHENTICATED_CONNS, Server, ServerConfig};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Semaphore, mpsc},
    };

    async fn loopback_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_fut = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server_sock, _) = listener.accept().await.unwrap();
        let client_sock = client_fut.await.unwrap();
        (server_sock, client_sock)
    }

    fn dummy_state() -> (Arc<ServerState>, mpsc::Receiver<Uuid>) {
        let (lost_con_tx, lost_con_rx) = mpsc::channel(256);
        let state = Arc::new(ServerState {
            config: ServerConfig::default(),
            http_client: reqwest::Client::new(),
            unauth_sem: Arc::new(Semaphore::new(MAX_UNAUTHENTICATED_CONNS)),
            sessions: Default::default(),
            users: Default::default(),
            rooms: Default::default(),
            lost_con_tx,
        });
        (state, lost_con_rx)
    }

    async fn spawn_test_server() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Server::from(listener)
    }

    #[tokio::test]
    async fn full_send_queue_kicks_session_without_blocking() {
        let (lost_con_tx, mut lost_con_rx) = mpsc::channel(16);
        let state = Arc::new(ServerState {
            config: ServerConfig::default(),
            http_client: reqwest::Client::new(),
            unauth_sem: Arc::new(Semaphore::new(MAX_UNAUTHENTICATED_CONNS)),
            sessions: Default::default(),
            users: Default::default(),
            rooms: Default::default(),
            lost_con_tx,
        });
        let user = Arc::new(User::new(
            1,
            "alice".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer_handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            sock // never reads
        });
        let sock = TcpStream::connect(addr).await.unwrap();
        let stream =
            Stream::<ServerCommand, ClientCommand>::new(Some(1), sock, Box::new(|_, _| async {}))
                .await
                .unwrap();
        let _peer = peer_handle.await.unwrap();

        let id = Uuid::new_v4();
        let session = Arc::new(Session {
            id,
            stream,
            user: Arc::clone(&user),
            panicked: Arc::new(AtomicBool::new(false)),
            monitor_task_handle: tokio::spawn(async {}),
        });
        user.set_session(Arc::downgrade(&session)).await;

        let big = "x".repeat(60_000);
        for _ in 0..2_000 {
            session
                .try_send(ServerCommand::Message(Message::Chat {
                    user: 1,
                    content: big.clone(),
                }))
                .await;
        }

        let kicked_id = time::timeout(Duration::from_millis(500), lost_con_rx.recv())
            .await
            .expect("timed out waiting for slow session to be marked lost")
            .expect("lost_con channel closed");
        assert_eq!(kicked_id, id);
    }

    /// Regression test for M-4 (ported from `scratch/audit`): a dangling user who is not in any
    /// room must still be evicted from `server.users` once the dangle timeout elapses.
    #[tokio::test]
    async fn dangling_user_without_room_is_evicted() {
        let (lost_con_tx, _lost_con_rx) = mpsc::channel(16);
        let state = Arc::new(ServerState {
            config: ServerConfig::default(),
            http_client: reqwest::Client::new(),
            unauth_sem: Arc::new(Semaphore::new(MAX_UNAUTHENTICATED_CONNS)),
            sessions: Default::default(),
            users: Default::default(),
            rooms: Default::default(),
            lost_con_tx,
        });
        let u = Arc::new(User::new(
            42,
            "u42".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        state.users.write().await.insert(42, Arc::clone(&u));
        u.dangle().await;
        time::sleep(Duration::from_secs(11)).await;
        assert!(
            !state.users.read().await.contains_key(&42),
            "dangling user without room should be removed from server.users"
        );
    }

    #[tokio::test]
    async fn session_has_no_reference_cycle_and_drops_on_remove() {
        let (state, _lost_rx) = dummy_state();
        let user = Arc::new(User::new(
            10,
            "u10".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        state.users.write().await.insert(10, Arc::clone(&user));

        let (server_sock, mut client_sock) = loopback_pair().await;
        client_sock.write_all(&[1]).await.unwrap();

        let id = Uuid::new_v4();
        let session =
            Session::connect_test_user(id, server_sock, user, Duration::from_secs(60)).await;
        let weak = Arc::downgrade(&session);
        drop(session);
        assert_eq!(weak.strong_count(), 1);

        // Removing the session from `server.sessions` must drop it immediately (no ref cycle).
        state.sessions.write().await.remove(&id);
        tokio::task::yield_now().await;
        assert_eq!(
            weak.strong_count(),
            0,
            "Session still has strong references after removal (reference cycle?)"
        );

        let mut buf = [0u8; 1];
        let n = time::timeout(Duration::from_millis(500), client_sock.read(&mut buf))
            .await
            .expect("peer did not see EOF after session drop")
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn heartbeat_timeout_closes_stream_and_blocks_zombie_commands() {
        let server = spawn_test_server().await;
        let user = Arc::new(User::new(
            11,
            "u11".to_owned(),
            Language::default(),
            Arc::clone(&server.state),
        ));
        server
            .state
            .users
            .write()
            .await
            .insert(11, Arc::clone(&user));

        let (server_sock, mut client_sock) = loopback_pair().await;
        client_sock.write_all(&[1]).await.unwrap();

        let id = Uuid::new_v4();
        let session = Session::connect_test_user(
            id,
            server_sock,
            Arc::clone(&user),
            Duration::from_millis(60),
        )
        .await;
        let weak = Arc::downgrade(&session);
        drop(session);

        // Wait past the 60ms heartbeat timeout so monitor task marks session lost & closes socket.
        time::sleep(Duration::from_millis(200)).await;
        assert!(
            !server.state.sessions.read().await.contains_key(&id),
            "timed-out session must be removed from server.sessions"
        );
        assert_eq!(weak.strong_count(), 0);

        // Any buffered or subsequent Ping/command must not receive Pong.
        let _ = client_sock.write_all(&[1, 0]).await;
        let mut buf = [0u8; 16];
        match time::timeout(Duration::from_millis(500), client_sock.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            other => panic!("expected EOF/error on closed zombie socket, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dangle_timer_does_not_evict_reconnected_user() {
        let (state, _lost_rx) = dummy_state();
        let user = Arc::new(User::new(
            12,
            "u12".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        state.users.write().await.insert(12, Arc::clone(&user));
        let room = Arc::new(Room::new(
            "r12".to_owned().try_into().unwrap(),
            Arc::downgrade(&user),
        ));
        *user.room.write().await = Some(Arc::clone(&room));
        state
            .rooms
            .write()
            .await
            .insert(room.id.clone(), Arc::clone(&room));

        let (s1_srv, mut s1_cli) = loopback_pair().await;
        s1_cli.write_all(&[1]).await.unwrap();
        let s1_id = Uuid::new_v4();
        let s1 =
            Session::connect_test_user(s1_id, s1_srv, Arc::clone(&user), Duration::from_secs(60))
                .await;
        drop(s1);

        // Trigger dangle timer with 80ms grace period.
        Arc::clone(&user)
            .dangle_with_delay(Some(s1_id), Duration::from_millis(80))
            .await;
        time::sleep(Duration::from_millis(20)).await;

        // User reconnects with s2 before dangle expires.
        let (s2_srv, mut s2_cli) = loopback_pair().await;
        s2_cli.write_all(&[1]).await.unwrap();
        let s2_id = Uuid::new_v4();
        let _s2 =
            Session::connect_test_user(s2_id, s2_srv, Arc::clone(&user), Duration::from_secs(60))
                .await;

        // Wait past old dangle timer expiration.
        time::sleep(Duration::from_millis(120)).await;
        assert!(
            state.users.read().await.contains_key(&12),
            "reconnected user was wrongly evicted by old dangle timer"
        );
        assert!(
            user.room.read().await.is_some(),
            "reconnected user lost room during stale dangle cleanup"
        );

        // Also test Playing branch: stale disconnect for s1_id after s2 reconnected must not abort.
        *room.state.write().await = InternalRoomState::Playing {
            results: std::collections::HashMap::new(),
            aborted: HashSet::new(),
        };
        Arc::clone(&user)
            .dangle_with_delay(Some(s1_id), Duration::from_millis(80))
            .await;
        assert!(state.users.read().await.contains_key(&12));
        assert!(room.contains_user(12).await);
    }

    #[tokio::test]
    async fn reconnect_kicks_old_session() {
        let (state, _lost_rx) = dummy_state();
        let user = Arc::new(User::new(
            13,
            "u13".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        state.users.write().await.insert(13, Arc::clone(&user));

        let (s1_srv, mut s1_cli) = loopback_pair().await;
        s1_cli.write_all(&[1]).await.unwrap();
        let s1_id = Uuid::new_v4();
        let s1 =
            Session::connect_test_user(s1_id, s1_srv, Arc::clone(&user), Duration::from_secs(60))
                .await;
        let weak_s1 = Arc::downgrade(&s1);
        drop(s1);

        let (s2_srv, mut s2_cli) = loopback_pair().await;
        s2_cli.write_all(&[1]).await.unwrap();
        let s2_id = Uuid::new_v4();
        let _s2 =
            Session::connect_test_user(s2_id, s2_srv, Arc::clone(&user), Duration::from_secs(60))
                .await;

        tokio::task::yield_now().await;
        assert!(!state.sessions.read().await.contains_key(&s1_id));
        assert!(state.sessions.read().await.contains_key(&s2_id));
        assert_eq!(weak_s1.strong_count(), 0);

        let mut buf = [0u8; 1];
        let n = time::timeout(Duration::from_millis(500), s1_cli.read(&mut buf))
            .await
            .expect("replaced client socket was not closed")
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn lost_con_handle_handles_burst_disconnects() {
        let server = spawn_test_server().await;
        let mut _peers = Vec::new();
        let mut ids = Vec::new();
        for i in 0..40 {
            let user = Arc::new(User::new(
                1000 + i,
                format!("burst{i}"),
                Language::default(),
                Arc::clone(&server.state),
            ));
            server
                .state
                .users
                .write()
                .await
                .insert(user.id, Arc::clone(&user));
            let (srv, mut cli) = loopback_pair().await;
            cli.write_all(&[1]).await.unwrap();
            let id = Uuid::new_v4();
            let _s = Session::connect_test_user(id, srv, user, Duration::from_secs(60)).await;
            ids.push(id);
            _peers.push(cli);
        }
        assert_eq!(server.state.sessions.read().await.len(), 40);

        // Interleave unknown UUIDs and all 40 real IDs.
        for id in ids {
            let _ = server.state.lost_con_tx.send(Uuid::new_v4()).await;
            server.state.lost_con_tx.send(id).await.unwrap();
        }

        time::timeout(Duration::from_secs(2), async {
            loop {
                if server.state.sessions.read().await.is_empty() {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for burst disconnects to clean up");
    }

    async fn connect_client_pair(
        state: &Arc<ServerState>,
        user_id: i32,
        name: &str,
    ) -> (
        Arc<User>,
        Stream<ClientCommand, ServerCommand>,
        mpsc::UnboundedReceiver<ServerCommand>,
    ) {
        let user = Arc::new(User::new(
            user_id,
            name.to_owned(),
            Language::default(),
            Arc::clone(state),
        ));
        state.users.write().await.insert(user_id, Arc::clone(&user));
        let (srv_sock, cli_sock) = loopback_pair().await;
        let (tx, rx) = mpsc::unbounded_channel();
        let client_fut = Stream::<ClientCommand, ServerCommand>::new(
            Some(1),
            cli_sock,
            Box::new(move |_, cmd| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(cmd);
                }
            }),
        );
        let server_fut = Session::connect_test_user(
            Uuid::new_v4(),
            srv_sock,
            Arc::clone(&user),
            Duration::from_secs(60),
        );
        let (client_res, _session) = tokio::join!(client_fut, server_fut);
        (user, client_res.unwrap(), rx)
    }

    async fn recv_cmd(rx: &mut mpsc::UnboundedReceiver<ServerCommand>) -> ServerCommand {
        time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for ServerCommand")
            .expect("ServerCommand stream closed")
    }

    #[tokio::test]
    async fn join_room_delivers_ok_before_broadcast_and_uses_ptr_eq_on_leave() {
        let (state, _lost_rx) = dummy_state();
        let (host, host_cli, mut host_rx) = connect_client_pair(&state, 1, "host").await;
        let (joiner, joiner_cli, mut joiner_rx) = connect_client_pair(&state, 2, "joiner").await;

        let rid: phira_mp_common::RoomId = "order-room".to_owned().try_into().unwrap();
        host_cli
            .send(ClientCommand::CreateRoom { id: rid.clone() })
            .await
            .unwrap();
        // Wait until CreateRoom settles cleanly.
        let _ = recv_cmd(&mut host_rx).await;
        let _ = recv_cmd(&mut host_rx).await;

        joiner_cli
            .send(ClientCommand::JoinRoom {
                id: rid.clone(),
                monitor: false,
            })
            .await
            .unwrap();

        // Joiner MUST receive ServerCommand::JoinRoom(Ok(...)) strictly BEFORE OnJoinRoom / Message::JoinRoom.
        let first = recv_cmd(&mut joiner_rx).await;
        assert!(
            matches!(first, ServerCommand::JoinRoom(Ok(_))),
            "expected JoinRoom(Ok) first, got {first:?}"
        );
        let second = recv_cmd(&mut joiner_rx).await;
        assert!(matches!(second, ServerCommand::OnJoinRoom(_)));
        let third = recv_cmd(&mut joiner_rx).await;
        assert!(matches!(
            third,
            ServerCommand::Message(Message::JoinRoom { user: 2, .. })
        ));

        // Also test that LeaveRoom checks Arc::ptr_eq before deleting a recreated room.
        let old_room = Arc::clone(host.room.read().await.as_ref().unwrap());
        let new_room = Arc::new(Room::new(rid.clone(), Arc::downgrade(&joiner)));
        state
            .rooms
            .write()
            .await
            .insert(rid.clone(), Arc::clone(&new_room));
        assert!(!old_room.on_user_leave(&joiner).await);
        assert!(matches!(
            process(Arc::clone(&host), ClientCommand::LeaveRoom).await,
            Some(ServerCommand::LeaveRoom(Ok(())))
        ));
        assert!(
            Arc::ptr_eq(state.rooms.read().await.get(&rid).unwrap(), &new_room),
            "leaving old room must not delete newly recreated room with the same ID"
        );
    }

    #[tokio::test]
    async fn cancel_ready_allows_new_host_not_in_started_and_rejects_wrong_state() {
        let (state, _lost_rx) = dummy_state();
        let host = Arc::new(User::new(
            1,
            "host".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        let p2 = Arc::new(User::new(
            2,
            "p2".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        let rid: phira_mp_common::RoomId = "cancel-room".to_owned().try_into().unwrap();
        let room = Arc::new(Room::new(rid.clone(), Arc::downgrade(&host)));
        assert_eq!(
            room.add_user(Arc::downgrade(&p2), false).await,
            AddUserResult::Added
        );
        *host.room.write().await = Some(Arc::clone(&room));
        *p2.room.write().await = Some(Arc::clone(&room));
        *room.chart.write().await = Some(Chart {
            id: 10,
            name: "test".to_owned(),
        });
        state.rooms.write().await.insert(rid, Arc::clone(&room));

        // Ready / CancelReady / Abort in SelectChart must all return Err (not silent Ok).
        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::Ready).await,
            Some(ServerCommand::Ready(Err(_)))
        ));
        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::CancelReady).await,
            Some(ServerCommand::CancelReady(Err(_)))
        ));
        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::Abort).await,
            Some(ServerCommand::Abort(Err(_)))
        ));

        // Host requests start -> WaitForReady { started: {1} }.
        assert!(matches!(
            process(Arc::clone(&host), ClientCommand::RequestStart).await,
            Some(ServerCommand::RequestStart(Ok(())))
        ));

        // Original host leaves; p2 becomes new host even though p2 is NOT in `started`.
        assert!(!room.on_user_leave(&host).await);
        assert_eq!(room.host.read().await.upgrade().unwrap().id, 2);

        // New host p2 must be able to cancel the game back to SelectChart.
        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::CancelReady).await,
            Some(ServerCommand::CancelReady(Ok(())))
        ));
        assert!(matches!(
            *room.state.read().await,
            InternalRoomState::SelectChart
        ));
    }

    #[tokio::test]
    async fn played_validation_rejects_monitor_wrong_state_aborted_and_duplicate_record() {
        let (state, _lost_rx) = dummy_state();
        let p1 = Arc::new(User::new(
            1,
            "p1".into(),
            Language::default(),
            Arc::clone(&state),
        ));
        let p2 = Arc::new(User::new(
            2,
            "p2".into(),
            Language::default(),
            Arc::clone(&state),
        ));
        let mon = Arc::new(User::new(
            3,
            "mon".into(),
            Language::default(),
            Arc::clone(&state),
        ));
        mon.monitor.store(true, Ordering::SeqCst);

        let room = Arc::new(Room::new(
            "play-room".to_owned().try_into().unwrap(),
            Arc::downgrade(&p1),
        ));
        assert_eq!(
            room.add_user(Arc::downgrade(&p2), false).await,
            AddUserResult::Added
        );
        assert_eq!(
            room.add_user(Arc::downgrade(&mon), true).await,
            AddUserResult::Added
        );
        *p1.room.write().await = Some(Arc::clone(&room));
        *p2.room.write().await = Some(Arc::clone(&room));
        *mon.room.write().await = Some(Arc::clone(&room));

        // Wrong state (SelectChart) rejected.
        assert!(
            apply_played_record(&p1, &room, make_rec(501, 1))
                .await
                .is_err()
        );

        // Enter Playing.
        *room.state.write().await = InternalRoomState::Playing {
            results: std::collections::HashMap::new(),
            aborted: HashSet::new(),
        };
        // Monitor cannot submit Played or Abort.
        assert!(
            apply_played_record(&mon, &room, make_rec(502, 3))
                .await
                .is_err()
        );
        assert!(matches!(
            process(Arc::clone(&mon), ClientCommand::Abort).await,
            Some(ServerCommand::Abort(Err(_)))
        ));
        // Wrong player record rejected.
        assert!(
            apply_played_record(&p1, &room, make_rec(503, 2))
                .await
                .is_err()
        );
        // Aborted player cannot submit Played.
        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::Abort).await,
            Some(ServerCommand::Abort(Ok(())))
        ));
        assert!(
            apply_played_record(&p2, &room, make_rec(504, 2))
                .await
                .is_err()
        );

        // Valid upload finishes round 1 -> transitions to SelectChart.
        assert!(
            apply_played_record(&p1, &room, make_rec(505, 1))
                .await
                .is_ok()
        );
        assert!(matches!(
            *room.state.read().await,
            InternalRoomState::SelectChart
        ));

        // Re-using the exact same record ID 505 in round 2 must be rejected.
        *room.state.write().await = InternalRoomState::Playing {
            results: std::collections::HashMap::new(),
            aborted: HashSet::new(),
        };
        assert!(
            apply_played_record(&p1, &room, make_rec(505, 1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn touches_enforces_playing_non_monitor_and_preserves_fifo_order() {
        let (mut state, _lost_rx) = dummy_state();
        Arc::get_mut(&mut state).unwrap().config.monitors = vec![2];
        let (p1, p1_cli, _p1_rx) = connect_client_pair(&state, 1, "p1").await;
        let (_mon, mon_cli, mut mon_rx) = connect_client_pair(&state, 2, "mon").await;

        let rid: phira_mp_common::RoomId = "live-room".to_owned().try_into().unwrap();
        let room = Arc::new(Room::new(rid.clone(), Arc::downgrade(&p1)));
        *p1.room.write().await = Some(Arc::clone(&room));
        state
            .rooms
            .write()
            .await
            .insert(rid.clone(), Arc::clone(&room));

        mon_cli
            .send(ClientCommand::JoinRoom {
                id: rid,
                monitor: true,
            })
            .await
            .unwrap();
        // Wait for JoinRoom(Ok) + OnJoinRoom + JoinRoom message on monitor.
        let _ = recv_cmd(&mut mon_rx).await;
        let _ = recv_cmd(&mut mon_rx).await;
        let _ = recv_cmd(&mut mon_rx).await;
        assert!(room.is_live());

        // Touches during SelectChart must be ignored (monitor receives nothing).
        p1_cli
            .send(ClientCommand::Touches {
                frames: Arc::new(vec![TouchFrame {
                    time: 999.0,
                    points: vec![],
                }]),
            })
            .await
            .unwrap();
        assert!(
            time::timeout(Duration::from_millis(80), mon_rx.recv())
                .await
                .is_err()
        );

        // Enter Playing.
        *room.state.write().await = InternalRoomState::Playing {
            results: std::collections::HashMap::new(),
            aborted: HashSet::new(),
        };

        // Touches sent by monitor must be ignored.
        mon_cli
            .send(ClientCommand::Touches {
                frames: Arc::new(vec![TouchFrame {
                    time: 888.0,
                    points: vec![],
                }]),
            })
            .await
            .unwrap();

        // Send 20 touch packets sequentially from p1; monitor must receive all 20 in exact order.
        for i in 0..20u32 {
            p1_cli
                .send(ClientCommand::Touches {
                    frames: Arc::new(vec![TouchFrame {
                        time: i as f32,
                        points: vec![],
                    }]),
                })
                .await
                .unwrap();
        }
        for i in 0..20u32 {
            match recv_cmd(&mut mon_rx).await {
                ServerCommand::Touches { player, frames } => {
                    assert_eq!(player, 1);
                    assert_eq!(frames.len(), 1);
                    assert_eq!(frames[0].time, i as f32);
                }
                other => panic!("unexpected command {other:?}"),
            }
        }
    }

    fn make_rec(rec_id: i32, player: i32) -> Record {
        Record {
            id: rec_id,
            player,
            score: 900_000,
            perfect: 100,
            good: 0,
            bad: 0,
            miss: 0,
            max_combo: 100,
            accuracy: 1.0,
            full_combo: true,
            std: 0.0,
            std_score: 0.0,
        }
    }

    #[tokio::test]
    async fn ranking_uses_current_host_and_preserves_finished_leaver_score() {
        let (state, _lost_rx) = dummy_state();
        let (p1, _p1_cli, _p1_rx) = connect_client_pair(&state, 1, "p1").await;
        let (p2, _p2_cli, mut p2_rx) = connect_client_pair(&state, 2, "p2").await;

        // p1 is first in room.users, while p2 is the current host.
        let rid: phira_mp_common::RoomId = "rank-host".to_owned().try_into().unwrap();
        let room = Arc::new(Room::new(rid.clone(), Arc::downgrade(&p1)));
        *p1.room.write().await = Some(Arc::clone(&room));
        assert_eq!(
            room.add_user(Arc::downgrade(&p2), false).await,
            AddUserResult::Added
        );
        *p2.room.write().await = Some(Arc::clone(&room));
        *room.host.write().await = Arc::downgrade(&p2);
        room.cycle.store(true, Ordering::SeqCst);
        *room.chart.write().await = Some(crate::Chart {
            id: 1,
            name: "c".to_owned(),
        });

        assert!(matches!(
            process(Arc::clone(&p2), ClientCommand::RequestStart).await,
            Some(ServerCommand::RequestStart(Ok(())))
        ));
        assert!(matches!(
            process(Arc::clone(&p1), ClientCommand::Ready).await,
            Some(ServerCommand::Ready(Ok(())))
        ));
        assert!(matches!(
            *room.state.read().await,
            InternalRoomState::Playing { .. }
        ));

        // p1 finishes and uploads record, then leaves before p2 finishes.
        let mut rec1 = make_rec(601, 1);
        rec1.score = 950_000;
        rec1.accuracy = 0.99;
        assert!(apply_played_record(&p1, &room, rec1).await.is_ok());
        assert!(!room.on_user_leave(&p1).await);

        // Drain earlier broadcasts on p2_rx.
        while p2_rx.try_recv().is_ok() {}

        // p2 finishes; ranking Chat must be sent as p2 (the host), not p1, and retain p1's score.
        let mut rec2 = make_rec(602, 2);
        rec2.score = 900_000;
        rec2.accuracy = 0.95;
        assert!(apply_played_record(&p2, &room, rec2).await.is_ok());

        let mut chat_msg = None;
        while let Ok(Some(cmd)) = time::timeout(Duration::from_millis(50), p2_rx.recv()).await {
            if let ServerCommand::Message(Message::Chat { user, content }) = cmd {
                chat_msg = Some((user, content));
                break;
            }
        }
        let (sender_id, content) = chat_msg.expect("expected ranking Chat message");
        assert_eq!(sender_id, 2, "ranking Chat must be sent as current host p2");
        assert!(content.starts_with("本局房主\n本局排名："));
        assert!(
            content.contains("p1  0950000"),
            "finished leaver score missing: {content}"
        );
        assert!(
            content.contains("p2  0900000"),
            "host score missing: {content}"
        );
        assert!(
            !content.contains("放弃："),
            "finished leaver falsely marked aborted: {content}"
        );
    }

    #[tokio::test]
    async fn finished_player_dangle_gets_grace_period_during_playing() {
        let (state, _lost_rx) = dummy_state();
        let p1 = Arc::new(User::new(
            21,
            "p1".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        let p2 = Arc::new(User::new(
            22,
            "p2".to_owned(),
            Language::default(),
            Arc::clone(&state),
        ));
        state.users.write().await.insert(21, Arc::clone(&p1));
        state.users.write().await.insert(22, Arc::clone(&p2));

        let rid: phira_mp_common::RoomId = "dangle-play".to_owned().try_into().unwrap();
        let room = Arc::new(Room::new(rid.clone(), Arc::downgrade(&p1)));
        *p1.room.write().await = Some(Arc::clone(&room));
        assert_eq!(
            room.add_user(Arc::downgrade(&p2), false).await,
            AddUserResult::Added
        );
        *p2.room.write().await = Some(Arc::clone(&room));
        state.rooms.write().await.insert(rid, Arc::clone(&room));

        let mut results = std::collections::HashMap::new();
        results.insert(21, make_rec(701, 21));
        *room.state.write().await = InternalRoomState::Playing {
            results,
            aborted: HashSet::new(),
        };

        // p1 already finished, so disconnecting gets the grace period instead of immediate kick.
        Arc::clone(&p1)
            .dangle_with_delay(None, Duration::from_millis(120))
            .await;
        assert!(state.users.read().await.contains_key(&21));
        assert!(room.contains_user(21).await);

        // p2 has not finished yet, so disconnecting aborts immediately.
        Arc::clone(&p2)
            .dangle_with_delay(None, Duration::from_millis(120))
            .await;
        assert!(!state.users.read().await.contains_key(&22));
        assert!(!room.contains_user(22).await);
    }
}
