mod bin;
pub use bin::*;

mod command;
pub use command::*;

use anyhow::{Error, Result, bail};
use std::{
    future::Future,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, watch},
    task::JoinHandle,
    time,
};
use tracing::{error, trace, warn};

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);
pub const HEARTBEAT_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const SEND_TIMEOUT: Duration = Duration::from_secs(15);
pub const MAX_PACKET_SIZE: usize = 2 * 1024 * 1024;
const IDLE_BUFFER_CAP: usize = 64 * 1024;

pub fn try_encode_packet(payload: &impl BinaryData, vec: &mut Vec<u8>) -> Result<()> {
    BinaryWriter::new(vec).write(payload)
}

pub fn encode_packet(payload: &impl BinaryData, vec: &mut Vec<u8>) {
    try_encode_packet(payload, vec).unwrap();
}

pub fn decode_packet<T>(data: &[u8]) -> Result<T>
where
    T: BinaryData,
{
    let mut reader = BinaryReader::new(data);
    let val = reader.read()?;
    if reader.remaining() != 0 {
        bail!("trailing bytes ({})", reader.remaining());
    }
    Ok(val)
}

struct CloseGuard(watch::Sender<bool>);

impl Drop for CloseGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

async fn write_packet<S: BinaryData + std::fmt::Debug>(
    write: &mut OwnedWriteHalf,
    payload: &S,
    buffer: &mut Vec<u8>,
    len_buf: &mut [u8; 5],
) -> bool {
    buffer.clear();
    if let Err(err) = try_encode_packet(payload, buffer) {
        error!("failed to encode packet: {err:?}");
        return true;
    }
    if buffer.len() > MAX_PACKET_SIZE {
        error!(
            "dropping oversized outgoing packet ({} bytes)",
            buffer.len()
        );
        buffer.clear();
        buffer.shrink_to(IDLE_BUFFER_CAP);
        return true;
    }
    trace!("sending {} bytes ({payload:?})", buffer.len());

    let mut x = buffer.len() as u32;
    let mut n = 0;
    loop {
        len_buf[n] = (x & 0x7f) as u8;
        n += 1;
        x >>= 7;
        if x == 0 {
            break;
        } else {
            len_buf[n - 1] |= 0x80;
        }
    }

    let ok = match time::timeout(SEND_TIMEOUT, async {
        write.write_all(&len_buf[..n]).await?;
        write.write_all(buffer).await?;
        Ok::<_, Error>(())
    })
    .await
    {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            error!("failed to send: {err:?}");
            false
        }
        Err(_) => {
            error!("send timed out after {SEND_TIMEOUT:?}");
            false
        }
    };
    if buffer.capacity() > IDLE_BUFFER_CAP {
        buffer.clear();
        buffer.shrink_to(IDLE_BUFFER_CAP);
    }
    ok
}

async fn read_packet_len(read: &mut OwnedReadHalf) -> Result<usize> {
    let mut len = 0u32;
    let mut pos = 0u32;
    loop {
        let byte = read.read_u8().await?;
        if pos == 28 && byte > 0x0f {
            bail!("invalid length");
        }
        len |= ((byte & 0x7f) as u32) << pos;
        if byte & 0x80 == 0 {
            break;
        }
        pos += 7;
    }
    if len as usize > MAX_PACKET_SIZE {
        bail!("data packet too large");
    }
    Ok(len as usize)
}

pub struct Stream<S, R> {
    version: u8,

    send_tx: Arc<mpsc::Sender<S>>,
    close_tx: watch::Sender<bool>,
    force_close: Arc<AtomicBool>,

    send_task_handle: JoinHandle<()>,
    recv_task_handle: JoinHandle<Result<()>>,

    _marker: PhantomData<(S, R)>,
}

impl<S, R> Stream<S, R>
where
    S: BinaryData + std::fmt::Debug + Send + Sync + 'static,
    R: BinaryData + std::fmt::Debug + Send + 'static,
{
    pub async fn new<F>(
        version: Option<u8>,
        stream: TcpStream,
        mut handler: Box<dyn FnMut(Arc<mpsc::Sender<S>>, R) -> F + Send + Sync>,
    ) -> Result<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        stream.set_nodelay(true)?;
        let (mut read, mut write) = stream.into_split();
        let version = if let Some(version) = version {
            write.write_u8(version).await?;
            version
        } else {
            read.read_u8().await?
        };

        let (send_tx, mut send_rx) = mpsc::channel(1024);
        let send_tx = Arc::new(send_tx);
        let (close_tx, mut send_close_rx) = watch::channel(false);
        let mut recv_close_rx = close_tx.subscribe();
        let force_close = Arc::new(AtomicBool::new(false));

        let send_task_handle = tokio::spawn({
            let send_close_tx = close_tx.clone();
            let force_close = Arc::clone(&force_close);
            async move {
                let _guard = CloseGuard(send_close_tx);
                let mut buffer = Vec::new();
                let mut len_buf = [0u8; 5];
                loop {
                    if *send_close_rx.borrow() {
                        break;
                    }
                    let payload = tokio::select! {
                        biased;
                        _ = send_close_rx.changed() => break,
                        payload = send_rx.recv() => match payload {
                            Some(payload) => payload,
                            None => break,
                        }
                    };
                    if !write_packet(&mut write, &payload, &mut buffer, &mut len_buf).await {
                        return;
                    }
                }
                if !force_close.load(Ordering::Acquire) {
                    while let Ok(payload) = send_rx.try_recv() {
                        if !write_packet(&mut write, &payload, &mut buffer, &mut len_buf).await {
                            return;
                        }
                    }
                    let _ = time::timeout(Duration::from_millis(200), write.shutdown()).await;
                }
            }
        });

        let recv_task_handle = tokio::spawn({
            let send_tx = Arc::clone(&send_tx);
            let recv_close_tx = close_tx.clone();
            async move {
                let _guard = CloseGuard(recv_close_tx);
                let mut buffer = Vec::new();
                loop {
                    if *recv_close_rx.borrow() {
                        break;
                    }
                    let read_res = tokio::select! {
                        biased;
                        _ = recv_close_rx.changed() => break,
                        res = async {
                            let len = read_packet_len(&mut read).await?;
                            buffer.clear();
                            if buffer.capacity() > IDLE_BUFFER_CAP && len <= IDLE_BUFFER_CAP {
                                buffer.shrink_to(IDLE_BUFFER_CAP);
                            }
                            (&mut read).take(len as u64).read_to_end(&mut buffer).await?;
                            if buffer.len() != len {
                                bail!("unexpected EOF");
                            }
                            Ok::<_, Error>(())
                        } => res,
                    };
                    read_res?;
                    trace!("received {} bytes", buffer.len());

                    let payload: R = match decode_packet(&buffer) {
                        Ok(val) => val,
                        Err(err) => {
                            // Do not dump the raw bytes: a malformed `Authenticate` packet
                            // would leak (part of) the bearer token into the logs.
                            warn!("invalid packet ({} bytes): {err:?}", buffer.len());
                            break;
                        }
                    };
                    trace!("decodes to {payload:?}");
                    if *recv_close_rx.borrow() {
                        break;
                    }
                    handler(Arc::clone(&send_tx), payload).await;
                }
                Ok(())
            }
        });

        Ok(Self {
            version,

            send_tx,
            close_tx,
            force_close,

            send_task_handle,
            recv_task_handle,

            _marker: PhantomData,
        })
    }
}

impl<S, R> Stream<S, R> {
    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn is_closed(&self) -> bool {
        *self.close_tx.borrow()
    }

    pub fn closed(&self) -> watch::Receiver<bool> {
        self.close_tx.subscribe()
    }

    pub fn close(&self) {
        self.force_close.store(true, Ordering::Release);
        let _ = self.close_tx.send(true);
        self.send_task_handle.abort();
        self.recv_task_handle.abort();
    }

    pub async fn close_after_flush(&mut self, timeout: Duration) {
        self.recv_task_handle.abort();
        let _ = self.close_tx.send(true);
        let _ = time::timeout(timeout, &mut self.send_task_handle).await;
        self.close();
    }
}

impl<S, R> Stream<S, R>
where
    S: Send + Sync + 'static,
{
    pub async fn send(&self, payload: S) -> Result<()> {
        self.send_tx.send(payload).await?;
        Ok(())
    }

    pub fn try_send(&self, payload: S) -> Result<()> {
        self.send_tx.try_send(payload)?;
        Ok(())
    }

    pub fn blocking_send(&self, payload: S) -> Result<()> {
        self.send_tx.blocking_send(payload)?;
        Ok(())
    }
}

impl<S, R> Drop for Stream<S, R> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use phira_mp_macros::BinaryData;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    #[derive(Debug, PartialEq, Eq, BinaryData)]
    struct TuplePair(u16, String);

    #[test]
    fn rejects_overlong_or_overflowing_uleb() {
        let overlong = [0x80u8; 11];
        assert!(decode_packet::<u64>(&overlong).is_err());

        // 10th byte with bit value > 1 overflows u64.
        let mut overflow = [0x80u8; 10];
        overflow[9] = 0x02;
        assert!(decode_packet::<u64>(&overflow).is_err());

        // Valid u64::MAX works and leaves 0 trailing bytes.
        let mut buf = Vec::new();
        encode_packet(&u64::MAX, &mut buf);
        assert_eq!(decode_packet::<u64>(&buf).unwrap(), u64::MAX);
    }

    #[test]
    fn rejects_invalid_bool_trailing_bytes_and_invalid_utf8() {
        assert!(decode_packet::<bool>(&[2]).is_err());
        assert!(decode_packet::<Option<u8>>(&[2]).is_err());
        assert!(decode_packet::<u8>(&[1, 0]).is_err());

        // Invalid UTF-8 in String and Varchar<20>
        assert!(decode_packet::<String>(&[2, 0xff, 0xfe]).is_err());
        assert!(decode_packet::<RoomId>(&[2, 0xff, 0xfe]).is_err());
    }

    #[test]
    fn rejects_array_and_map_count_overflow_without_oom() {
        let mut huge_count = Vec::new();
        BinaryWriter::new(&mut huge_count).uleb(u64::MAX).unwrap();

        assert!(decode_packet::<Vec<u8>>(&huge_count).is_err());
        assert!(decode_packet::<HashMap<u8, u8>>(&huge_count).is_err());
        assert!(decode_packet::<Vec<()>>(&huge_count).is_err());
        assert!(decode_packet::<HashMap<(), ()>>(&huge_count).is_err());
    }

    #[test]
    fn take_checked_add_prevents_overflow() {
        let mut reader = BinaryReader::new(&[1, 2, 3]);
        assert!(reader.byte().is_ok());
        assert!(reader.take(usize::MAX).is_err());
    }

    #[test]
    fn derive_binary_data_handles_tuple_struct() {
        let orig = TuplePair(42, "hello".to_owned());
        let mut buf = Vec::new();
        encode_packet(&orig, &mut buf);
        assert_eq!(decode_packet::<TuplePair>(&buf).unwrap(), orig);
    }

    #[tokio::test]
    async fn send_task_drops_oversized_packet_and_keeps_stream_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (rx_tx, mut rx_rx) = mpsc::channel(4);
        let peer_task = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let _peer: Stream<ClientCommand, ServerCommand> = Stream::new(
                None,
                sock,
                Box::new(move |_, cmd| {
                    let rx_tx = rx_tx.clone();
                    async move {
                        let _ = rx_tx.send(cmd).await;
                    }
                }),
            )
            .await
            .unwrap();
            rx_rx.recv().await
        });

        let sock = TcpStream::connect(addr).await.unwrap();
        let stream: Stream<ServerCommand, ClientCommand> =
            Stream::new(Some(1), sock, Box::new(|_, _| async {}))
                .await
                .unwrap();

        stream
            .send(ServerCommand::Message(Message::Chat {
                user: 1,
                content: "a".repeat(MAX_PACKET_SIZE + 100),
            }))
            .await
            .unwrap();
        stream.send(ServerCommand::Pong).await.unwrap();

        let received = time::timeout(Duration::from_secs(2), peer_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(received, ServerCommand::Pong));
    }

    #[tokio::test]
    async fn stream_close_and_close_after_flush_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (rx_tx, mut rx_rx) = mpsc::channel(4);
        let peer_task = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let _peer: Stream<ClientCommand, ServerCommand> = Stream::new(
                None,
                sock,
                Box::new(move |_, cmd| {
                    let rx_tx = rx_tx.clone();
                    async move {
                        let _ = rx_tx.send(cmd).await;
                    }
                }),
            )
            .await
            .unwrap();
            (rx_rx.recv().await, rx_rx.recv().await)
        });

        let sock = TcpStream::connect(addr).await.unwrap();
        let mut stream: Stream<ServerCommand, ClientCommand> =
            Stream::new(Some(1), sock, Box::new(|_, _| async {}))
                .await
                .unwrap();
        let mut closed = stream.closed();
        assert!(!stream.is_closed());

        stream
            .try_send(ServerCommand::Authenticate(Err("bad token".into())))
            .unwrap();
        stream.close_after_flush(Duration::from_secs(1)).await;

        assert!(stream.is_closed());
        assert!(*closed.borrow_and_update());
        let (first, second) = time::timeout(Duration::from_secs(2), peer_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, Some(ServerCommand::Authenticate(Err(_)))));
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn try_send_fails_immediately_when_queue_is_full() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            sock
        });
        let sock = TcpStream::connect(addr).await.unwrap();
        let stream: Stream<ServerCommand, ClientCommand> =
            Stream::new(Some(1), sock, Box::new(|_, _| async {}))
                .await
                .unwrap();
        let _peer = peer.await.unwrap();

        let big = "x".repeat(60_000);
        let mut hit_full = false;
        for _ in 0..5_000 {
            let msg = ServerCommand::Message(Message::Chat {
                user: 1,
                content: big.clone(),
            });
            if stream.try_send(msg).is_err() {
                hit_full = true;
                break;
            }
        }
        assert!(
            hit_full,
            "expected try_send to fail once the send queue and socket buffer filled"
        );
    }
}
