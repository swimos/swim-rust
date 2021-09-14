use crate::framed::{CodecFlags, FramedIoParts, FramedRead, FramedWrite};
use crate::{framed, Extension, WebSocket, WebSocketStream};
use bytes::BytesMut;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf as TokioWriteHalf};
use tokio::sync::Mutex;

// todo sink and stream implementations
// todo split extension
pub fn split<S, E>(
    framed: framed::FramedIo<S>,
    control_buffer: BytesMut,
    _extension: E,
) -> (Sender<S, E>, Receiver<S, E>)
where
    S: WebSocketStream,
    E: Extension,
{
    let FramedIoParts {
        io,
        read_buffer,
        write_buffer,
        flags,
        max_size,
    } = framed.into_parts();

    let closed = Arc::new(AtomicBool::new(false));
    let (read_half, write_half) = tokio::io::split(io);
    let split_writer = Arc::new(WriteHalf {
        split_writer: Mutex::new(write_half),
        writer: FramedWrite::new(write_buffer),
    });

    let sender = Sender {
        closed: closed.clone(),
        split_writer: split_writer.clone(),
        _pde: PhantomData::default(),
    };
    let receiver = Receiver {
        closed,
        control_buffer,
        framed: FramedIo {
            flags,
            max_size,
            read_half,
            reader: FramedRead::new(read_buffer),
            split_writer,
        },
        _pde: PhantomData::default(),
    };

    (sender, receiver)
}

struct WriteHalf<S> {
    split_writer: Mutex<TokioWriteHalf<S>>,
    writer: FramedWrite,
}

pub struct Sender<S, E> {
    closed: Arc<AtomicBool>,
    split_writer: Arc<WriteHalf<S>>,
    _pde: PhantomData<E>,
}

impl<S, E> Sender<S, E> {
    pub fn reunite(self, receiver: Receiver<S, E>) -> Result<WebSocket<S, E>, ReuniteError<S, E>> {
        reunite(self, receiver)
    }
}

pub struct Receiver<S, E> {
    closed: Arc<AtomicBool>,
    control_buffer: BytesMut,
    framed: FramedIo<S>,
    _pde: PhantomData<E>,
}

struct FramedIo<S> {
    flags: CodecFlags,
    max_size: usize,
    read_half: ReadHalf<S>,
    reader: FramedRead,
    split_writer: Arc<WriteHalf<S>>,
}

impl<S, E> Receiver<S, E> {
    pub fn reunite(self, sender: Sender<S, E>) -> Result<WebSocket<S, E>, ReuniteError<S, E>> {
        reunite(sender, self)
    }
}

pub struct ReuniteError<S, E> {
    pub sender: Sender<S, E>,
    pub receiver: Receiver<S, E>,
}

fn reunite<S, E>(
    sender: Sender<S, E>,
    receiver: Receiver<S, E>,
) -> Result<WebSocket<S, E>, ReuniteError<S, E>> {
    if Arc::ptr_eq(&sender.split_writer, &receiver.framed.split_writer) {
        unimplemented!()
    } else {
        return Err(ReuniteError { sender, receiver });
    }
}
