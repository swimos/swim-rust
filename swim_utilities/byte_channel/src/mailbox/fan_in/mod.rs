mod read;
mod send;

use crate::io::byte_channel::ByteWriter;
use crate::io::mailbox::core::{Node, Queue, WriteTask};
use crate::io::mailbox::fan_in::read::Read;
use bytes::BytesMut;
use futures::future::Shared;
use futures::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::future::Future;
use std::io::Error;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

pub fn channel<C>(capacity: NonZeroUsize, decoder: C) -> (ChannelRegistrar, ChannelReceiver<C>) {
    let shared = Arc::new(SharedInner::new(capacity));
    let registrar = ChannelRegistrar {
        shared: shared.clone(),
    };
    let receiver = ChannelReceiver { decoder, shared };
    (registrar, receiver)
}

pub struct ChannelRegistrar {
    shared: Arc<SharedInner>,
}

impl ChannelRegistrar {
    pub fn register(&self) -> Result<RawChannelSender, ()> {
        unimplemented!()
    }
}

pub struct RawChannelSender {
    shared: Arc<SharedInner>,
}

impl RawChannelSender {
    // collides with the Send trait
    pub fn send<'r, I>(&self, item: I) -> send::Send<'_, I>
    where
        I: AsRef<[u8]>,
    {
        send::Send::new(self, item)
    }
}

struct SharedInner {
    current: UnsafeCell<Option<WriteTask>>,
    write_queue: Queue<WriteTask>,
    receiver: AtomicWaker,
    capacity: usize,
    mailbox: BytesMut,
}

impl SharedInner {
    fn new(capacity: NonZeroUsize) -> SharedInner {
        let empty = Node::new(None);

        SharedInner {
            capacity: capacity.get(),
            mailbox: BytesMut::default(),
            current: UnsafeCell::default(),
            write_queue: Queue::new(AtomicPtr::new(empty), UnsafeCell::new(empty)),
            receiver: AtomicWaker::default(),
        }
    }
}

pub struct ChannelReceiver<C> {
    decoder: C,
    shared: Arc<SharedInner>,
}

impl<C> ChannelReceiver<C> {
    pub fn read_next<I>(&self) -> Read<'_, C>
    where
        C: Decoder<Item = I>,
    {
        Read::new(self)
    }
}
