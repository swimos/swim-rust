use crate::mailbox::fan_in::mailbox_channel;
use bytes::BytesMut;
use futures::future::join;
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::codec::{Decoder, FramedRead};

struct StubDecoder;
impl Decoder for StubDecoder {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //println!("Decode");

        if src.is_empty() {
            Ok(None)
        } else {
            let r = src.to_vec();
            src.clear();
            Ok(Some(r))
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn t() {
    let (registrar, mut rx) = mailbox_channel(NonZeroUsize::new(64).unwrap());

    let idx = Arc::new(AtomicUsize::new(10));

    for i in 0..10 {
        let tx = registrar.register().unwrap();
        let mine = idx.clone();
        tokio::spawn(async move {
            tx.send(vec![i + 1, i + 2, i + 3]).await.unwrap();
            let prev = mine.fetch_sub(1, Ordering::Relaxed);
            if prev - 1 == 0 {
                println!("Writers complete");
            }
        });
    }

    let b = async move {
        let mut decoder = FramedRead::new(rx, StubDecoder);

        loop {
            let r = decoder.next().await.unwrap().unwrap();
            // if r.is_empty() {
            //     break;
            // } else {
            //println!("{:?}", r);
            // tokio::time::sleep(Duration::from_millis(500)).await;

            // }
        }
    };

    b.await
}
