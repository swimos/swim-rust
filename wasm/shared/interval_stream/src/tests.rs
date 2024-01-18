use std::future::Future;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use tokio::time::timeout;

use crate::{IntervalStream, ItemStatus, ScheduleDef, StreamItem};

async fn test<F: Future>(secs: u64, f: F) {
    if timeout(Duration::from_secs(secs), f).await.is_err() {
        panic!("Test timed out")
    }
}

#[tokio::test]
async fn remove() {
    test(5, async move {
        let mut stream = IntervalStream::new();
        stream.push(
            ScheduleDef::Interval {
                count: 13,
                interval: Duration::from_millis(100),
            },
            13,
        );

        assert_eq!(stream.next().await.expect("Missing item").item, 13);
        assert_eq!(stream.next().await.expect("Missing item").item, 13);
        assert_eq!(stream.next().await.expect("Missing item").item, 13);

        let item = stream.next().await.expect("Missing item");
        let key = match item.status {
            ItemStatus::Complete => {
                panic!("Item completed early")
            }
            ItemStatus::WillYield { key } => key,
        };

        stream.remove(&key);

        assert!(stream.is_empty());
        assert_eq!(stream.next().now_or_never(), Some(None));
    })
    .await;
}

#[tokio::test]
async fn once() {
    test(5, async move {
        let mut stream = IntervalStream::new();
        stream.push(
            ScheduleDef::Once {
                after: Duration::from_millis(100),
            },
            13,
        );

        assert_eq!(
            stream.next().await,
            Some(StreamItem {
                item: 13,
                status: ItemStatus::Complete,
            })
        )
    })
    .await;
}

#[tokio::test]
async fn interval() {
    test(5, async move {
        let run_count = 30;

        let mut stream = IntervalStream::new();
        stream.push(
            ScheduleDef::Interval {
                count: run_count,
                interval: Duration::from_millis(100),
            },
            13,
        );

        for i in 0..run_count {
            let item = stream.next().await.expect("Missing stream element");
            if i + 1 == run_count {
                assert!(item.is_complete());
            } else {
                assert!(item.is_will_yield());
            }
        }
    })
    .await;
}

#[tokio::test]
async fn infinite() {
    test(5, async move {
        let mut stream = IntervalStream::new();
        stream.push(
            ScheduleDef::Infinite {
                interval: Duration::from_nanos(100),
            },
            13,
        );

        for _ in 0..100 {
            let item = stream.next().await.expect("Missing stream element");
            assert!(item.is_will_yield());
        }
    })
    .await;
}

#[tokio::test]
async fn heterogeneous_schedules() {
    test(60, async move {
        #[derive(Debug, PartialEq, Clone)]
        struct StreamKey(usize);

        let mut stream = IntervalStream::new();

        stream.push(
            ScheduleDef::Once {
                after: Duration::from_secs(1),
            },
            StreamKey(0),
        );
        stream.push(
            ScheduleDef::Once {
                after: Duration::from_secs(2),
            },
            StreamKey(1),
        );
        stream.push(
            ScheduleDef::Interval {
                count: 3,
                interval: Duration::from_millis(600),
            },
            StreamKey(2),
        );
        stream.push(
            ScheduleDef::Infinite {
                interval: Duration::from_secs(5),
            },
            StreamKey(3),
        );

        async fn expect_key(stream: &mut IntervalStream<StreamKey>, expected_key: usize) {
            let stream_item = stream.next().await.expect("Missing stream element");
            assert_eq!(stream_item.item.0, expected_key);
        }

        expect_key(&mut stream, 2).await;
        expect_key(&mut stream, 0).await;
        expect_key(&mut stream, 2).await;
        expect_key(&mut stream, 2).await;
        expect_key(&mut stream, 1).await;
        expect_key(&mut stream, 3).await;
        expect_key(&mut stream, 3).await;

        let stream_item = stream.next().await.expect("Missing stream element");
        let key = match stream_item.status {
            ItemStatus::Complete => {
                panic!("Item completed early")
            }
            ItemStatus::WillYield { key } => key,
        };

        stream.remove(&key);
        assert!(stream.is_empty());

        assert_eq!(stream.next().now_or_never(), Some(None));
    })
    .await;
}
