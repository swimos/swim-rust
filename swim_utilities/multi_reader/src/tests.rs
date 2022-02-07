use crate::MultiReader;
use byte_channel::byte_channel;
use futures_util::future::join;
use futures_util::stream::SelectAll;
use futures_util::{SinkExt, StreamExt};
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_runtime::compat::{
    AgentMessageDecoder, Operation, RawRequestMessageEncoder, TaggedRequestMessage,
};
use swim_runtime::routing::RoutingAddr;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{FramedRead, FramedWrite};

#[tokio::test]
async fn multi_reader_test_select() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (second_writer, second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (third_writer, third_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let write = async move {
        sleep(Duration::from_secs(1)).await;
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);
        let mut second_framed = FramedWrite::new(second_writer, RawRequestMessageEncoder);
        let third_framed = FramedWrite::new(third_writer, RawRequestMessageEncoder);

        first_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(1),
                path: RelativePath::new("node_1", "lane_1"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 1");
        sleep(Duration::from_secs(1)).await;

        second_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(2),
                path: RelativePath::new("node_2", "lane_2"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 2");
        sleep(Duration::from_secs(1)).await;

        first_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(3),
                path: RelativePath::new("node_3", "lane_3"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 3");
        sleep(Duration::from_secs(1)).await;

        second_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(4),
                path: RelativePath::new("node_4", "lane_4"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 4");
        sleep(Duration::from_secs(1)).await;

        drop(first_framed);
        drop(second_framed);
        drop(third_framed);
        sleep(Duration::from_secs(1)).await;
    };

    let read = async move {
        let mut multi_reader = SelectAll::new();

        let first_framed = FramedRead::new(
            first_reader,
            AgentMessageDecoder::new(Value::make_recognizer()),
        );
        let second_framed = FramedRead::new(
            second_reader,
            AgentMessageDecoder::new(Value::make_recognizer()),
        );
        let third_framed = FramedRead::new(
            third_reader,
            AgentMessageDecoder::new(Value::make_recognizer()),
        );

        multi_reader.push(first_framed);
        multi_reader.push(second_framed);
        multi_reader.push(third_framed);

        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
    };

    let _ = join(timeout(Duration::from_secs(10), read), write).await;
}

#[tokio::test]
async fn multi_reader_test() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (second_writer, second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (third_writer, third_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add_reader(FramedRead::new(
        first_reader,
        AgentMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add_reader(FramedRead::new(
        second_reader,
        AgentMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add_reader(FramedRead::new(
        third_reader,
        AgentMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        sleep(Duration::from_secs(1)).await;
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);
        let mut second_framed = FramedWrite::new(second_writer, RawRequestMessageEncoder);
        let third_framed = FramedWrite::new(third_writer, RawRequestMessageEncoder);

        first_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(1),
                path: RelativePath::new("node_1", "lane_1"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 1");
        sleep(Duration::from_secs(1)).await;

        second_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(2),
                path: RelativePath::new("node_2", "lane_2"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 2");
        sleep(Duration::from_secs(1)).await;

        first_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(3),
                path: RelativePath::new("node_3", "lane_3"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 3");
        sleep(Duration::from_secs(1)).await;

        second_framed
            .send(TaggedRequestMessage {
                origin: RoutingAddr::remote(4),
                path: RelativePath::new("node_4", "lane_4"),
                envelope: Operation::Link,
            })
            .await
            .unwrap();
        println!("sent 4");
        sleep(Duration::from_secs(1)).await;

        drop(first_framed);
        drop(second_framed);
        drop(third_framed);
        sleep(Duration::from_secs(1)).await;
    };

    let read = async move {
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        let c = multi_reader.next().await;
        eprintln!("c = {:?}", c);
        eprintln!(
            "multi_reader.readers.len() = {:?}",
            multi_reader.readers.len()
        );
    };

    let _ = join(timeout(Duration::from_secs(10), read), write).await;
}
