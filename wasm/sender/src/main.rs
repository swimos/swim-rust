use bytes::BytesMut;
use ratchet::{NoExt, WebSocket};
use std::time::Instant;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:61171").await.unwrap();
    let mut client = ratchet::subscribe(Default::default(), stream, "ws://localhost:61171")
        .await
        .unwrap()
        .websocket;
    let mut buf = BytesMut::new();

    round_trip(
        &mut client,
        &mut buf,
        "@link(node:\"/example/1\", lane:lane)",
    )
    .await;
    // round_trip(&mut client, &mut buf, "@sync(node:\"/example/1\", lane:lane)").await;
    // client.read(&mut buf).await.unwrap();
    // println!("{:?}", std::str::from_utf8(buf.as_ref()).unwrap());
    // buf.clear();

    println!("Warming up");

    for i in 0..1000 {
        round_trip(
            &mut client,
            &mut buf,
            format!("@command(node:\"/example/1\", lane:lane){i}").as_str(),
        )
        .await;
    }

    println!("Warmed up");

    let events = 100_000;
    let start = Instant::now();

    println!("Sending events up");

    for i in 0..events {
        round_trip(
            &mut client,
            &mut buf,
            format!("@command(node:\"/example/1\", lane:lane){i}").as_str(),
        )
        .await;
    }

    let elapsed = start.elapsed();
    println!(
        "Test took: {}.{}",
        elapsed.as_secs(),
        elapsed.subsec_millis()
    );
    // println!("Message rate: {} per second", (events / elapsed.as_secs()));
}

async fn round_trip(client: &mut WebSocket<TcpStream, NoExt>, buf: &mut BytesMut, msg: &str) {
    client.write_text(msg).await.unwrap();
    // client.read(buf).await.unwrap();
    // println!("{:?}", std::str::from_utf8(buf.as_ref()).unwrap());
    buf.clear();
}
