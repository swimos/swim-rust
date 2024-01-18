use bytes::BytesMut;
use ratchet::{NoExt, WebSocket};
use tokio::net::TcpStream;

#[tokio::test]
async fn t() {
    let stream = TcpStream::connect("127.0.0.1:6363").await.unwrap();
    let mut client = ratchet::subscribe(Default::default(), stream, "ws://localhost:6363")
        .await
        .unwrap()
        .websocket;
    let mut buf = BytesMut::new();

    round_trip(&mut client, &mut buf, "@link(node:test, lane:lane)").await;
    round_trip(&mut client, &mut buf, "@sync(node:test, lane:lane)").await;
    client.read(&mut buf).await.unwrap();
    println!("{:?}", std::str::from_utf8(buf.as_ref()).unwrap());
    buf.clear();

    for i in 0..10 {
        round_trip(
            &mut client,
            &mut buf,
            format!("@command(node:test, lane:lane){i}").as_str(),
        )
        .await;
    }
}

async fn round_trip(client: &mut WebSocket<TcpStream, NoExt>, buf: &mut BytesMut, msg: &str) {
    client.write_text(msg).await.unwrap();
    client.read(buf).await.unwrap();
    println!("{:?}", std::str::from_utf8(buf.as_ref()).unwrap());
    buf.clear();
}
