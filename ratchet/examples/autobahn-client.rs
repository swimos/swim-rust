use bytes::BytesMut;
use ratchet::{client, Upgraded};
use ratchet::{
    Error, Message, NoExt, NoExtProxy, PayloadType, ProtocolRegistry, TryIntoRequest,
    WebSocketConfig,
};
use tokio::net::TcpStream;

const AGENT: &str = "Ratchet";

async fn subscribe(url: &str) -> Result<Upgraded<TcpStream, NoExt>, Error> {
    let stream = TcpStream::connect("127.0.0.1:9001").await.unwrap();
    stream.set_nodelay(true).unwrap();

    client(
        WebSocketConfig::default(),
        stream,
        url.try_into_request().unwrap(),
        NoExtProxy,
        ProtocolRegistry::default(),
    )
    .await
}

async fn get_case_count() -> Result<u32, Error> {
    let stream = TcpStream::connect("127.0.0.1:9001").await.unwrap();
    stream.set_nodelay(true).unwrap();

    let mut websocket = subscribe("ws://localhost:9001/getCaseCount")
        .await
        .unwrap()
        .socket;
    let mut buf = BytesMut::new();

    match websocket.read(&mut buf).await? {
        Message::Text => {
            let count = String::from_utf8(buf.to_vec()).unwrap();
            Ok(count.parse::<u32>().unwrap())
        }
        _ => panic!(),
    }
}

async fn update_reports() -> Result<(), Error> {
    let mut _websocket = subscribe(&format!(
        "ws://localhost:9001/updateReports?agent={}",
        AGENT
    ))
    .await
    .unwrap();
    Ok(())
}

async fn run_test(case: u32) -> Result<(), Error> {
    let mut websocket = subscribe(&format!(
        "ws://localhost:9001/runCase?case={}&agent={}",
        case, AGENT
    ))
    .await
    .unwrap()
    .socket;

    let mut buf = BytesMut::new();

    loop {
        match websocket.read(&mut buf).await? {
            Message::Text => {
                let _s = String::from_utf8(buf.to_vec())?;
                websocket.write(&mut buf, PayloadType::Text).await?;
                buf.clear();
            }
            Message::Binary => {
                websocket.write(&mut buf, PayloadType::Binary).await?;
                buf.clear();
            }
            Message::Ping | Message::Pong => {}
            Message::Close(_) => break Ok(()),
        }
    }
}

#[tokio::main]
async fn main() {
    let total = get_case_count().await.unwrap();

    for case in 1..=total {
        if let Err(e) = run_test(case).await {
            println!("{}", e);
        }
    }

    update_reports().await.unwrap();
}
