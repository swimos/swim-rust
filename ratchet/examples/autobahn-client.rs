#![allow(warnings)]

use log::*;
use url::Url;

use bytes::BytesMut;
use futures::stream::{Stream, StreamExt};
use futures::SinkExt;
use ratchet::ws::{client, WebSocket};
use ratchet::{Error, Message, MessageType, NoExt, NoExtProxy, TryIntoRequest, WebSocketConfig};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::net::TcpStream;

const AGENT: &str = "Ratchet";

async fn subscribe(url: &str) -> Result<WebSocket<TcpStream, NoExt>, Error> {
    let stream = TcpStream::connect("127.0.0.1:9001").await.unwrap();
    stream.set_nodelay(true).unwrap();

    client(
        WebSocketConfig::default(),
        stream,
        url.try_into_request().unwrap(),
        NoExtProxy,
    )
    .await
    .map(|(l, _)| l)
}

async fn get_case_count() -> Result<u32, Error> {
    let stream = TcpStream::connect("127.0.0.1:9001").await.unwrap();
    stream.set_nodelay(true).unwrap();

    let mut websocket = subscribe("ws://localhost:9001/getCaseCount").await.unwrap();

    println!("get_case_count Connected");
    let mut buf = BytesMut::new();
    let msg = websocket.read(&mut buf).await?;
    println!("get_case_count got message");

    // websocket.close().await;
    println!("get_case_count closed");

    match msg {
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
    // websocket.close().await
    Ok(())
}

async fn run_test(case: u32) -> Result<(), Error> {
    info!("Running test case {}", case);
    let mut websocket = subscribe(&format!(
        "ws://localhost:9001/runCase?case={}&agent={}",
        case, AGENT
    ))
    .await
    .unwrap();

    let mut buf = BytesMut::new();

    loop {
        match websocket.read(&mut buf).await? {
            Message::Text => {
                let _s = String::from_utf8(buf.to_vec())?;
                websocket.write(&mut buf, MessageType::Text).await?;
                buf.clear();
            }
            Message::Binary => {
                websocket.write(&mut buf, MessageType::Binary).await?;
                buf.clear();
            }
            Message::Ping | Message::Pong => {}
            Message::Close(_) => break Ok(()),
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let total = get_case_count().await.unwrap();

    for case in 1..=total {
        if let Err(e) = run_test(case).await {
            println!("{}", e);
            // if !e.closed_normal() {
            // panic!("test: {}", e);
            // }
        }
    }

    update_reports().await.unwrap();
}
