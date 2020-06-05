// Working receive:
fn a() {
    let url = Url::parse("ws://127.0.0.1:9001/").unwrap();

    let (ws, wsio) = WsMeta::connect(url, None).await.unwrap();
    let state = ws.ready_state();
    log(&format!("State: {:?}", state));

    let (mut sink, mut stream) = wsio.split();

    sink.send(WsMessage::Text(String::from(
        "@link(node: \"unit/foo\", lane: \"random\")",
    )))
    .await
    .unwrap();

    sink.send(WsMessage::Text(String::from(
        "@sync(node: \"unit/foo\", lane: \"random\")",
    )))
    .await
    .unwrap();

    while let Some(e) = stream.next().await {
        log(&format!("Msg: {:?}", e));
    }

    log("Started client...");
}
