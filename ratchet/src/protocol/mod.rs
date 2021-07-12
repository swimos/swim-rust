pub mod frame;

pub enum Message {
    String,
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}
