pub trait Extension {}

#[derive(Debug, PartialEq)]
pub enum WebsocketExtension {
    None,
    Deflate,
}
