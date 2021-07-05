pub trait Extension {}

#[derive(Debug)]
pub enum WebsocketExtension {
    None,
    Deflate,
}
