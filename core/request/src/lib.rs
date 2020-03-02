use tokio::sync::oneshot;

#[derive(Debug)]
pub struct Request<T> {
    satisfy: oneshot::Sender<T>,
}

impl<T> Request<T> {
    pub fn new(sender: oneshot::Sender<T>) -> Request<T> {
        Request { satisfy: sender }
    }

    pub fn send(self, data: T) -> Option<()> {
        match self.satisfy.send(data) {
            Ok(_) => None,
            Err(_) => Some(()),
        }
    }
}
