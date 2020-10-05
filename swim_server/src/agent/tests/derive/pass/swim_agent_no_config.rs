use swim_server::SwimAgent;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    #[derive(Debug, SwimAgent)]
    struct TestAgent;
}
