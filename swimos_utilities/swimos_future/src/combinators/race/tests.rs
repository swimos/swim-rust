use crate::combinators::race;
use futures::future::{pending, ready};
use futures::FutureExt;

#[tokio::test]
async fn race2_test() {
    let left_ready = race(ready(()), pending::<()>()).await;
    assert!(left_ready.is_left());

    let right_ready = race(pending::<()>(), ready(())).await;
    assert!(right_ready.is_right());

    let both_pending = race(pending::<()>(), pending::<()>());
    assert!(both_pending.now_or_never().is_none());

    let both_ready = race(ready(()), ready(())).await;
    assert!(both_ready.is_left());
}
