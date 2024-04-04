use crate::combinators::{race, race3, Either3};
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

#[tokio::test]
async fn race3_test() {
    let left_ready = race3(ready(()), pending::<()>(), pending::<()>()).await;
    assert!(left_ready.is_left());

    let middle_ready = race3(pending::<()>(), ready(()), pending::<()>()).await;
    assert!(middle_ready.is_middle());

    let right_ready = race3(pending::<()>(), pending::<()>(), ready(())).await;
    assert!(right_ready.is_right());

    let all_pending = race3(pending::<()>(), pending::<()>(), pending::<()>());
    assert!(all_pending.now_or_never().is_none());

    let both_ready = race3(ready(()), ready(()), ready(())).await;
    assert!(both_ready.is_left());
}

#[test]
fn either_three() {
    assert!(Either3::<(), (), ()>::Left(()).is_left());
    assert!(Either3::<(), (), ()>::Middle(()).is_middle());
    assert!(Either3::<(), (), ()>::Right(()).is_right());
}
