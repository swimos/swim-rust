// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::var::observer::Observer;
use crate::var::TVar;
use futures::future::{ready, Ready};
use std::any::Any;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn var_get() {
    let var = TVar::new(3);
    let n = var.load().await;
    assert_eq!(n, Arc::new(3));
}

#[tokio::test]
async fn var_snapshot() {
    let var = TVar::new(3);
    let n = var.snapshot().await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn var_store() {
    let var = TVar::new(3);
    var.store(7).await;
    let n = var.snapshot().await;
    assert_eq!(n, 7);
}

#[tokio::test]
async fn var_store_arc() {
    let var = TVar::new(3);
    let replacement = Arc::new(7);
    var.store_arc(replacement.clone()).await;
    let n = var.load().await;
    assert!(Arc::ptr_eq(&replacement, &n));
}

#[derive(Clone)]
pub struct TestObserver<T>(Arc<Mutex<Option<Arc<T>>>>);

impl<T: Any + Send + Sync> TestObserver<T> {
    pub fn new(init: Option<Arc<T>>) -> Self {
        TestObserver(Arc::new(Mutex::new(init)))
    }

    pub fn get(&self) -> Option<Arc<T>> {
        let TestObserver(inner) = self;
        inner.lock().unwrap().clone()
    }
}

impl<'a, T> Observer<'a, Arc<T>> for TestObserver<T>
where
    T: Any + Send + Sync,
{
    type RecFuture = Ready<()>;

    fn notify(&'a self, value: Arc<T>) -> Self::RecFuture {
        let TestObserver(inner) = self;
        let mut lock = inner.lock().unwrap();
        *lock = Some(value);
        ready(())
    }
}

#[tokio::test]
async fn observe_var_store() {
    let observer = TestObserver::new(None);

    let var = TVar::new_with_observer(0, observer.clone());

    var.store(17).await;

    let observed = observer.get();

    assert_eq!(observed, Some(Arc::new(17)));

    var.store(-34).await;

    let observed = observer.get();

    assert_eq!(observed, Some(Arc::new(-34)));
}
