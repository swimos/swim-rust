// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed mod in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crossbeam::atomic::AtomicCell;

pub trait EffCell<'a> {
    type Contents;
    type GetF: Future<Output=Self::Contents> + 'a;
    type SetF: Future<Output=()> + 'a;
    type UpdF: Future<Output=Self::Contents> + 'a;

    fn get<'s: 'a>(&'s self) -> Self::GetF;

    fn set<'s: 'a>(&'s self, value: Self::Contents) -> Self::SetF;

    fn update<'s: 'a>(&'s self, f: &'a dyn Fn(Self::Contents) -> Self::Contents) -> Self::UpdF;
}

pub struct CopyGet<'a, T> {
    cell: &'a AtomicCell<T>,
}

pub struct CopySet<'a, T> {
    cell: &'a AtomicCell<T>,
    value: T,
}

pub struct CopyEqUpdate<'a, T> {
    cell: &'a AtomicCell<T>,
    upd: &'a dyn Fn(T) -> T,
}

impl<'a, T: Copy> Future for CopyGet<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.cell.load())
    }
}

impl<'a, T: Copy> Future for CopySet<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let set = &*self;
        set.cell.store(set.value);
        Poll::Ready(())
    }
}

impl<'a, T: Copy + Eq> Future for CopyEqUpdate<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let updater = &*self;
        Poll::Ready(loop {
            let current = updater.cell.load();
            let next = (updater.upd)(current);
            match updater.cell.compare_exchange(current, next) {
                Ok(prev) => break prev,
                Err(_) => continue,
            }
        })
    }
}

impl<'a, T: Copy + Eq + 'a> EffCell<'a> for AtomicCell<T> {
    type Contents = T;
    type GetF = CopyGet<'a, T>;
    type SetF = CopySet<'a, T>;
    type UpdF = CopyEqUpdate<'a, T>;

    fn get<'s: 'a>(&'s self) -> Self::GetF {
        CopyGet { cell: self }
    }

    fn set<'s: 'a>(&'s self, value: Self::Contents) -> Self::SetF {
        CopySet { cell: self, value }
    }

    fn update<'s: 'a>(&'s self, f: &'a dyn Fn(Self::Contents) -> Self::Contents) -> Self::UpdF {
        CopyEqUpdate { cell: self, upd: f }
    }
}
