// Copyright 2015-2023 Swim Inc.
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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;
use pin_project::pin_project;

#[derive(Debug, Clone, Copy)]
#[pin_project(project = Union3Proj)]
pub enum UnionFuture3<F1, F2, F3> {
    First(#[pin] F1),
    Second(#[pin] F2),
    Third(#[pin] F3),
}

#[derive(Debug, Clone, Copy)]
#[pin_project(project = Union4Proj)]
pub enum UnionFuture4<F1, F2, F3, F4> {
    First(#[pin] F1),
    Second(#[pin] F2),
    Third(#[pin] F3),
    Fourth(#[pin] F4),
}

impl<F1, F2, F3> UnionFuture3<F1, F2, F3> {
    pub fn first(f: F1) -> Self {
        UnionFuture3::First(f)
    }

    pub fn second(f: F2) -> Self {
        UnionFuture3::Second(f)
    }

    pub fn third(f: F3) -> Self {
        UnionFuture3::Third(f)
    }
}

impl<F1, F2, F3> Future for UnionFuture3<F1, F2, F3>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            Union3Proj::First(f) => f.poll(cx),
            Union3Proj::Second(f) => f.poll(cx),
            Union3Proj::Third(f) => f.poll(cx),
        }
    }
}

impl<F1, F2, F3, F4> UnionFuture4<F1, F2, F3, F4> {
    pub fn first(f: F1) -> Self {
        UnionFuture4::First(f)
    }

    pub fn second(f: F2) -> Self {
        UnionFuture4::Second(f)
    }

    pub fn third(f: F3) -> Self {
        UnionFuture4::Third(f)
    }

    pub fn fourth(f: F4) -> Self {
        UnionFuture4::Fourth(f)
    }
}

impl<F1, F2, F3, F4> Future for UnionFuture4<F1, F2, F3, F4>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
    F4: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            Union4Proj::First(f) => f.poll(cx),
            Union4Proj::Second(f) => f.poll(cx),
            Union4Proj::Third(f) => f.poll(cx),
            Union4Proj::Fourth(f) => f.poll(cx),
        }
    }
}
