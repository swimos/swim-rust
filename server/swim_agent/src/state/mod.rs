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
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::VecDeque,
    marker::PhantomData,
};

use crate::event_handler::{
    ConstHandler, EventHandler, HandlerAction, HandlerActionExt, SideEffect, UnitHandler,
};

pub struct State<Context, T> {
    _context_type: PhantomData<fn(&Context)>,
    content: RefCell<T>,
}

impl<Context, T: Default> Default for State<Context, T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<Context, T> State<Context, T> {
    pub fn new(value: T) -> Self {
        State {
            content: RefCell::new(value),
            _context_type: PhantomData,
        }
    }
}

impl<Context, T> From<T> for State<Context, T> {
    fn from(value: T) -> Self {
        State::new(value)
    }
}

impl<Context, T> State<Context, T>
where
    T: Clone,
{
    pub fn get(&self) -> impl HandlerAction<Context, Completion = T> + '_ {
        SideEffect::from(move || self.content.borrow().clone())
    }
}

impl<Context, T> State<Context, T>
where
    T: Default,
{
    pub fn take(&self) -> impl HandlerAction<Context, Completion = T> + '_ {
        SideEffect::from(move || self.content.take())
    }
}

impl<Context, T> State<Context, T> {
    pub fn replace(&self, value: T) -> impl HandlerAction<Context, Completion = T> + '_ {
        SideEffect::from(move || self.content.replace(value))
    }

    pub fn set(&self, value: T) -> impl EventHandler<Context> + '_ {
        self.replace(value).discard()
    }

    pub fn and_then_with<'a, B, F, H>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = H::Completion> + 'a
    where
        T: Borrow<B>,
        F: FnOnce(&B) -> H + 'a,
        H: HandlerAction<Context> + 'a,
    {
        ConstHandler::from(&self.content).and_then(move |content: &RefCell<T>| {
            let guard = content.borrow();
            f((*guard).borrow())
        })
    }

    pub fn with<'a, B, F, U>(&'a self, f: F) -> impl HandlerAction<Context, Completion = U> + 'a
    where
        T: Borrow<B>,
        F: FnOnce(&B) -> U + 'a,
        U: 'a,
    {
        self.and_then_with(move |s| ConstHandler::from(f(s)))
    }

    pub fn and_then_with_mut<'a, B, F, H>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = H::Completion> + 'a
    where
        T: BorrowMut<B>,
        F: FnOnce(&mut B) -> H + 'a,
        H: HandlerAction<Context> + 'a,
    {
        ConstHandler::from(&self.content).and_then(move |content: &RefCell<T>| {
            let mut guard = content.borrow_mut();
            f((*guard).borrow_mut())
        })
    }

    pub fn with_mut<'a, B, F, U>(&'a self, f: F) -> impl HandlerAction<Context, Completion = U> + 'a
    where
        T: BorrowMut<B>,
        F: FnOnce(&mut B) -> U + 'a,
        U: 'a,
    {
        self.and_then_with_mut(move |s| ConstHandler::from(f(s)))
    }
}

impl<Context, T> State<Context, Option<T>> {
    pub fn with_as_ref<'a, B, F, U>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = Option<U>> + 'a
    where
        T: Borrow<B>,
        F: FnOnce(&B) -> U + 'a,
        U: 'a,
    {
        self.with(move |maybe| maybe.as_ref().map(move |s| f(s.borrow())))
    }

    pub fn an_then_as_ref<'a, B, F, H>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = Option<H::Completion>> + 'a
    where
        T: Borrow<B>,
        F: FnOnce(&B) -> H + 'a,
        H: HandlerAction<Context> + 'a,
    {
        self.and_then_with(move |maybe| maybe.as_ref().map(move |s| f(s.borrow())))
    }

    pub fn with_as_mut<'a, B, F, U>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = Option<U>> + 'a
    where
        T: BorrowMut<B>,
        F: FnOnce(&mut B) -> U + 'a,
        U: 'a,
    {
        self.with_mut(move |maybe| maybe.as_mut().map(move |s| f(s.borrow_mut())))
    }

    pub fn an_then_as_mut<'a, B, F, H>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = Option<H::Completion>> + 'a
    where
        T: BorrowMut<B>,
        F: FnOnce(&mut B) -> H + 'a,
        H: HandlerAction<Context> + 'a,
    {
        self.and_then_with_mut(move |maybe| maybe.as_mut().map(move |s| f(s.borrow_mut())))
    }
}

pub struct History<Context, T> {
    inner: State<Context, VecDeque<T>>,
    max_size: usize,
}

impl<Context, T> History<Context, T> {
    pub fn new(max_size: usize) -> Self {
        History {
            inner: Default::default(),
            max_size,
        }
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn push(&self, item: T) -> impl EventHandler<Context> + '_ {
        let max = self.max_size;
        self.inner.and_then_with_mut(move |history| {
            history.push_back(item);
            if history.len() > max {
                history.pop_front();
            }
            UnitHandler::default()
        })
    }

    pub fn len(&self) -> impl HandlerAction<Context, Completion = usize> + '_ {
        self.inner
            .and_then_with(|history| ConstHandler::from(history.len()))
    }

    pub fn with<'a, F, H>(
        &'a self,
        f: F,
    ) -> impl HandlerAction<Context, Completion = H::Completion> + 'a
    where
        F: FnOnce(&VecDeque<T>) -> H + 'a,
        H: HandlerAction<Context> + 'a,
    {
        self.inner.and_then_with(f)
    }
}
