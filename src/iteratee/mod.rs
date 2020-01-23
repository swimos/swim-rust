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
use std::num::NonZeroUsize;

#[cfg(test)]
mod tests;

pub trait Iteratee<In> {
    type Item;

    fn feed(&mut self, input: In) -> Option<Self::Item>;

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        None
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    fn comap<B, F>(self, f: F) -> Comap<Self, F>
    where
        Self: Sized,
        F: FnMut(B) -> In,
    {
        Comap::new(self, f)
    }

    fn maybe_comap<B, F>(self, f: F) -> MaybeComap<Self, F>
    where
        Self: Sized,
        F: FnMut(B) -> Option<In>,
    {
        MaybeComap::new(self, f)
    }

    fn map<B, F>(self, f: F) -> IterateeMap<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        IterateeMap::new(self, f)
    }

    fn scan_with_flush<State, B, U, F>(
        self,
        init: State,
        scan: U,
        flush: F,
    ) -> IterateeScan<Self, State, U, F>
    where
        Self: Sized,
        U: FnMut(&mut State, Self::Item) -> Option<B>,
        F: FnMut(State) -> Option<B>,
    {
        IterateeScan::new(self, init, scan, flush)
    }

    fn scan<State, B, U>(self, init: State, scan: U) -> IterateeScanSimple<Self, State, U>
    where
        Self: Sized,
        U: FnMut(&mut State, Self::Item) -> Option<B>,
    {
        IterateeScanSimple::new(self, init, scan)
    }

    fn and_then<I>(self, next: I) -> IterateeAndThen<Self, I>
    where
        Self: Sized,
        I: Iteratee<Self::Item>,
    {
        IterateeAndThen {
            first: self,
            second: next,
        }
    }

    fn flat_map<I, F>(self, f: F) -> IterateeFlatMap<Self, I, F>
    where
        Self: Sized,
        I: Iteratee<In>,
        F: FnMut(Self::Item) -> I,
    {
        IterateeFlatMap::new(self, f)
    }

    fn flatten<I>(self) -> IterateeFlatten<Self, I>
    where
        Self: Sized,
        Self::Item: Iteratee<In>,
    {
        IterateeFlatten::new(self)
    }

    fn fold<State, F>(self, init: State, fold: F) -> IterateeFold<Self, State, F>
    where
        Self: Sized,
        F: FnMut(&mut State, Self::Item) -> (),
    {
        IterateeFold::new(self, init, fold)
    }

    fn transduce<It>(self, iterator: It) -> TransducedIterator<It, Self>
    where
        Self: Sized,
        It: Iterator<Item = In>,
    {
        TransducedIterator::new(iterator, self)
    }
}

pub fn unfold<In, State, Out, U>(
    init: State,
    unfold: U,
) -> Unfold<State, U, impl FnMut(State) -> Option<Out>>
where
    U: FnMut(&mut State, In) -> Option<Out>,
{
    Unfold::new(init, unfold, |_| None)
}

pub fn unfold_with_flush<In, State, Out, U, F>(
    init: State,
    unfold: U,
    flush: F,
) -> Unfold<State, U, F>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    F: FnMut(State) -> Option<Out>,
{
    Unfold::new(init, unfold, flush)
}

pub fn unfold_into<In, State, Out, I, U, F>(
    init: I,
    unfolder: U,
    extract: F,
) -> UnfoldInto<State, I, U, F>
where
    I: FnMut() -> State,
    U: FnMut(&mut State, In) -> bool,
    F: FnMut(State) -> Option<Out>,
{
    UnfoldInto::new(init, unfolder, extract)
}

pub fn collect_vec<T>(
    num: NonZeroUsize,
) -> impl Iteratee<T, Item = Vec<T>> {
    unfold(None, vec_unfolder(num.get()))
}

pub fn collect_vec_with_rem<T>(num: NonZeroUsize) -> impl Iteratee<T, Item = Vec<T>> {
    unfold_with_flush(None, vec_unfolder(num.get()), |maybe_vec| maybe_vec)
}

fn vec_unfolder<T>(n: usize) -> impl FnMut(&mut Option<Vec<T>>, T) -> Option<Vec<T>> {
    move |maybe_vec, t| match maybe_vec {
        Some(vec) => {
            vec.push(t);
            if vec.len() == n {
                maybe_vec.take()
            } else {
                None
            }
        }
        _ => {
            let new_vec = vec![t];
            if n == 1 {
                Some(new_vec)
            } else {
                *maybe_vec = Some(new_vec);
                None
            }
        }
    }
}

pub fn collect_all_vec<T>() -> impl Iteratee<T, Item = Vec<T>> {
    unfold_with_flush(
        vec![],
        |vec, input| {
            vec.push(input);
            None
        },
        |vec| Some(vec),
    )
}

#[derive(Clone)]
pub struct Comap<I, F> {
    iteratee: I,
    f: F,
}

impl<I, F> Comap<I, F> {
    fn new(iteratee: I, f: F) -> Comap<I, F> {
        Comap { iteratee, f }
    }
}

impl<In, B, I, F> Iteratee<B> for Comap<I, F>
where
    I: Iteratee<In>,
    F: FnMut(B) -> In,
{
    type Item = I::Item;

    fn feed(&mut self, input: B) -> Option<Self::Item> {
        self.iteratee.feed((self.f)(input))
    }

    fn flush(self) -> Option<Self::Item> where
        Self: Sized, {
        self.iteratee.flush()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        self.iteratee.demand_hint()
    }


}

#[derive(Clone)]
pub struct MaybeComap<I, F> {
    iteratee: I,
    f: F,
}

impl<I, F> MaybeComap<I, F> {
    fn new(iteratee: I, f: F) -> MaybeComap<I, F> {
        MaybeComap { iteratee, f }
    }
}

impl<In, B, I, F> Iteratee<B> for MaybeComap<I, F>
where
    I: Iteratee<In>,
    F: FnMut(B) -> Option<In>,
{
    type Item = I::Item;

    fn feed(&mut self, input: B) -> Option<Self::Item> {
        (self.f)(input)
            .map(|input| self.iteratee.feed(input))
            .flatten()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
    }
}

pub struct Unfold<B, U, F> {
    state: B,
    unfold: U,
    flush: F,
}

impl<State, U, F> Unfold<State, U, F> {
    fn new(init: State, unfold: U, flush: F) -> Unfold<State, U, F> {
        Unfold {
            state: init,
            unfold,
            flush,
        }
    }
}

impl<In, State, Out, U, F> Iteratee<In> for Unfold<State, U, F>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    F: FnMut(State) -> Option<Out>,
{
    type Item = Out;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let Unfold {
            state,
            unfold,
            flush: _,
        } = self;
        unfold(state, input)
    }

    fn flush(self) -> Option<Self::Item> {
        let Unfold {
            state,
            unfold: _,
            mut flush,
        } = self;
        flush(state)
    }
}

#[derive(Clone)]
pub struct IterateeMap<I, F> {
    iteratee: I,
    f: F,
}

impl<I, F> IterateeMap<I, F> {
    fn new(iteratee: I, f: F) -> IterateeMap<I, F> {
        IterateeMap { iteratee, f }
    }
}

impl<In, B, I, F> Iteratee<In> for IterateeMap<I, F>
where
    I: Iteratee<In>,
    F: FnMut(I::Item) -> B,
{
    type Item = B;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeMap { iteratee, f } = self;
        iteratee.feed(input).map(f)
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeMap { iteratee, f } = self;
        iteratee.flush().map(f)
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        self.iteratee.demand_hint()
    }
}

pub struct IterateeScan<I, State, U, F> {
    iteratee: I,
    state: State,
    scan: U,
    flush: F,
}

impl<I, State, U, F> IterateeScan<I, State, U, F> {
    fn new(iteratee: I, init: State, scan: U, flush: F) -> IterateeScan<I, State, U, F> {
        IterateeScan {
            iteratee,
            state: init,
            scan,
            flush,
        }
    }
}

impl<I, In, State, B, U, F> Iteratee<In> for IterateeScan<I, State, U, F>
where
    I: Iteratee<In>,
    U: FnMut(&mut State, I::Item) -> Option<B>,
    F: FnMut(State) -> Option<B>,
{
    type Item = B;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeScan {
            iteratee,
            state,
            scan,
            flush: _,
        } = self;
        iteratee
            .feed(input)
            .map(|out| (*scan)(state, out))
            .flatten()
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeScan {
            iteratee,
            mut state,
            mut scan,
            mut flush,
        } = self;
        iteratee
            .flush()
            .map(|out| scan(&mut state, out).or_else(|| flush(state)))
            .flatten()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
    }
}

pub struct IterateeScanSimple<I, State, U> {
    iteratee: I,
    state: State,
    scan: U,
}

impl<I, State, U> IterateeScanSimple<I, State, U> {
    fn new(iteratee: I, init: State, scan: U) -> IterateeScanSimple<I, State, U> {
        IterateeScanSimple {
            iteratee,
            state: init,
            scan,
        }
    }
}

impl<I, In, State, B, U> Iteratee<In> for IterateeScanSimple<I, State, U>
where
    I: Iteratee<In>,
    U: FnMut(&mut State, I::Item) -> Option<B>,
{
    type Item = B;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeScanSimple {
            iteratee,
            state,
            scan,
        } = self;
        iteratee
            .feed(input)
            .map(|out| (*scan)(state, out))
            .flatten()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
    }
}

pub struct IterateeAndThen<I1, I2> {
    first: I1,
    second: I2,
}

impl<S, I1, I2> Iteratee<S> for IterateeAndThen<I1, I2>
where
    I1: Iteratee<S>,
    I2: Iteratee<I1::Item>,
{
    type Item = I2::Item;

    fn feed(&mut self, input: S) -> Option<Self::Item> {
        let IterateeAndThen { first, second } = self;
        first
            .feed(input)
            .map(|intermediate| second.feed(intermediate))
            .flatten()
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeAndThen { first, mut second } = self;
        first
            .flush()
            .map(|intermediate| second.feed(intermediate))
            .flatten()
            .or_else(|| second.flush())
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.first.demand_hint().0, None)
    }
}

pub enum IterateeFlatMap<I1, I2, F> {
    First(I1, F),
    Second(I2),
}

impl<I1, I2, F> IterateeFlatMap<I1, I2, F> {
    fn new(iteratee: I1, f: F) -> IterateeFlatMap<I1, I2, F> {
        IterateeFlatMap::First(iteratee, f)
    }
}

impl<In, I1, I2, F> Iteratee<In> for IterateeFlatMap<I1, I2, F>
where
    I1: Iteratee<In>,
    I2: Iteratee<In>,
    F: FnMut(I1::Item) -> I2,
{
    type Item = I2::Item;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        match self {
            IterateeFlatMap::First(iteratee, f) => {
                if let Some(s) = iteratee.feed(input) {
                    *self = IterateeFlatMap::Second(f(s));
                }
                None
            }
            IterateeFlatMap::Second(iteratee) => iteratee.feed(input),
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        match self {
            IterateeFlatMap::First(iteratee, mut f) => {
                if let Some(s) = iteratee.flush() {
                    f(s).flush()
                } else {
                    None
                }
            }
            IterateeFlatMap::Second(iteratee) => iteratee.flush(),
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        match self {
            IterateeFlatMap::First(iteratee, _) => (iteratee.demand_hint().0, None),
            IterateeFlatMap::Second(iteratee) => iteratee.demand_hint(),
        }
    }
}

pub enum IterateeFlatten<I1, I2> {
    First(I1),
    Second(I2),
}

impl<I1, I2> IterateeFlatten<I1, I2> {
    fn new(iteratee: I1) -> IterateeFlatten<I1, I2> {
        IterateeFlatten::First(iteratee)
    }
}

impl<In, I1, I2> Iteratee<In> for IterateeFlatten<I1, I2>
where
    I1: Iteratee<In, Item = I2>,
    I2: Iteratee<In>,
{
    type Item = I2::Item;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        match self {
            IterateeFlatten::First(iteratee) => {
                if let Some(next) = iteratee.feed(input) {
                    *self = IterateeFlatten::Second(next);
                }
                None
            }
            IterateeFlatten::Second(iteratee) => iteratee.feed(input),
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        match self {
            IterateeFlatten::First(iteratee) => {
                if let Some(next) = iteratee.flush() {
                    next.flush()
                } else {
                    None
                }
            }
            IterateeFlatten::Second(iteratee) => iteratee.flush(),
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        match self {
            IterateeFlatten::First(iteratee) => (iteratee.demand_hint().0, None),
            IterateeFlatten::Second(iteratee) => iteratee.demand_hint(),
        }
    }
}

pub struct IterateeFold<I, State, F> {
    iteratee: I,
    state: State,
    f: F,
}

impl<I, State, F> IterateeFold<I, State, F> {
    fn new(iteratee: I, init: State, f: F) -> IterateeFold<I, State, F> {
        IterateeFold {
            iteratee,
            state: init,
            f,
        }
    }
}

impl<In, State, I, F> Iteratee<In> for IterateeFold<I, State, F>
where
    I: Iteratee<In>,
    F: FnMut(&mut State, I::Item) -> (),
{
    type Item = State;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeFold { iteratee, state, f } = self;
        if let Some(item) = iteratee.feed(input) {
            f(state, item);
        }
        None
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeFold {
            iteratee,
            mut state,
            mut f,
        } = self;
        if let Some(item) = iteratee.flush() {
            f(&mut state, item)
        }
        Some(state)
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct UnfoldInto<State, I, U, F> {
    maybe_state: Option<State>,
    init: I,
    unfold: U,
    extract: F,
}

impl<State, I, U, F> UnfoldInto<State, I, U, F> {
    fn new(init: I, unfold: U, extract: F) -> UnfoldInto<State, I, U, F> {
        UnfoldInto {
            maybe_state: None,
            init,
            unfold,
            extract,
        }
    }
}

impl<In, State, Out, I, U, F> Iteratee<In> for UnfoldInto<State, I, U, F>
where
    I: FnMut() -> State,
    U: FnMut(&mut State, In) -> bool,
    F: FnMut(State) -> Option<Out>,
{
    type Item = Out;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let UnfoldInto {
            init,
            maybe_state,
            unfold,
            extract,
        } = self;
        match maybe_state {
            Some(state) => {
                if unfold(state, input) {
                    maybe_state.take().map(extract).flatten()
                } else {
                    None
                }
            }
            _ => {
                let mut new_state = init();
                if unfold(&mut new_state, input) {
                    extract(new_state)
                } else {
                    *maybe_state = Some(new_state);
                    None
                }
            }
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let UnfoldInto {
            init: _,
            mut maybe_state,
            unfold: _,
            extract,
        } = self;
        maybe_state.take().map(extract).flatten()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct TransducedIterator<I, T> {
    iterator: I,
    transform: Option<T>,
}

impl<I, T> TransducedIterator<I, T> {
    fn new(iterator: I, transform: T) -> TransducedIterator<I, T> {
        TransducedIterator {
            iterator,
            transform: Some(transform),
        }
    }
}

impl<I, T> Iterator for TransducedIterator<I, T>
where
    I: Iterator,
    T: Iteratee<I::Item>,
{
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let TransducedIterator {
            iterator,
            transform,
        } = self;
        match transform {
            Some(trans) => {
                while let Some(item) = iterator.next() {
                    let result = trans.feed(item);
                    if result.is_some() {
                        return result;
                    }
                }
                transform.take().unwrap().flush()
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.iterator.size_hint().1)
    }
}
