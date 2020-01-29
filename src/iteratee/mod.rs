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

/// Dual of ['Iterator'] that can repeatedly consume some number of items before producing
/// an output. In general, ['Iteratee']s are stateful and can be flushed to obtain their
/// final state when the are no longer required.
///
/// # Examples
/// ```
/// use std::num::NonZeroUsize;
/// use swim_rust::iteratee::*;
///
/// let size = NonZeroUsize::new(2).unwrap();
/// let mut iteratee = collect_vec_with_rem(size);
///
/// //Collects elements until an output can be produced.
/// assert!(iteratee.feed(4).is_none());
/// assert_eq!(iteratee.feed(-1), Some(vec![4, -1]));
///
/// //In general, an iteratee instance can be used multiple times.
/// assert!(iteratee.feed(7).is_none());
///
/// //Flushing the iteratee consumes it after which it can no longer be used.
/// assert_eq!(iteratee.flush(), Some(vec![7]));
/// ```
pub trait Iteratee<In> {
    /// The type of the values that will be produced by the iteratee.
    type Item;

    /// Feed a single value into the iteratee. If a value can then be produced this will return
    /// ['Some'].
    fn feed(&mut self, input: In) -> Option<Self::Item>;

    /// Feed all values from an ['Iterator'] into this iteratee, collecting all outputs generated
    /// into a vector.
    fn feed_all<U>(&mut self, inputs: U) -> Vec<Self::Item>
    where
        U: Iterator<Item = In>,
    {
        inputs.flat_map(|input| self.feed(input)).collect()
    }

    /// Flush the state of the iteratee, consuming it. By default this does nothing and must be
    /// overridden by implementors.
    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        None
    }

    /// Gives a hint of how many items this iteratee will consume before producing a result. The
    /// returned value is a range from the minimum number of items to the maximum. This should
    /// obey the same contract as specified for ['size_hint()'] on ['Iterator'].
    fn demand_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    /// Apply a transformation to the input values for this iteratee.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity().comap(|i: i32| i.to_string());
    ///
    /// assert_eq!(iteratee.feed(2), Some("2".to_owned()));
    /// ```
    fn comap<B, F>(self, f: F) -> Comap<Self, F>
    where
        Self: Sized,
        F: FnMut(B) -> In,
    {
        Comap::new(self, f)
    }

    /// Apply a transformation that may filter out values to the input values of this iteratee.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity().maybe_comap(|i: i32| {
    ///     if i % 2 == 0 {
    ///         Some(i.to_string())
    ///     } else {
    ///         None
    ///     }   
    /// });
    ///
    /// assert!(iteratee.feed(1).is_none());
    /// assert_eq!(iteratee.feed(2), Some("2".to_owned()));
    /// ```
    fn maybe_comap<B, F>(self, f: F) -> MaybeComap<Self, F>
    where
        Self: Sized,
        F: FnMut(B) -> Option<In>,
    {
        MaybeComap::new(self, f)
    }

    /// Apply a transformation to the outputs of this iteratee.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity().map(|i: i32| i.to_string());
    ///
    /// assert_eq!(iteratee.feed(2), Some("2".to_string()));
    /// ```
    fn map<B, F>(self, f: F) -> IterateeMap<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        IterateeMap::new(self, f)
    }

    /// Apply a transformation to the outputs of this iteratee that may filter out some of the
    /// values.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity().maybe_map(|i: i32| {
    ///     if i % 2 == 0 {
    ///         Some(i.to_string())
    ///     } else {
    ///         None
    ///     }
    /// });
    ///
    /// assert!(iteratee.feed(1).is_none());
    /// assert_eq!(iteratee.feed(2), Some("2".to_string()));
    /// ```
    fn maybe_map<B, F>(self, f: F) -> IterateeMaybeMap<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        IterateeMaybeMap::new(self, f)
    }

    /// Apply a stateful transformation to the outputs of this iteratee. The final value of the
    /// state is ignored on flush.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// //Stores the largest values that has been seen in the state an only produces an output
    /// //when a new largest value is seen.
    /// let mut iteratee = identity::<i32>().scan(0, |max, i| {
    ///     if i > *max {
    ///         *max = i;
    ///         Some(i)
    ///     } else {
    ///         None
    ///     }
    /// });
    ///
    /// assert_eq!(iteratee.feed(4), Some(4));
    /// assert!(iteratee.feed(2).is_none());
    /// assert_eq!(iteratee.feed(7), Some(7));
    /// assert!(iteratee.feed(4).is_none());
    ///
    /// assert!(iteratee.flush().is_none());
    /// ```
    fn scan<State, B, U>(self, init: State, scan: U) -> IterateeScanSimple<Self, State, U>
    where
        Self: Sized,
        U: FnMut(&mut State, Self::Item) -> Option<B>,
    {
        IterateeScanSimple::new(self, init, scan)
    }

    /// Apply a stateful transformation to the outputs of this iteratee. The final value of the
    /// state may be output on flush..
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// //Stores the previously seen value in the state and returns it on each input. Outputs
    /// //the value of the state on flush.
    /// let mut iteratee = identity::<i32>().scan_with_flush(
    ///        None,
    ///        |prev, i| match *prev {
    ///            Some(p) => {
    ///                *prev = Some(i);
    ///                Some(p)
    ///            }
    ///            _ => {
    ///                *prev = Some(i);
    ///                None
    ///            }
    ///        },
    ///        |prev| prev,
    ///    );
    ///
    /// assert!(iteratee.feed(2).is_none());
    /// assert_eq!(iteratee.feed(7), Some(2));
    /// assert_eq!(iteratee.feed(3), Some(7));
    ///
    /// assert_eq!(iteratee.flush(), Some(3));
    /// ```
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

    /// Filter the outputs of this iteratee using a predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity::<i32>().filter(|i| i % 2 == 0);
    ///
    /// assert!(iteratee.feed(1).is_none());
    /// assert_eq!(iteratee.feed(6), Some(6));
    ///
    /// ```
    fn filter<P>(self, predicate: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        Filter::new(self, predicate)
    }

    /// Compose this iteratee with another that consumes its output type.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use swim_rust::iteratee::*;
    ///
    /// let size = NonZeroUsize::new(2).unwrap();
    ///
    /// //Collects even inputs into vectors of length 2.
    /// let mut iteratee = identity::<i32>()
    ///     .filter(|i| i % 2 == 0)
    ///     .and_then(collect_vec(size));
    ///
    /// assert!(iteratee.feed(1).is_none());
    /// assert!(iteratee.feed(2).is_none());
    /// assert_eq!(iteratee.feed(10), Some(vec![2, 10]));
    /// ```
    ///
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

    /// Compose together two fallible iteratees where the error types can be unified.
    fn and_then_fallible<I, T1, T2, E1, E2>(self, next: I) -> IterateeAndThenFallible<Self, I>
    where
        Self: Iteratee<In, Item = Result<T1, E1>> + Sized,
        I: Iteratee<T1, Item = Result<T2, E2>>,
        E2: From<E1>,
    {
        IterateeAndThenFallible {
            first: self,
            second: next,
        }
    }

    /// Chooses another iterateee to use based on the input and then delegates all following inputs
    /// to that iteratee until it produces a value. This process is repeated indefinitely.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use swim_rust::iteratee::*;
    ///
    /// //Assembles a vector the length of which is determined by the first value.
    /// let mut iteratee = identity::<usize>()
    ///        .maybe_map(|n| NonZeroUsize::new(n))
    ///        .flat_map(|i| collect_vec(i));
    ///
    /// assert!(iteratee.feed(2).is_none());
    /// assert!(iteratee.feed(6).is_none());
    /// assert_eq!(iteratee.feed(17), Some(vec![6, 17]));
    /// ```
    fn flat_map<I, F>(self, f: F) -> IterateeFlatMap<Self, I, F>
    where
        Self: Sized,
        I: Iteratee<In>,
        F: FnMut(Self::Item) -> I,
    {
        IterateeFlatMap::new(self, f)
    }

    ///Replace the flush output of this iteratee with a specified item.
    fn with_flush(self, value: Self::Item) -> WithFlush<Self, Self::Item>
    where
        Self: Sized,
    {
        WithFlush::new(self, value)
    }

    ///Remove the flush output of this iteratee.
    fn without_flush(self) -> WithFlush<Self, Self::Item>
    where
        Self: Sized,
    {
        WithFlush::new_opt(self, None)
    }

    /// Flatten an iteratee of iteratees. This is a simplified version of 'flat_map()' where the
    /// mapping function is the identity.
    fn flatten<I>(self) -> IterateeFlatten<Self, I>
    where
        Self: Sized,
        Self::Item: Iteratee<In>,
    {
        IterateeFlatten::new(self)
    }

    /// Fold over the outputs of this iteratee, emitting a value only on flush.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    ///
    /// let mut iteratee = identity::<i32>().fold(0, |sum, i| *sum = *sum + i);
    ///
    /// assert!(iteratee.feed(3).is_none());
    /// assert!(iteratee.feed(-1).is_none());
    /// assert!(iteratee.feed(7).is_none());
    ///
    /// assert_eq!(iteratee.flush(), Some(9));
    ///
    /// ```
    fn fold<State, F>(self, init: State, fold: F) -> IterateeFold<Self, State, F>
    where
        Self: Sized,
        F: FnMut(&mut State, Self::Item) -> (),
    {
        IterateeFold::new(self, init, fold)
    }

    /// Transduce an iterator with this iteratee. This creates a new iterator that will feed
    /// tha values of the original iterator into this iteratee and yields the outputs of this
    /// iteratee. The iteratee will not be flushed and can be used again.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use swim_rust::iteratee::*;
    ///
    /// let size = NonZeroUsize::new(2).unwrap();
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let mut iteratee = copy_into_vec(size);
    ///
    /// let output = iteratee.transduce(data.iter()).collect::<Vec<_>>();
    ///
    /// assert_eq!(output, vec![vec![1, 2], vec![3, 4]]);
    /// ```
    fn transduce<It>(&mut self, iterator: It) -> TransducedRefIterator<It, Self>
    where
        Self: Sized,
        It: Iterator<Item = In>,
    {
        TransducedRefIterator::new(iterator, self)
    }

    /// Transduce an iterator with this iteratee. This creates a new iterator that will feed
    /// tha values of the original iterator into this iteratee and yields the outputs of this
    /// iteratee. This consumes the iteratee which will be flushed after the input iterator is
    /// exhausted.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use swim_rust::iteratee::*;
    ///
    /// let size = NonZeroUsize::new(2).unwrap();
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let mut iteratee = copy_into_vec_with_rem(size);
    ///
    /// let output = iteratee.transduce_into(data.iter()).collect::<Vec<_>>();
    ///
    /// assert_eq!(output, vec![vec![1, 2], vec![3, 4], vec![5]]);
    /// ```
    fn transduce_into<It>(self, iterator: It) -> TransducedIterator<It, Self>
    where
        Self: Sized,
        It: Iterator<Item = In>,
    {
        TransducedIterator::new(iterator, self)
    }

    /// Create a new iteratee that will delegates to this iteratee until it emits a value and then
    /// will never emit another value (including on flush).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use swim_rust::iteratee::*;
    ///
    /// let size = NonZeroUsize::new(2).unwrap();
    ///
    /// let mut iteratee = collect_vec(size).fuse();
    ///
    /// assert!(iteratee.feed(8).is_none());
    /// assert_eq!(iteratee.feed(4), Some(vec![8, 4]));
    ///
    /// assert!(iteratee.feed(2).is_none());
    /// assert!(iteratee.feed(0).is_none());
    ///
    /// assert!(iteratee.flush().is_none());
    /// ```
    ///
    fn fuse(self) -> IterateeFuse<Self>
    where
        Self: Sized,
    {
        IterateeFuse::new(self)
    }

    /// Create a new iteratee that will delegates to this iteratee until it emits an error and then
    /// will never emit another value (including on flush).
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::iteratee::*;
    /// use swim_rust::iteratee::Iteratee;
    /// let mut iteratee = identity::<&str>().map(|s| {
    ///     if s.is_empty() { Err(()) } else { Ok(s) }
    /// }).fuse_on_error();
    ///
    /// assert_eq!(iteratee.feed("Hello"), Some(Ok("Hello")));
    /// assert_eq!(iteratee.feed("World"), Some(Ok("World")));
    /// assert_eq!(iteratee.feed(""), Some(Err(())));
    /// assert!(iteratee.feed("Hello").is_none());
    ///
    /// assert!(iteratee.flush().is_none());
    ///
    /// ```
    fn fuse_on_error<T, E>(self) -> IterateeFuseOnError<Self>
    where
        Self: Iteratee<In, Item = Result<T, E>> + Sized,
    {
        IterateeFuseOnError::new(self)
    }
}

/// Create a stateful iteratee that generates its outputs from its internal state and the value
/// of each input.
///
/// # Examples
///
/// ```
/// use swim_rust::iteratee::*;
///
/// //Emits every fourth value.
/// let mut iteratee = unfold(0, |count: &mut u32, i: i32| {
///     *count = *count + 1;
///     if *count % 4 == 0 {
///         Some(i)
///     } else {
///         None
///     }
/// });
///
/// assert!(iteratee.feed(3).is_none());
/// assert!(iteratee.feed(-1).is_none());
/// assert!(iteratee.feed(12).is_none());
/// assert_eq!(iteratee.feed(17), Some(17));
/// assert!(iteratee.feed(-66).is_none());
/// ```
///
pub fn unfold<In, State, Out, U>(
    init: State,
    unfold: U,
) -> Unfold<State, U, impl FnMut(State) -> Option<Out>, impl Fn(&State) -> (usize, Option<usize>)>
where
    U: FnMut(&mut State, In) -> Option<Out>,
{
    Unfold::new(init, unfold, |_| None, |_: &State| (0, None))
}

/// Create a stateful iteratee that generates its outputs from its internal state and the value
/// of each input. Allows the caller to provide a demand hint, otherwise identical to 'unfold()'.
pub fn unfold_with_hint<In, State, Out, U, H>(
    init: State,
    unfold: U,
    hint: H,
) -> Unfold<State, U, impl FnMut(State) -> Option<Out>, H>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    H: Fn(&State) -> (usize, Option<usize>),
{
    Unfold::new(init, unfold, |_| None, hint)
}

/// Create a stateful iteratee that generates its outputs from its internal state and the value
/// of each input. The iteratee may also produce a value on flush.
///
/// # Examples
///
/// ```
/// use swim_rust::iteratee::*;
///
/// let mut iteratee = unfold_with_flush(None, |prev, i| {
///     prev.replace(i)
/// }, |prev| prev);
///
/// assert!(iteratee.feed(1).is_none());
/// assert_eq!(iteratee.feed(3), Some(1));
/// assert_eq!(iteratee.flush(), Some(3));
/// ```
///
pub fn unfold_with_flush<In, State, Out, U, F>(
    init: State,
    unfold: U,
    flush: F,
) -> Unfold<State, U, F, impl Fn(&State) -> (usize, Option<usize>)>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    F: FnMut(State) -> Option<Out>,
{
    Unfold::new(init, unfold, flush, |_: &State| (0, None))
}

/// Create a stateful iteratee that generates its outputs from its internal state and the value
/// of each input. The iteratee may also produce a value on flush. Allows the caller to provide a
/// demand hint, otherwise identical to 'unfold_with_flush()'.
pub fn unfold_with_flush_and_hint<In, State, Out, U, F, H>(
    init: State,
    unfold: U,
    flush: F,
    hint: H,
) -> Unfold<State, U, F, H>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    F: FnMut(State) -> Option<Out>,
    H: Fn(&State) -> (usize, Option<usize>),
{
    Unfold::new(init, unfold, flush, hint)
}

/// Variant unfold iteratee that splits the determination of whether a value should be emitted and
/// the actual extraction of that value from the state.
pub fn unfold_with_extract<In, State, Out, I, U, F>(
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

/// Collects the input values into vectors of a fixed length. Leftover elements are discarded
/// on flush.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use swim_rust::iteratee::*;
///
/// let size = NonZeroUsize::new(3).unwrap();
///
/// let mut iteratee = collect_vec::<i32>(size);
///
/// assert!(iteratee.feed(1).is_none());
/// assert!(iteratee.feed(2).is_none());
/// assert_eq!(iteratee.feed(3), Some(vec![1, 2, 3]));
/// assert!(iteratee.feed(4).is_none());
///
/// assert!(iteratee.flush().is_none());
/// ```
pub fn collect_vec<T>(num: NonZeroUsize) -> impl Iteratee<T, Item = Vec<T>> {
    unfold_with_hint(None, vec_unfolder(num.get(), |t| t), vec_hint(num))
}

/// Collects the input values into vectors of a fixed length. Leftover elements are emitted as an
/// underfull vector on flush.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use swim_rust::iteratee::*;
///
/// let size = NonZeroUsize::new(3).unwrap();
///
/// let mut iteratee = collect_vec_with_rem::<i32>(size);
///
/// assert!(iteratee.feed(1).is_none());
/// assert!(iteratee.feed(2).is_none());
/// assert_eq!(iteratee.feed(3), Some(vec![1, 2, 3]));
/// assert!(iteratee.feed(4).is_none());
///
/// assert_eq!(iteratee.flush(), Some(vec![4]));
/// ```
pub fn collect_vec_with_rem<T>(num: NonZeroUsize) -> impl Iteratee<T, Item = Vec<T>> {
    unfold_with_flush_and_hint(
        None,
        vec_unfolder(num.get(), |t| t),
        |maybe_vec| maybe_vec,
        vec_hint(num),
    )
}

/// Create an iteratee that consumes copyable items by reference and copies them into a vector.
/// Otherwise identical to 'collect_vec()'.
pub fn copy_into_vec<'a, T: Copy + 'a>(num: NonZeroUsize) -> impl Iteratee<&'a T, Item = Vec<T>> {
    unfold_with_hint(None, vec_unfolder(num.get(), |t: &'a T| *t), vec_hint(num))
}

/// Create an iteratee that consumes copyable items by reference and copies them into a vector.
/// Otherwise identical to 'collect_vec_with_rem()'.
pub fn copy_into_vec_with_rem<'a, T: Copy + 'a>(
    num: NonZeroUsize,
) -> impl Iteratee<&'a T, Item = Vec<T>> {
    unfold_with_flush_and_hint(
        None,
        vec_unfolder(num.get(), |t: &'a T| *t),
        |maybe_vec| maybe_vec,
        vec_hint(num),
    )
}

/// Collects all inputs into a single vector that is emitted on flush.
///
/// # Examples
///
/// ```
/// use swim_rust::iteratee::*;
///
/// let mut iteratee = collect_all_vec::<i32>();
///
/// assert!(iteratee.feed(34).is_none());
/// assert!(iteratee.feed(-12).is_none());
/// assert!(iteratee.feed(0).is_none());
///
/// assert_eq!(iteratee.flush(), Some(vec![34, -12, 0]));
/// ```
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

/// The trivial iteratee that emits all of its inputs unchanged.
pub fn identity<T>() -> impl Iteratee<T, Item = T> {
    return Identity {};
}

/// The trivial iteratee that never emits anything.
pub fn never<T>() -> impl Iteratee<T, Item = T> {
    return Never {};
}

/// Adds single item lookahead to incoming values.
///
/// # Examples
///
/// ```
/// use swim_rust::iteratee::*;
///
/// let mut iteratee = look_ahead::<char>();
///
/// assert!(iteratee.feed('a').is_none());
/// assert_eq!(iteratee.feed('b'), Some(('a', Some('b'))));
/// assert_eq!(iteratee.feed('c'), Some(('b', Some('c'))));
/// assert_eq!(iteratee.flush(), Some(('c', None)));
/// ```
pub fn look_ahead<T: Clone>() -> impl Iteratee<T, Item = (T, Option<T>)> {
    unfold_with_flush(
        None,
        |prev, current: T| {
            let prev_opt = std::mem::replace(prev, Some(current.clone()));
            prev_opt.map(|p| (p, Some(current)))
        },
        |prev| prev.map(|p| (p, None)),
    )
}

/// Attaches the byte offset in UTF8 of each character received as input.
pub fn utf8_byte_offsets() -> impl Iteratee<char, Item = (usize, char)> {
    unfold(0, |offset, c: char| {
        let result = (*offset, c);
        *offset += c.len_utf8();
        Some(result)
    })
}

fn vec_hint<T>(num: NonZeroUsize) -> impl Fn(&Option<Vec<T>>) -> (usize, Option<usize>) {
    move |v| {
        let diff: usize = match v {
            Some(v) => num.get() - v.len(),
            _ => num.get(),
        };
        (diff, Some(diff))
    }
}

fn vec_unfolder<S, T>(
    n: usize,
    mut conform: impl FnMut(S) -> T,
) -> impl FnMut(&mut Option<Vec<T>>, S) -> Option<Vec<T>> {
    move |maybe_vec, s| match maybe_vec {
        Some(vec) => {
            vec.push(conform(s));
            if vec.len() == n {
                maybe_vec.take()
            } else {
                None
            }
        }
        _ => {
            let new_vec = vec![conform(s)];
            if n == 1 {
                Some(new_vec)
            } else {
                *maybe_vec = Some(new_vec);
                None
            }
        }
    }
}

pub struct Identity;
pub struct Never;

impl<T> Iteratee<T> for Identity {
    type Item = T;

    fn feed(&mut self, input: T) -> Option<Self::Item> {
        Some(input)
    }
}

impl<T> Iteratee<T> for Never {
    type Item = T;

    fn feed(&mut self, _: T) -> Option<Self::Item> {
        None
    }
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

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
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

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.iteratee.flush()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
    }
}

pub struct Unfold<B, U, F, H> {
    state: B,
    unfold: U,
    flush: F,
    hint: H,
}

impl<State, U, F, H> Unfold<State, U, F, H> {
    fn new(init: State, unfold: U, flush: F, hint: H) -> Unfold<State, U, F, H> {
        Unfold {
            state: init,
            unfold,
            flush,
            hint,
        }
    }
}

impl<In, State, Out, U, F, H> Iteratee<In> for Unfold<State, U, F, H>
where
    U: FnMut(&mut State, In) -> Option<Out>,
    F: FnMut(State) -> Option<Out>,
    H: Fn(&State) -> (usize, Option<usize>),
{
    type Item = Out;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let Unfold { state, unfold, .. } = self;
        unfold(state, input)
    }

    fn flush(self) -> Option<Self::Item> {
        let Unfold {
            state, mut flush, ..
        } = self;
        flush(state)
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        let Unfold { state, hint, .. } = self;
        hint(state)
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

pub struct IterateeMaybeMap<I, F> {
    iteratee: I,
    f: F,
}

impl<I, F> IterateeMaybeMap<I, F> {
    fn new(iteratee: I, f: F) -> IterateeMaybeMap<I, F> {
        IterateeMaybeMap { iteratee, f }
    }
}

impl<In, Out, I, F> Iteratee<In> for IterateeMaybeMap<I, F>
where
    I: Iteratee<In>,
    F: FnMut(I::Item) -> Option<Out>,
{
    type Item = Out;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeMaybeMap { iteratee, f } = self;
        iteratee.feed(input).map(f).flatten()
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.iteratee.flush().map(self.f).flatten()
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
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
        match iteratee.flush() {
            Some(v) => scan(&mut state, v).or_else(|| flush(state)),
            _ => flush(state),
        }
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

pub struct IterateeAndThenFallible<I1, I2> {
    first: I1,
    second: I2,
}

impl<In, I1, I2, T1, T2, E1, E2> Iteratee<In> for IterateeAndThenFallible<I1, I2>
where
    I1: Iteratee<In, Item = Result<T1, E1>>,
    I2: Iteratee<T1, Item = Result<T2, E2>>,
    E2: From<E1>,
{
    type Item = Result<T2, E2>;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeAndThenFallible { first, second } = self;
        match first.feed(input) {
            Some(Err(e)) => Some(Err(e.into())),
            Some(Ok(t)) => second.feed(t),
            _ => None,
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeAndThenFallible { first, mut second } = self;
        match first.flush() {
            Some(Err(e)) => Some(Err(e.into())),
            Some(Ok(t)) => second.feed(t).or_else(|| second.flush()),
            _ => second.flush(),
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.first.demand_hint().0, None)
    }
}

pub struct IterateeFlatMap<I1, I2, F> {
    selector: I1,
    maybe_current: Option<I2>,
    f: F,
}

impl<I1, I2, F> IterateeFlatMap<I1, I2, F> {
    fn new(iteratee: I1, f: F) -> IterateeFlatMap<I1, I2, F> {
        IterateeFlatMap {
            selector: iteratee,
            maybe_current: None,
            f,
        }
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
        let IterateeFlatMap {
            selector,
            maybe_current,
            f,
        } = self;
        match maybe_current {
            Some(current) => {
                let result = current.feed(input);
                if result.is_some() {
                    *maybe_current = None;
                }
                result
            }
            _ => {
                if let Some(s) = selector.feed(input) {
                    *maybe_current = Some(f(s));
                }
                None
            }
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeFlatMap {
            selector,
            maybe_current,
            mut f,
        } = self;
        match maybe_current {
            Some(current) => current.flush(),
            _ => selector.flush().map(|it| f(it).flush()).flatten(),
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        let IterateeFlatMap {
            selector,
            maybe_current,
            f: _,
        } = self;
        match maybe_current {
            Some(current) => current.demand_hint(),
            _ => (selector.demand_hint().0, None),
        }
    }
}

pub struct IterateeFlatten<I1, I2> {
    selector: I1,
    maybe_current: Option<I2>,
}

impl<I1, I2> IterateeFlatten<I1, I2> {
    fn new(iteratee: I1) -> IterateeFlatten<I1, I2> {
        IterateeFlatten {
            selector: iteratee,
            maybe_current: None,
        }
    }
}

impl<In, I1, I2> Iteratee<In> for IterateeFlatten<I1, I2>
where
    I1: Iteratee<In, Item = I2>,
    I2: Iteratee<In>,
{
    type Item = I2::Item;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeFlatten {
            selector,
            maybe_current,
        } = self;
        match maybe_current {
            Some(current) => {
                let result = current.feed(input);
                if result.is_some() {
                    *maybe_current = None;
                }
                result
            }
            _ => {
                if let Some(s) = selector.feed(input) {
                    *maybe_current = Some(s);
                }
                None
            }
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeFlatten {
            selector,
            maybe_current,
        } = self;
        match maybe_current {
            Some(current) => current.flush(),
            _ => selector.flush().map(|it| it.flush()).flatten(),
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        let IterateeFlatten {
            selector,
            maybe_current,
        } = self;
        match maybe_current {
            Some(current) => current.demand_hint(),
            _ => (selector.demand_hint().0, None),
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
            mut maybe_state,
            extract,
            ..
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

pub struct TransducedRefIterator<'a, I, T> {
    iterator: I,
    transform: &'a mut T,
}

impl<'a, I, T> TransducedRefIterator<'a, I, T> {
    fn new(iterator: I, transform: &'a mut T) -> TransducedRefIterator<'a, I, T> {
        TransducedRefIterator {
            iterator,
            transform,
        }
    }
}

impl<'a, I, T> Iterator for TransducedRefIterator<'a, I, T>
where
    I: Iterator,
    T: Iteratee<I::Item>,
{
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let TransducedRefIterator {
            iterator,
            transform,
        } = self;
        while let Some(item) = iterator.next() {
            let result = transform.feed(item);
            if result.is_some() {
                return result;
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.iterator.size_hint().1)
    }
}

pub struct IterateeFuse<I> {
    iteratee: I,
    done: bool,
}

impl<I> IterateeFuse<I> {
    fn new(iteratee: I) -> IterateeFuse<I> {
        IterateeFuse {
            iteratee,
            done: false,
        }
    }
}

impl<In, I> Iteratee<In> for IterateeFuse<I>
where
    I: Iteratee<In>,
{
    type Item = I::Item;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            let result = self.iteratee.feed(input);
            if result.is_some() {
                self.done = true;
            }
            result
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        if self.done {
            None
        } else {
            self.iteratee.flush()
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        if self.done {
            (usize::max_value(), None)
        } else {
            self.iteratee.demand_hint()
        }
    }
}

pub struct IterateeFuseOnError<I> {
    iteratee: I,
    failed: bool,
}

impl<I> IterateeFuseOnError<I> {
    fn new(iteratee: I) -> IterateeFuseOnError<I> {
        IterateeFuseOnError {
            iteratee,
            failed: false,
        }
    }
}

impl<In, T, E, I> Iteratee<In> for IterateeFuseOnError<I>
where
    I: Iteratee<In, Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let IterateeFuseOnError { iteratee, failed } = self;
        if *failed {
            None
        } else {
            let result = iteratee.feed(input);
            if let Some(Err(_)) = &result {
                *failed = true;
            }
            result
        }
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let IterateeFuseOnError { iteratee, failed } = self;
        if failed {
            None
        } else {
            iteratee.flush()
        }
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        let IterateeFuseOnError { iteratee, failed } = self;
        if *failed {
            (usize::max_value(), None)
        } else {
            iteratee.demand_hint()
        }
    }
}

pub struct Filter<I, P> {
    iteratee: I,
    predicate: P,
}

impl<I, P> Filter<I, P> {
    fn new(iteratee: I, predicate: P) -> Filter<I, P> {
        Filter {
            iteratee,
            predicate,
        }
    }
}

impl<In, I, P> Iteratee<In> for Filter<I, P>
where
    I: Iteratee<In>,
    P: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        let Filter {
            iteratee,
            predicate,
        } = self;
        iteratee.feed(input).filter(predicate)
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.iteratee.flush().filter(self.predicate)
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        (self.iteratee.demand_hint().0, None)
    }
}

pub struct WithFlush<I, T> {
    iteratee: I,
    last: Option<T>,
}

impl<I, T> WithFlush<I, T> {
    fn new_opt(iteratee: I, last: Option<T>) -> WithFlush<I, T> {
        WithFlush { iteratee, last }
    }

    fn new(iteratee: I, val: T) -> WithFlush<I, T> {
        WithFlush {
            iteratee,
            last: Some(val),
        }
    }
}

impl<In, I, T> Iteratee<In> for WithFlush<I, T>
where
    I: Iteratee<In, Item = T>,
{
    type Item = T;

    fn feed(&mut self, input: In) -> Option<Self::Item> {
        self.iteratee.feed(input)
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.last
    }

    fn demand_hint(&self) -> (usize, Option<usize>) {
        self.iteratee.demand_hint()
    }
}
