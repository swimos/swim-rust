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

pub mod futures {
    use futures::Future;
    use std::convert::TryFrom;

    /// Additional combinators for futures that all return nameable types.
    ///
    pub trait FutureCombinators: Future {
        /// Apply a transformation to the output of a future, using [`Into`].
        fn map_into<T>(self) -> into::MapInto<Self, T>
        where
            Self: Sized,
            T: From<Self::Output>,
        {
            into::MapInto::new(self)
        }

        /// Apply a transformation to the otuput of a future, using [`TryInto`].
        fn try_map_into<T>(self) -> try_into::MapTryInto<Self, T>
        where
            Self: Sized,
            T: TryFrom<Self::Output>,
        {
            try_into::MapTryInto::new(self)
        }

        /// Apply a transformation to the error of the output of a result valued future, using [`Into`].
        fn map_err_into<E2>(self) -> err_into::MapErrInto<Self, E2>
        where
            Self: Sized,
        {
            err_into::MapErrInto::new(self)
        }
    }

    impl<X> FutureCombinators for X where X: Future {}

    pub mod into {
        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_project::pin_project;
        use std::marker::PhantomData;
        use std::pin::Pin;

        #[pin_project]
        pub struct MapInto<F, T> {
            #[pin]
            future: F,
            _target: PhantomData<T>,
        }

        impl<F, T> MapInto<F, T> {
            pub fn new(future: F) -> MapInto<F, T> {
                MapInto {
                    future,
                    _target: PhantomData,
                }
            }
        }

        impl<F, T> Future for MapInto<F, T>
        where
            F: Future,
            T: From<F::Output>,
        {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.project().future.poll(cx).map(Into::into)
            }
        }
    }

    pub mod try_into {
        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_project::pin_project;
        use std::convert::TryFrom;
        use std::marker::PhantomData;
        use std::pin::Pin;

        #[pin_project]
        pub struct MapTryInto<F, T> {
            #[pin]
            future: F,
            _target: PhantomData<T>,
        }

        impl<F, T> MapTryInto<F, T> {
            pub fn new(future: F) -> MapTryInto<F, T> {
                MapTryInto {
                    future,
                    _target: PhantomData,
                }
            }
        }

        impl<F, T> Future for MapTryInto<F, T>
        where
            F: Future,
            T: TryFrom<F::Output>,
        {
            type Output = Result<T, T::Error>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.project().future.poll(cx).map(TryFrom::try_from)
            }
        }
    }

    pub mod err_into {
        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_project::pin_project;
        use std::marker::PhantomData;
        use std::pin::Pin;

        #[pin_project]
        pub struct MapErrInto<F, E> {
            #[pin]
            future: F,
            _target: PhantomData<E>,
        }

        impl<F, E> MapErrInto<F, E> {
            pub fn new(future: F) -> MapErrInto<F, E> {
                MapErrInto {
                    future,
                    _target: PhantomData,
                }
            }
        }

        impl<F, T, E1, E2> Future for MapErrInto<F, E2>
        where
            F: Future<Output = Result<T, E1>>,
            E2: From<E1>,
        {
            type Output = Result<T, E2>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let projected = self.project();
                projected.future.poll(cx).map_err(Into::into)
            }
        }
    }
}
