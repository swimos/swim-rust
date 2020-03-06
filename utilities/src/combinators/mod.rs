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

    pub trait FutureCombinators: Future {
        fn map_into<T>(self) -> into::MapInto<Self, T>
        where
            Self: Sized,
            T: From<Self::Output>,
        {
            into::MapInto::new(self)
        }

        fn try_map_into<T>(self) -> try_into::MapTryInto<Self, T>
        where
            Self: Sized,
            T: TryFrom<Self::Output>,
        {
            try_into::MapTryInto::new(self)
        }
    }

    impl<X> FutureCombinators for X where X: Future {}

    pub mod into {
        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_utils::unsafe_pinned;
        use std::marker::PhantomData;
        use std::pin::Pin;

        pub struct MapInto<F, T> {
            future: F,
            _target: PhantomData<T>,
        }

        impl<F, T> MapInto<F, T> {
            unsafe_pinned!(future: F);

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
                match self.future().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(value) => Poll::Ready(value.into()),
                }
            }
        }
    }

    pub mod try_into {
        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_utils::unsafe_pinned;
        use std::convert::TryFrom;
        use std::marker::PhantomData;
        use std::pin::Pin;

        pub struct MapTryInto<F, T> {
            future: F,
            _target: PhantomData<T>,
        }

        impl<F, T> MapTryInto<F, T> {
            unsafe_pinned!(future: F);

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
                match self.future().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(value) => Poll::Ready(TryFrom::try_from(value)),
                }
            }
        }
    }
}
