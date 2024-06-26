// Copyright 2015-2024 Swim Inc.
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

use std::collections::HashMap;

use crate::{
    agent_lifecycle::HandlerContext,
    lanes::http::{lifecycle::HttpRequestContext, Response, UnitResponse},
};

use super::{EventHandler, HandlerAction};

pub trait HandlerFn0<'a, Context, Shared> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, F, H> HandlerFn0<'a, Context, Shared> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::Handler {
        self(shared, handler_context)
    }
}

pub trait EventFn<'a, Context, Shared, T: ?Sized> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::Handler;
}

pub trait EventConsumeFn<'a, Context, Shared, T> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, T, F, H> EventFn<'a, Context, Shared, T> for F
where
    T: ?Sized,
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &T) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::Handler {
        self(shared, handler_context, value)
    }
}

impl<'a, Context, Shared, T, F, H> EventConsumeFn<'a, Context, Shared, T> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, T) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::Handler {
        self(shared, handler_context, value)
    }
}

pub trait UpdateFn<'a, Context, Shared, T> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
        old: Option<T>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, T, F, H> UpdateFn<'a, Context, Shared, T> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &T, Option<T>) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
        old: Option<T>,
    ) -> Self::Handler {
        self(shared, handler_context, value, old)
    }
}

pub trait UpdateBorrowFn<'a, Context, Shared, T, B: ?Sized> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &B,
        old: Option<T>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, T, B, F, H> UpdateBorrowFn<'a, Context, Shared, T, B> for F
where
    B: ?Sized,
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &B, Option<T>) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &B,
        old: Option<T>,
    ) -> Self::Handler {
        self(shared, handler_context, value, old)
    }
}

pub trait TakeFn<'a, Context, Shared, T> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, T, F, H> TakeFn<'a, Context, Shared, T> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, T) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: T,
    ) -> Self::Handler {
        self(shared, handler_context, value)
    }
}

pub trait MapRemoveFn<'a, Context, Shared, K, V> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, K, V, F, H> MapRemoveFn<'a, Context, Shared, K, V> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &HashMap<K, V>, K, V) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::Handler {
        self(shared, handler_context, map, key, prev_value)
    }
}

pub trait MapUpdateFn<'a, Context, Shared, K, V> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, K, V, F, H> MapUpdateFn<'a, Context, Shared, K, V> for F
where
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &HashMap<K, V>, K, Option<V>, &V) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::Handler {
        self(shared, handler_context, map, key, prev_value, new_value)
    }
}

pub trait MapUpdateBorrowFn<'a, Context, Shared, K, V, B: ?Sized> {
    type Handler: EventHandler<Context> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &B,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, K, V, B, F, H> MapUpdateBorrowFn<'a, Context, Shared, K, V, B> for F
where
    B: ?Sized,
    H: EventHandler<Context> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, &HashMap<K, V>, K, Option<V>, &B) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &B,
    ) -> Self::Handler {
        self(shared, handler_context, map, key, prev_value, new_value)
    }
}

pub trait CueFn0<'a, T, Context, Shared> {
    type Handler: HandlerAction<Context, Completion = T> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::Handler;
}

impl<'a, T, Context, Shared, F, H> CueFn0<'a, T, Context, Shared> for F
where
    H: HandlerAction<Context, Completion = T> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + 'a,
    Shared: 'a,
    T: 'static,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::Handler {
        self(shared, handler_context)
    }
}

pub trait CueFn1<'a, In, Out, Context, Shared> {
    type Handler: HandlerAction<Context, Completion = Out> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: In,
    ) -> Self::Handler;
}

impl<'a, In, Out, Context, Shared, F, H> CueFn1<'a, In, Out, Context, Shared> for F
where
    H: HandlerAction<Context, Completion = Out> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, In) -> H + 'a,
    Shared: 'a,
    Out: 'static,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: In,
    ) -> Self::Handler {
        self(shared, handler_context, value)
    }
}

pub trait GetFn<'a, T, Context, Shared> {
    type Handler: HandlerAction<Context, Completion = Response<T>> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::Handler;
}

impl<'a, T, Context, Shared, F, H> GetFn<'a, T, Context, Shared> for F
where
    H: HandlerAction<Context, Completion = Response<T>> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, HttpRequestContext) -> H + 'a,
    Shared: 'a,
    T: 'static,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::Handler {
        self(shared, handler_context, http_context)
    }
}

pub trait RequestFn0<'a, Context, Shared> {
    type Handler: HandlerAction<Context, Completion = UnitResponse> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, F, H> RequestFn0<'a, Context, Shared> for F
where
    H: HandlerAction<Context, Completion = UnitResponse> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, HttpRequestContext) -> H + 'a,
    Shared: 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::Handler {
        self(shared, handler_context, http_context)
    }
}

pub trait RequestFn1<'a, T, Context, Shared> {
    type Handler: HandlerAction<Context, Completion = UnitResponse> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: T,
    ) -> Self::Handler;
}

impl<'a, T, Context, Shared, F, H> RequestFn1<'a, T, Context, Shared> for F
where
    H: HandlerAction<Context, Completion = UnitResponse> + 'a,
    F: Fn(&'a Shared, HandlerContext<Context>, HttpRequestContext, T) -> H + 'a,
    Shared: 'a,
    T: 'static,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: T,
    ) -> Self::Handler {
        self(shared, handler_context, http_context, value)
    }
}
