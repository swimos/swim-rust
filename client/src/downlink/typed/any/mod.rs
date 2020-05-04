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

pub mod value {
    use crate::downlink::buffered::BufferedDownlink;
    use crate::downlink::dropping::DroppingDownlink;
    use crate::downlink::model::value::{Action, SharedValue};
    use crate::downlink::queue::QueueDownlink;
    use crate::downlink::typed::ValueDownlink;

    pub type QueueValueDownlink<T> = ValueDownlink<QueueDownlink<Action, SharedValue>, T>;
    pub type DroppingValueDownlink<T> = ValueDownlink<DroppingDownlink<Action, SharedValue>, T>;
    pub type BufferedValueDownlink<T> = ValueDownlink<BufferedDownlink<Action, SharedValue>, T>;

    /// Enumeration of specific value downlink types.
    #[derive(Debug)]
    pub enum AnyValueDownlink<T> {
        Queue(QueueValueDownlink<T>),
        Dropping(DroppingValueDownlink<T>),
        Buffered(BufferedValueDownlink<T>),
    }

    impl<T> Clone for AnyValueDownlink<T> {
        fn clone(&self) -> Self {
            match self {
                AnyValueDownlink::Queue(qdl) => AnyValueDownlink::Queue(qdl.clone()),
                AnyValueDownlink::Dropping(ddl) => AnyValueDownlink::Dropping(ddl.clone()),
                AnyValueDownlink::Buffered(bdl) => AnyValueDownlink::Buffered(bdl.clone()),
            }
        }
    }
}

pub mod map {
    use crate::downlink::buffered::BufferedDownlink;
    use crate::downlink::dropping::DroppingDownlink;
    use crate::downlink::model::map::{MapAction, ViewWithEvent};
    use crate::downlink::queue::QueueDownlink;
    use crate::downlink::typed::MapDownlink;

    pub type QueueMapDownlink<K, V> = MapDownlink<QueueDownlink<MapAction, ViewWithEvent>, K, V>;
    pub type DroppingMapDownlink<K, V> =
        MapDownlink<DroppingDownlink<MapAction, ViewWithEvent>, K, V>;
    pub type BufferedMapDownlink<K, V> =
        MapDownlink<BufferedDownlink<MapAction, ViewWithEvent>, K, V>;

    /// Enumeration of specific map downlink types.
    #[derive(Debug)]
    pub enum AnyMapDownlink<K, V> {
        Queue(QueueMapDownlink<K, V>),
        Dropping(DroppingMapDownlink<K, V>),
        Buffered(BufferedMapDownlink<K, V>),
    }

    impl<K, V> Clone for AnyMapDownlink<K, V> {
        fn clone(&self) -> Self {
            match self {
                AnyMapDownlink::Queue(qdl) => AnyMapDownlink::Queue(qdl.clone()),
                AnyMapDownlink::Dropping(ddl) => AnyMapDownlink::Dropping(ddl.clone()),
                AnyMapDownlink::Buffered(bdl) => AnyMapDownlink::Buffered(bdl.clone()),
            }
        }
    }
}
