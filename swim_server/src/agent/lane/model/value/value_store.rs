use crate::agent::lane::store::error::{StoreErrorHandler, StoreErrorReport};
use crate::agent::lane::store::m2::StoreIo;
use crate::agent::lane::store::{LaneStoreTask, UninitialisedLaneStore};
use crate::agent::model::value::ValueLane;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use store::{PlaneStore, StoreError, ValueDataModel};

pub struct UninitialisedValueLaneStore<D, T> {
    lane: ValueLane<T>,
    model: ValueDataModel<D>,
}

impl<D, T> UninitialisedValueLaneStore<D, T> {
    pub fn new(lane: ValueLane<T>, model: ValueDataModel<D>) -> Self {
        UninitialisedValueLaneStore { lane, model }
    }
}

impl<D, T> UninitialisedLaneStore for UninitialisedValueLaneStore<D, T>
where
    D: PlaneStore,
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Initialised = ValueLaneStore<D, Arc<T>>;

    fn initialise(self) -> BoxFuture<'static, Result<Self::Initialised, StoreError>> {
        let f = async move {
            let UninitialisedValueLaneStore { lane, model } = self;
            match model.load() {
                Ok(Some(value)) => {
                    lane.store(value).await;
                    Ok(ValueLaneStore {
                        model,
                        _pd: Default::default(),
                    })
                }
                Ok(None) => Ok(ValueLaneStore {
                    model,
                    _pd: Default::default(),
                }),
                Err(e) => Err(e),
            }
        };
        f.boxed()
    }
}

pub struct ValueLaneStore<D, T> {
    model: ValueDataModel<D>,
    _pd: PhantomData<T>,
}

impl<D, T> ValueLaneStore<D, T> {
    #[cfg(test)]
    pub fn new(model: ValueDataModel<D>) -> Self {
        ValueLaneStore {
            model,
            _pd: Default::default(),
        }
    }
}
