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

use futures::Future;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{ConsumerContext, Rebalance, StreamConsumer},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message, Statistics, TopicPartitionList,
};
use swimos_agent::{
    agent_lifecycle::{ConnectorContext, HandlerContext},
    event_handler::{
        ConstHandler, EventHandler, HandlerAction, HandlerActionExt, TryHandlerActionExt,
        UnitHandler,
    },
};
use swimos_model::Value;
use tracing::{debug, error, info, warn};

use crate::{Connector, ConnectorNext, GenericConnectorAgent, ValueLaneSelectorFn};

type ConContext = HandlerContext<GenericConnectorAgent>;

pub struct KafkaConnector {
    configuration: KafkaConnectorConfiguration,
}

impl KafkaConnector {
    pub fn new(configuration: KafkaConnectorConfiguration) -> Self {
        KafkaConnector { configuration }
    }
}

type LoggingConsumer = StreamConsumer<KafkaClientContext>;

impl Connector for KafkaConnector {
    type StreamError = KafkaError;

    fn on_start(&self) -> impl EventHandler<crate::GenericConnectorAgent> + '_ {
        UnitHandler::default()
    }

    fn on_stop(&self) -> impl EventHandler<crate::GenericConnectorAgent> + '_ {
        UnitHandler::default()
    }

    type ConnectorState = Consumer;

    fn create_state(&self) -> Result<Self::ConnectorState, Self::StreamError> {
        let KafkaConnector { configuration } = self;
        let mut client_builder = ClientConfig::new();
        configuration.properties.iter().for_each(|(k, v)| {
            client_builder.set(k, v);
        });
        let consumer = client_builder
            .set_log_level(configuration.log_level)
            .create_with_context::<_, LoggingConsumer>(KafkaClientContext)?;
        Ok(Consumer::new(consumer))
    }
}

pub struct Consumer {
    consumer: Option<LoggingConsumer>,
}

impl Consumer {
    fn new(inner: LoggingConsumer) -> Self {
        Consumer {
            consumer: Some(inner),
        }
    }
}

impl ConnectorNext<KafkaError> for Consumer {
    fn next_state(
        mut self,
    ) -> impl Future<
        Output: HandlerAction<
            GenericConnectorAgent,
            Completion = Option<Result<Self, KafkaError>>,
        > + Send
                    + 'static,
    > + Send
           + 'static {
        async move {
            let handler_context: ConContext = ConContext::default();
            if let Some(consumer) = &mut self.consumer {
                Some(handler_context.value(Ok(self)))
            } else {
                None
            }
        }
    }

    fn commit(
        self,
    ) -> Result<
        impl Future<
                Output: HandlerAction<
                    GenericConnectorAgent,
                    Completion = Result<Self, KafkaError>,
                > + Send
                            + 'static,
            > + Send
            + 'static,
        Self,
    > {
        Ok(async move { ConstHandler::from(Ok(self)) })
    }
}

#[derive(Clone, Debug)]
pub struct KafkaConnectorConfiguration {
    properties: HashMap<String, String>,
    log_level: RDKafkaLogLevel,
}

struct KafkaClientContext;

impl ClientContext for KafkaClientContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                info!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                info!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!("Kafka Connector: {} {}", fac, log_message)
            }
        }
    }

    fn stats(&self, statistics: Statistics) {
        info!("Kafka Connector Statistics: {:?}", statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("Kafka Connector: {}: {}", error, reason);
    }
}

impl ConsumerContext for KafkaClientContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

const BODY_LANE: &str = "body";

async fn consume_record(
    consumer: &LoggingConsumer,
) -> impl HandlerAction<GenericConnectorAgent, Completion = Result<(), KafkaError>> + Send + 'static
{
    let handler_context: ConContext = ConContext::default();
    handler_context
        .value(consumer.recv().await)
        .and_then_ok(|msg| {
            let context: ConnectorContext<GenericConnectorAgent> = ConnectorContext::default();
            let set_body = if let Some(Ok(body)) = msg.payload_view::<str>() {
                Some(context.set_value(
                    ValueLaneSelectorFn::new(BODY_LANE.to_string()),
                    Value::from(body),
                ))
            } else {
                None
            };
            set_body.discard()
        });
    ConstHandler::from(Ok(()))
}
