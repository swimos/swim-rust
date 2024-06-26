use bytes::BytesMut;
use futures_util::future::{ready, BoxFuture};
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use ratchet::{
    ExtensionProvider, Message, NegotiatedExtension, NoExt, PayloadType, Role, WebSocket,
    WebSocketConfig, WebSocketStream,
};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use swimos_form::Form;
use swimos_messages::remote_protocol::FindNode;
use swimos_model::{Text, Value};
use swimos_recon::parser::parse_recognize;
use swimos_recon::print_recon;
use swimos_remote::dns::{BoxDnsResolver, DnsResolver};
use swimos_remote::websocket::{RatchetError, WebsocketClient, WebsocketServer, WsOpenFuture};
use swimos_remote::{
    ClientConnections, ConnectionError, ConnectionResult, Listener, ListenerError, Scheme,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[derive(Debug)]
struct Inner {
    addrs: HashMap<(String, u16), SocketAddr>,
    sockets: HashMap<SocketAddr, DuplexStream>,
}

impl Inner {
    fn new<R, S>(resolver: R, sockets: S) -> Inner
    where
        R: IntoIterator<Item = ((String, u16), SocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        Inner {
            addrs: HashMap::from_iter(resolver),
            sockets: HashMap::from_iter(sockets),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockClientConnections {
    inner: Arc<Mutex<Inner>>,
}

impl MockClientConnections {
    pub fn new<R, S>(resolver: R, sockets: S) -> MockClientConnections
    where
        R: IntoIterator<Item = ((String, u16), SocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        MockClientConnections {
            inner: Arc::new(Mutex::new(Inner::new(resolver, sockets))),
        }
    }
}

impl ClientConnections for MockClientConnections {
    type ClientSocket = DuplexStream;

    fn try_open(
        &self,
        _scheme: Scheme,
        _host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnectionResult<Self::ClientSocket>> {
        async move {
            self.inner
                .lock()
                .await
                .sockets
                .remove(&addr)
                .ok_or_else(|| ConnectionError::ConnectionFailed(ErrorKind::NotFound.into()))
        }
        .boxed()
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.clone())
    }

    fn lookup(
        &self,
        host: String,
        port: u16,
    ) -> BoxFuture<'static, std::io::Result<Vec<SocketAddr>>> {
        self.resolve(host, port).boxed()
    }
}

impl DnsResolver for MockClientConnections {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        let inner = self.inner.clone();
        async move {
            match inner.lock().await.addrs.get(&(host, port)) {
                Some(sock) => Ok(vec![*sock]),
                None => Err(io::ErrorKind::NotFound.into()),
            }
        }
        .boxed()
    }
}

pub enum WsAction {
    Open,
    Fail(Box<dyn Fn() -> RatchetError + Send + Sync + 'static>),
}

impl WsAction {
    pub fn fail<F>(with: F) -> WsAction
    where
        F: Fn() -> RatchetError + Send + Sync + 'static,
    {
        WsAction::Fail(Box::new(with))
    }
}

pub struct MockWs {
    states: HashMap<String, WsAction>,
}

impl MockWs {
    pub fn new<S>(states: S) -> MockWs
    where
        S: IntoIterator<Item = (String, WsAction)>,
    {
        MockWs {
            states: HashMap::from_iter(states),
        }
    }
}

impl WebsocketClient for MockWs {
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        _provider: &'a Provider,
        addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static,
    {
        let result = match self.states.get(&addr) {
            Some(WsAction::Open) => Ok(WebSocket::from_upgraded(
                WebSocketConfig::default(),
                socket,
                NegotiatedExtension::from(None),
                BytesMut::default(),
                Role::Client,
            )),
            Some(WsAction::Fail(e)) => Err(e()),
            None => Err(ratchet::Error::new(ratchet::ErrorKind::Http).into()),
        };
        ready(result).boxed()
    }
}

impl WebsocketServer for MockWs {
    type WsStream<Sock, Ext> =
        BoxStream<'static, Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>>;

    fn wrap_listener<Sock, L, Provider>(
        &self,
        _listener: L,
        _provider: Provider,
        _find: mpsc::Sender<FindNode>,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        L: Listener<Sock> + Send + 'static,
        Provider: ExtensionProvider + Send + Sync + Unpin + 'static,
        Provider::Extension: Send + Sync + Unpin + 'static,
    {
        futures::stream::pending().boxed()
    }
}

#[derive(Clone, Debug, PartialEq, Form)]

pub enum Envelope {
    #[form(tag = "link")]
    Link {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "sync")]
    Sync {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlink")]
    Unlink {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "command")]
    Command {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "linked")]
    Linked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "synced")]
    Synced {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlinked")]
    Unlinked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "event")]
    Event {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
}

pub struct Lane {
    node: String,
    lane: String,
    server: Arc<Mutex<Server>>,
}

impl Lane {
    pub async fn read(&mut self) -> Envelope {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { buf, transport } = &mut guard.deref_mut();

        match transport.read(buf).await.unwrap() {
            Message::Text => {}
            m => panic!("Unexpected message type: {:?}", m),
        }
        let read = String::from_utf8(buf.to_vec()).unwrap();
        buf.clear();

        parse_recognize::<Envelope>(read.as_str(), false).unwrap()
    }

    pub async fn write(&mut self, env: Envelope) {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { transport, .. } = &mut guard.deref_mut();

        let response = print_recon(&env);
        transport
            .write(format!("{}", response), PayloadType::Text)
            .await
            .unwrap();
    }

    pub async fn write_bytes(&mut self, msg: &[u8]) {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { transport, .. } = &mut guard.deref_mut();

        transport.write(msg, PayloadType::Text).await.unwrap();
    }

    pub async fn await_link(&mut self) {
        match self.read().await {
            Envelope::Link {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);
                self.write(Envelope::Linked {
                    node_uri: node_uri.clone(),
                    lane_uri: lane_uri.clone(),
                    rate: None,
                    prio: None,
                    body: None,
                })
                .await;
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn await_sync<V: Form>(&mut self, val: Vec<V>) {
        match self.read().await {
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);

                for v in val {
                    self.write(Envelope::Event {
                        node_uri: node_uri.clone(),
                        lane_uri: lane_uri.clone(),
                        body: Some(v.as_value()),
                    })
                    .await;
                }

                self.write(Envelope::Synced {
                    node_uri: node_uri.clone(),
                    lane_uri: lane_uri.clone(),
                    body: None,
                })
                .await;
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn await_command(&mut self, expected: i32) {
        match self.read().await {
            Envelope::Command {
                node_uri,
                lane_uri,
                body: Some(val),
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);
                assert_eq!(val, Value::Int32Value(expected));
            }
            e => panic!("Unexpected envelope {:?}", e),
        }
    }

    pub async fn send_unlinked(&mut self) {
        self.write(Envelope::Unlinked {
            node_uri: self.node.clone().into(),
            lane_uri: self.lane.clone().into(),
            body: None,
        })
        .await;
    }

    pub async fn send_event<V: Form>(&mut self, val: V) {
        self.write(Envelope::Event {
            node_uri: self.node.clone().into(),
            lane_uri: self.lane.clone().into(),
            body: Some(val.as_value()),
        })
        .await;
    }

    pub async fn await_closed(&mut self) {
        let Lane { server, .. } = self;
        let mut guard = server.lock().await;
        let Server { buf, transport } = &mut guard.deref_mut();

        match transport.borrow_mut().read(buf).await.unwrap() {
            Message::Close(_) => {}
            m => panic!("Unexpected message type: {:?}", m),
        }
    }
}

pub struct Server {
    pub buf: BytesMut,
    pub transport: WebSocket<DuplexStream, NoExt>,
}

impl Server {
    pub fn lane_for<N, L>(server: Arc<Mutex<Self>>, node: N, lane: L) -> Lane
    where
        N: ToString,
        L: ToString,
    {
        Lane {
            node: node.to_string(),
            lane: lane.to_string(),
            server,
        }
    }
}

impl Server {
    pub fn new(transport: DuplexStream) -> Server {
        Server {
            buf: BytesMut::new(),
            transport: WebSocket::from_upgraded(
                WebSocketConfig::default(),
                transport,
                NegotiatedExtension::from(NoExt),
                BytesMut::default(),
                Role::Server,
            ),
        }
    }
}
