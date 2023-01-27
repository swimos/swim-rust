use bytes::BytesMut;
use futures_util::future::{ready, BoxFuture};
use futures_util::stream::Empty;
use futures_util::FutureExt;
use ratchet::{Message, NegotiatedExtension, NoExt, PayloadType, Role, WebSocket, WebSocketConfig};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use swim_model::Value;
use swim_recon::parser::{parse_recognize, Span};
use swim_recon::printer::print_recon;
use swim_runtime::error::{ConnectionError, IoError};
use swim_runtime::remote::net::dns::{BoxDnsResolver, DnsResolver};
use swim_runtime::remote::table::SchemeHostPort;
use swim_runtime::remote::{ExternalConnections, Listener, SchemeSocketAddr};
use swim_runtime::ws::{WsConnections, WsOpenFuture};
use swim_warp::envelope::Envelope;
use tokio::io::DuplexStream;

#[derive(Debug)]
struct Inner {
    addrs: HashMap<SchemeHostPort, SchemeSocketAddr>,
    sockets: HashMap<SocketAddr, DuplexStream>,
}

impl Inner {
    fn new<R, S>(resolver: R, sockets: S) -> Inner
    where
        R: IntoIterator<Item = (SchemeHostPort, SchemeSocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        Inner {
            addrs: HashMap::from_iter(resolver),
            sockets: HashMap::from_iter(sockets),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockExternalConnections {
    inner: Arc<Mutex<Inner>>,
}

impl MockExternalConnections {
    pub fn new<R, S>(resolver: R, sockets: S) -> MockExternalConnections
    where
        R: IntoIterator<Item = (SchemeHostPort, SchemeSocketAddr)>,
        S: IntoIterator<Item = (SocketAddr, DuplexStream)>,
    {
        MockExternalConnections {
            inner: Arc::new(Mutex::new(Inner::new(resolver, sockets))),
        }
    }
}

pub struct MockListener;
impl Listener for MockListener {
    type Socket = DuplexStream;
    type AcceptStream = Empty<io::Result<(DuplexStream, SchemeSocketAddr)>>;

    fn into_stream(self) -> Self::AcceptStream {
        panic!("Unexpected listener invocation")
    }
}

impl ExternalConnections for MockExternalConnections {
    type Socket = DuplexStream;
    type ListenerType = MockListener;

    fn bind(
        &self,
        _addr: SocketAddr,
    ) -> BoxFuture<'static, io::Result<(SocketAddr, Self::ListenerType)>> {
        panic!("Unexpected bind invocation")
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>> {
        let result = self
            .inner
            .lock()
            .unwrap()
            .sockets
            .remove(&addr)
            .ok_or_else(|| ErrorKind::NotFound.into());
        ready(result).boxed()
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.clone())
    }

    fn lookup(
        &self,
        host: SchemeHostPort,
    ) -> BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>> {
        self.resolve(host).boxed()
    }
}

impl DnsResolver for MockExternalConnections {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>>;

    fn resolve(&self, host: SchemeHostPort) -> Self::ResolveFuture {
        let result = match self.inner.lock().unwrap().addrs.get(&host) {
            Some(sock) => Ok(vec![*sock]),
            None => Err(io::ErrorKind::NotFound.into()),
        };
        ready(result).boxed()
    }
}

pub enum WsAction {
    Open,
    Fail(ConnectionError),
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

impl WsConnections<DuplexStream> for MockWs {
    type Ext = NoExt;
    type Error = ConnectionError;

    fn open_connection(
        &self,
        socket: DuplexStream,
        addr: String,
    ) -> WsOpenFuture<DuplexStream, Self::Ext, Self::Error> {
        let result = match self.states.get(&addr) {
            Some(WsAction::Open) => Ok(WebSocket::from_upgraded(
                WebSocketConfig::default(),
                socket,
                NegotiatedExtension::from(NoExt),
                BytesMut::default(),
                Role::Client,
            )),
            Some(WsAction::Fail(e)) => Err(e.clone()),
            None => Err(ConnectionError::Io(IoError::new(ErrorKind::NotFound, None))),
        };
        ready(result).boxed()
    }

    fn accept_connection(
        &self,
        _socket: DuplexStream,
    ) -> WsOpenFuture<DuplexStream, Self::Ext, Self::Error> {
        panic!("Unexpected accept connection invocation")
    }
}

pub struct Lane<'l> {
    node: String,
    lane: String,
    server: RefCell<&'l mut Server>,
}

impl<'l> Lane<'l> {
    pub async fn read(&mut self) -> Envelope {
        let Lane { server, .. } = self;
        let Server { buf, transport } = server.get_mut();

        match transport.borrow_mut().read(buf).await.unwrap() {
            Message::Text => {}
            m => panic!("Unexpected message type: {:?}", m),
        }
        let read = String::from_utf8(buf.to_vec()).unwrap();
        buf.clear();

        parse_recognize::<Envelope>(Span::new(&read), false).unwrap()
    }

    pub async fn write(&mut self, env: Envelope) {
        let Lane { server, .. } = self;
        let Server { transport, .. } = server.get_mut();

        let response = print_recon(&env);
        transport
            .write(format!("{}", response), PayloadType::Text)
            .await
            .unwrap();
    }

    pub async fn write_bytes(&mut self, msg: &[u8]) {
        let Lane { server, .. } = self;
        let Server { transport, .. } = server.get_mut();

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

    pub async fn await_sync<V: Into<Value>>(&mut self, val: V) {
        match self.read().await {
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, self.node);
                assert_eq!(lane_uri, self.lane);
                self.write(Envelope::Event {
                    node_uri: node_uri.clone(),
                    lane_uri: lane_uri.clone(),
                    body: Some(val.into()),
                })
                .await;
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

    pub async fn send_event<V: Into<Value>>(&mut self, val: V) {
        self.write(Envelope::Event {
            node_uri: self.node.clone().into(),
            lane_uri: self.lane.clone().into(),
            body: Some(val.into()),
        })
        .await;
    }

    pub async fn await_closed(&mut self) {
        let Lane { server, .. } = self;
        let Server { buf, transport } = server.get_mut();

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
    pub fn lane_for<N, L>(&mut self, node: N, lane: L) -> Lane<'_>
    where
        N: ToString,
        L: ToString,
    {
        Lane {
            node: node.to_string(),
            lane: lane.to_string(),
            server: RefCell::new(self),
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
