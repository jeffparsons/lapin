use lapin_async;
use lapin_async::format::frame::Frame;
use std::default::Default;
use std::io;
use futures::{future,Future,Poll,Stream};
use futures::sync::oneshot;
use tokio_io::{AsyncRead,AsyncWrite};
use tokio_timer::Interval;
use std::sync::{Arc,Mutex};
use std::time::{Duration,Instant};

use transport::*;
use channel::{Channel, ConfirmSelectOptions};

/// the Client structures connects to a server and creates channels
//#[derive(Clone)]
pub struct Client<T> {
    transport:         Arc<Mutex<AMQPTransport<T>>>,
    pub configuration: ConnectionConfiguration,
}

impl<T> Clone for Client<T>
    where T: Send {
  fn clone(&self) -> Client<T> {
    Client {
      transport:     self.transport.clone(),
      configuration: self.configuration.clone(),
    }
  }
}
#[derive(Clone,Debug,PartialEq)]
pub struct ConnectionOptions {
  pub username:  String,
  pub password:  String,
  pub vhost:     String,
  pub frame_max: u32,
  pub heartbeat: u16,
}

impl Default for ConnectionOptions {
  fn default() -> ConnectionOptions {
    ConnectionOptions {
      username:  "guest".to_string(),
      password:  "guest".to_string(),
      vhost:     "/".to_string(),
      frame_max: 0,
      heartbeat: 0,
    }
  }
}

pub type ConnectionConfiguration = lapin_async::connection::Configuration;

/// A heartbeat task.
pub struct Heartbeat {
    handle: Option<HeartbeatHandle>,
    pulse: Box<Future<Item = (), Error = io::Error> + Send>,
}

impl Heartbeat {
    fn new<T>(transport: Arc<Mutex<AMQPTransport<T>>>, heartbeat: u16) -> Self
    where
      T: AsyncRead + AsyncWrite + Send + 'static
    {
        use self::future::{loop_fn, Either, Loop};
        let (tx, rx) = oneshot::channel();
        let interval = Interval::new(Instant::now(), Duration::from_secs(heartbeat.into()))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
            .into_future();
        let neverending = loop_fn((interval, rx), move |(f, rx)| {
            let transport = Arc::clone(&transport);
            let heartbeat = f.and_then(move |(_instant, interval)| {
                debug!("poll heartbeat");
                let mut transport = match transport.lock() {
                    Ok(mut t) => t,
                    Err(_err) => return Err((io::Error::new(io::ErrorKind::Other, "Couldn't lock transport mutex"), interval)),
                };
                debug!("Sending heartbeat");
                match transport.send_frame(Frame::Heartbeat(0)) {
                    Ok(_) => Ok(interval),
                    Err(e) => Err((e, interval)),
                }
            }).or_else(|(err, _interval)| {
                error!("Error occured in heartbeat interval: {}", err);
                Err(err)
            });
            heartbeat.select2(rx).then(|res| {
                match res {
                    Ok(Either::A((interval, rx))) => Ok(Loop::Continue((interval.into_future(), rx))),
                    Ok(Either::B((_rx, _interval))) => Ok(Loop::Break(())),
                    Err(Either::A((err, _rx))) => Err(io::Error::new(io::ErrorKind::Other, err)),
                    Err(Either::B((err, _interval))) => Err(io::Error::new(io::ErrorKind::Other, err)),
                }
            })
        });
        Heartbeat {
            handle: Some(HeartbeatHandle(tx)),
            pulse: Box::new(neverending),
        }
    }

    /// Get the handle for this heartbeat.
    ///
    /// As there can only be one handle for a given heartbeat task, this function can return
    /// `None` if the handle to this heartbeat was already acquired.
    pub fn handle(&mut self) -> Option<HeartbeatHandle> {
        self.handle.take()
    }
}

impl Future for Heartbeat {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pulse.poll()
    }
}

/// A handle to stop a connection heartbeat.
pub struct HeartbeatHandle(oneshot::Sender<()>);

impl HeartbeatHandle {
    /// Signals the heartbeat task to stop sending packets to the broker.
    pub fn stop(self) {
        if let Err(_) = self.0.send(()) {
            warn!("Couldn't send stop signal to heartbeat: already gone");
        }
    }
}

impl<T: AsyncRead+AsyncWrite+Send+'static> Client<T> {
  /// takes a stream (TCP, TLS, unix socket, etc) and uses it to connect to an AMQP server.
  ///
  /// this method returns a future that resolves once the connection handshake is done.
  /// The result is a tuple containing a client that can be used to create a channel and a callback
  /// to which you need to pass the client to get a future that will handle the Heartbeat. The
  /// heartbeat future should be run in a dedicated thread so that nothing can prevent it from
  /// dispatching events on time.
  /// If we ran it as part of the "main" chain of futures, we might end up not sending
  /// some heartbeats if we don't poll often enough (because of some blocking task or such).
  pub fn connect(stream: T, options: &ConnectionOptions) -> Box<Future<Item = (Self, Heartbeat), Error = io::Error> + Send> {
    Box::new(AMQPTransport::connect(stream, options).and_then(|transport| {
      debug!("got client service");
      Box::new(future::ok(Self::connect_internal(transport)))
    }))
  }

  fn connect_internal(transport: AMQPTransport<T>) -> (Self, Heartbeat) {
      let configuration = transport.conn.configuration.clone();
      let transport = Arc::new(Mutex::new(transport));
      let heartbeat = Heartbeat::new(transport.clone(), configuration.heartbeat);
      let client = Client { configuration, transport };
      (client, heartbeat)
  }

  /// creates a new channel
  ///
  /// returns a future that resolves to a `Channel` once the method succeeds
  pub fn create_channel(&self) -> Box<Future<Item = Channel<T>, Error = io::Error> + Send> {
    Channel::create(self.transport.clone())
  }

  /// returns a future that resolves to a `Channel` once the method succeeds
  /// the channel will support RabbitMQ's confirm extension
  pub fn create_confirm_channel(&self, options: ConfirmSelectOptions) -> Box<Future<Item = Channel<T>, Error = io::Error> + Send> {

    //FIXME: maybe the confirm channel should be a separate type
    //especially, if we implement transactions, the methods should be available on the original channel
    //but not on the confirm channel. And the basic publish method should have different results
    Box::new(self.create_channel().and_then(move |channel| {
      let ch = channel.clone();

      channel.confirm_select(&options).map(|_| ch)
    }))
  }
}
