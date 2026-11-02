//! High-level session: connect, set up, keep alive, deliver events.

use std::net::{IpAddr, SocketAddr};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::beacon::DEVICE_PORT;
use crate::conn::{Connection, EventReader, PacketWriter};
use crate::error::Result;
use crate::event::Event;
use crate::packet::{Hand, Request};

/// Options for [`Session::start`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptions {
    /// Putting hand, sent before measuring starts. Default right.
    pub hand: Hand,
    /// Deliver the camera frames the device streams while it locates the mat
    /// as [`Event::CameraFrame`]. When off, the frames are dropped in the
    /// reader thread without being decoded. The session starts and stops the
    /// device's camera stream either way. Default off.
    pub camera: bool,
    /// Device TCP port. Default [`DEVICE_PORT`] (8889).
    pub port: u16,
    /// TCP connect and greeting timeout. Default 5 s.
    pub connect_timeout: Duration,
    /// Time to wait for each `N_OK` reply during the handshake. Default 5 s.
    pub reply_timeout: Duration,
    /// Longest wait for the device to report the mat found before measuring
    /// starts anyway, in [`Session::start`] and [`Session::find_mat`].
    /// Default 10 s.
    pub mat_timeout: Duration,
    /// Keepalive period. Default 2 s.
    pub keepalive_interval: Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            hand: Hand::Right,
            camera: false,
            port: DEVICE_PORT,
            connect_timeout: Duration::from_secs(5),
            reply_timeout: Duration::from_secs(5),
            mat_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(2),
        }
    }
}

/// A measuring session with the device.
///
/// [`Session::start`] runs the whole setup:
///
/// 1. TCP connect to port 8889 and ZMTP greeting.
/// 2. `N_VERIFY_ID`, wait for `N_OK`.
/// 3. `N_REQ_CONNECT`, wait for `N_OK`.
/// 4. `sendImage`, then ask the device to locate the mat (`resyncToMat`,
///    which makes the device stream camera frames until told to stop), and
///    wait for [`Event::MatFound`] (at most [`SessionOptions::mat_timeout`]).
///    Then `stopSendImage`.
/// 5. Send the putting hand, then begin measuring.
///
/// From step 3 on, a background thread sends a keepalive every
/// [`SessionOptions::keepalive_interval`], and a reader thread delivers every
/// device event, including those received during setup, to
/// [`events`](Self::events). The channel is unbounded: drain it, especially
/// with [`SessionOptions::camera`] on.
///
/// [`find_mat`](Self::find_mat) runs step 4 again on a live session, and
/// every new round ([`restart_match`](Self::restart_match)) does too.
///
/// [`close`](Self::close) (or dropping the session) ends measuring, tells the
/// device the client is leaving, and closes the socket. When the device drops
/// the connection, the event channel disconnects. So it does when a keepalive
/// cannot be sent: the socket is shut down, the reader thread ends, and
/// [`is_connected`](Self::is_connected) turns false.
#[derive(Debug)]
pub struct Session {
    writer: PacketWriter,
    events: Receiver<Event>,
    stop_keepalive: Option<Sender<()>>,
    keepalive: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    /// Cleared by the reader thread before the event channel disconnects.
    receiving: Arc<AtomicBool>,
    /// Signalled by the reader thread on the next [`Event::MatFound`], and
    /// dropped when the reader thread ends.
    mat_waiter: Arc<Mutex<Option<Sender<()>>>>,
    mat_timeout: Duration,
    // Lock order: `finder` before `hand`. The finder thread takes only
    // `hand`, never `finder`, so a caller holding `finder` can join it.
    /// The putting hand, and whether the finder thread has yet to send it.
    hand: Arc<Mutex<HandState>>,
    /// The thread finishing a [`find_mat`](Self::find_mat).
    finder: Mutex<Option<JoinHandle<()>>>,
    closed: Arc<AtomicBool>,
    /// Handed to the next finder thread, which waits on it twice once it has
    /// sent the hand and match begin.
    #[cfg(test)]
    finder_pause: Mutex<Option<Arc<Barrier>>>,
}

/// The putting hand for the current round.
#[derive(Debug)]
struct HandState {
    hand: Hand,
    /// The finder thread is running and has not yet sent `hand`. It sends
    /// `hand` as it is once the mat is found.
    pending: bool,
}

impl Session {
    /// Connect to the device at `ip` and start measuring.
    ///
    /// # Errors
    ///
    /// Connection, greeting and handshake failures: socket errors,
    /// [`Error::Timeout`](crate::Error::Timeout),
    /// [`Error::Greeting`](crate::Error::Greeting),
    /// [`Error::Closed`](crate::Error::Closed).
    pub fn start(ip: IpAddr, options: &SessionOptions) -> Result<Self> {
        let addr = SocketAddr::new(ip, options.port);
        let mut conn = Connection::connect(addr, options.connect_timeout)?;
        conn.handshake(options.reply_timeout)?;
        let (reader, writer) = conn.split();

        let (event_tx, events) = mpsc::channel();
        let mat_waiter = Arc::new(Mutex::new(None));
        let receiving = Arc::new(AtomicBool::new(true));
        let deliver_frames = options.camera;
        let reader = {
            let mat_waiter = Arc::clone(&mat_waiter);
            let receiving = Arc::clone(&receiving);
            thread::Builder::new()
                .name("threeputt-reader".into())
                .spawn(move || {
                    read_loop(reader, &event_tx, &mat_waiter, deliver_frames);
                    receiving.store(false, Ordering::SeqCst);
                })?
        };

        let mut session = Self {
            writer: writer.clone(),
            events,
            stop_keepalive: None,
            keepalive: None,
            reader: Some(reader),
            receiving,
            mat_waiter,
            mat_timeout: options.mat_timeout,
            hand: Arc::new(Mutex::new(HandState {
                hand: options.hand,
                pending: false,
            })),
            finder: Mutex::new(None),
            closed: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            finder_pause: Mutex::new(None),
        };
        // From here on, an early return drops `session`, which closes it.

        let (stop_tx, stop_rx) = mpsc::channel();
        let interval = options.keepalive_interval;
        session.keepalive = Some(
            thread::Builder::new()
                .name("threeputt-keepalive".into())
                .spawn(move || keepalive_loop(&writer, &stop_rx, interval))?,
        );
        session.stop_keepalive = Some(stop_tx);

        let mat_found = session.await_mat_found();
        session.send(Request::StartCamera)?;
        session.send(Request::ResyncToMat)?;
        // Times out, or ends early if the device drops the connection; the
        // sends below then report the failure.
        let _ = mat_found.recv_timeout(options.mat_timeout);
        session.send(Request::StopCamera)?;
        session.send(Request::SetHand(options.hand))?;
        session.send(Request::MatchBegin)?;
        Ok(session)
    }

    /// Device events, in arrival order. Disconnects when the connection
    /// ends.
    #[must_use]
    pub fn events(&self) -> &Receiver<Event> {
        &self.events
    }

    /// Send a packet.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn send(&self, request: Request) -> Result<()> {
        self.writer.send(request)
    }

    /// End the current round and start a new one with `hand`, for example
    /// after the player switches hands.
    ///
    /// A new round always locates the mat again: this runs the same steps as
    /// [`find_mat`](Self::find_mat) with `hand` as the putting hand, which
    /// later [`find_mat`](Self::find_mat) calls keep. When a previous
    /// `find_mat` is still waiting for the mat, the round it begins uses
    /// `hand` and nothing else is sent. Once that round has sent its hand,
    /// a call starts another round.
    ///
    /// # Errors
    ///
    /// As [`find_mat`](Self::find_mat).
    pub fn restart_match(&self, hand: Hand) -> Result<()> {
        let mut finder = lock(&self.finder);
        {
            let mut state = lock(&self.hand);
            state.hand = hand;
            if state.pending {
                return Ok(());
            }
        }
        self.begin_round(&mut finder)
    }

    /// Locate the mat again, for example when the device locked onto the
    /// wrong spot because someone stood in its view as the session started.
    ///
    /// The device finds the mat only between rounds, as in
    /// [`start`](Self::start), so this ends the round and runs the same steps
    /// as `start` from there: match end (`PM_API_MatchEnded`), `sendImage`,
    /// `resyncToMat`, then, once the device reports the mat found (at most
    /// [`SessionOptions::mat_timeout`]), `stopSendImage`, the putting hand
    /// last set, and match begin (`PM_API_MatchBegin`).
    ///
    /// Returns once the first three are sent. A background thread sends the
    /// rest, so [`Event::FindingMat`], [`Event::MatFound`] and
    /// [`Event::PuttReady`] arrive on [`events`](Self::events) as usual.
    /// Camera frames follow [`SessionOptions::camera`]. A call while a
    /// previous one is still waiting for the mat does nothing. Closing the
    /// session ends the wait: it sends `stopSendImage`, then the goodbye.
    ///
    /// # Errors
    ///
    /// Socket errors from the first three sends. Errors from the later sends
    /// are not reported here; a dead connection ends the event channel.
    pub fn find_mat(&self) -> Result<()> {
        let mut finder = lock(&self.finder);
        if lock(&self.hand).pending {
            return Ok(());
        }
        self.begin_round(&mut finder)
    }

    /// End the round and start the finder thread for the next one. The
    /// caller holds the `finder` lock and has checked that no finder thread
    /// has yet to send its hand.
    fn begin_round(&self, finder: &mut Option<JoinHandle<()>>) -> Result<()> {
        // A previous finder thread has sent its hand and is exiting.
        if let Some(h) = finder.take() {
            let _ = h.join();
        }
        let mat_found = self.await_mat_found();
        self.send(Request::MatchEnded)?;
        self.send(Request::StartCamera)?;
        self.send(Request::ResyncToMat)?;

        let writer = self.writer.clone();
        let hand = Arc::clone(&self.hand);
        let closed = Arc::clone(&self.closed);
        let timeout = self.mat_timeout;
        #[cfg(test)]
        let pause = lock(&self.finder_pause).take();
        // Set before the thread starts, so it cannot clear `pending` first.
        let mut state = lock(&self.hand);
        state.pending = true;
        let spawned = thread::Builder::new()
            .name("threeputt-find-mat".into())
            .spawn(move || {
                let _ = mat_found.recv_timeout(timeout);
                if closed.load(Ordering::SeqCst) {
                    let _ = writer.send(Request::StopCamera);
                    return;
                }
                {
                    // Held through the sends, so a hand change either lands
                    // before them and is sent, or after `pending` clears and
                    // starts a new round.
                    let mut state = lock(&hand);
                    let _ = writer
                        .send(Request::StopCamera)
                        .and_then(|()| writer.send(Request::SetHand(state.hand)))
                        .and_then(|()| writer.send(Request::MatchBegin));
                    state.pending = false;
                }
                #[cfg(test)]
                if let Some(pause) = pause {
                    pause.wait();
                    pause.wait();
                }
            });
        match spawned {
            Ok(h) => {
                *finder = Some(h);
                Ok(())
            }
            Err(e) => {
                state.pending = false;
                Err(e.into())
            }
        }
    }

    /// A receiver signalled on the next [`Event::MatFound`]. It disconnects
    /// when the reader thread ends or the session closes.
    fn await_mat_found(&self) -> Receiver<()> {
        let (tx, rx) = mpsc::channel();
        *lock(&self.mat_waiter) = Some(tx);
        rx
    }

    /// Whether the reader thread is still receiving from the device.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.reader.is_some() && self.receiving.load(Ordering::SeqCst)
    }

    /// Stop measuring, say goodbye and close the connection.
    ///
    /// # Errors
    ///
    /// The first socket error while sending the goodbye packets or shutting
    /// down. The connection is closed either way.
    pub fn close(mut self) -> Result<()> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // End a find-mat wait and stop keepalives first so nothing follows
        // the goodbye.
        drop(lock(&self.mat_waiter).take());
        if let Some(h) = lock(&self.finder).take() {
            let _ = h.join();
        }
        drop(self.stop_keepalive.take());
        if let Some(h) = self.keepalive.take() {
            let _ = h.join();
        }
        let ended = self.writer.send(Request::MatchEnded);
        let paused = self.writer.send(Request::AppPause);
        let shut = self.writer.shutdown();
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
        ended.and(paused).and(shut)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_loop(
    mut reader: EventReader,
    events: &Sender<Event>,
    mat_waiter: &Mutex<Option<Sender<()>>>,
    deliver_frames: bool,
) {
    while let Ok(event) = reader.recv_filtered(deliver_frames) {
        if event == Event::MatFound
            && let Some(tx) = lock(mat_waiter).take()
        {
            let _ = tx.send(());
        }
        if events.send(event).is_err() {
            break;
        }
    }
    drop(lock(mat_waiter).take());
}

fn keepalive_loop(writer: &PacketWriter, stop: &Receiver<()>, interval: Duration) {
    loop {
        match stop.recv_timeout(interval) {
            Err(RecvTimeoutError::Timeout) => {
                if writer.send(Request::Alive).is_err() {
                    // The connection is unusable. Shutting it down ends the
                    // reader thread, which a device that has vanished would
                    // otherwise leave blocked in a read.
                    let _ = writer.shutdown();
                    return;
                }
            }
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use std::io::Write;
    use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
    use std::time::Instant;

    use super::*;
    use crate::testing::{MockConfig, MockDevice, ReceivedPacket};

    const WAIT: Duration = Duration::from_secs(5);

    /// A failed keepalive write shuts the socket down, so a reader blocked in
    /// a read (the device is silent and has not closed) ends and the event
    /// channel disconnects.
    #[test]
    fn a_failed_keepalive_ends_the_reader() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, released) = mpsc::channel::<()>();
        let device = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(&crate::zmtp::greeting(crate::zmtp::SOCKET_ROUTER))
                .unwrap();
            // Holds the connection open, sending nothing and ignoring what
            // arrives (the client's FIN included), until released.
            let _ = released.recv();
        });

        let stream = TcpStream::connect(addr).unwrap();
        // Writes on this handle fail from now on; reads are unaffected.
        let breaker = stream.try_clone().unwrap();
        let (reader, writer) = Connection::from_stream(stream).unwrap().split();
        breaker.shutdown(Shutdown::Write).unwrap();

        let (event_tx, events) = mpsc::channel();
        let mat_waiter = Mutex::new(None);
        let read = thread::spawn(move || read_loop(reader, &event_tx, &mat_waiter, false));
        let (_stop_tx, stop_rx) = mpsc::channel();
        let keepalive = thread::spawn({
            let writer = writer.clone();
            move || keepalive_loop(&writer, &stop_rx, Duration::from_millis(20))
        });

        assert_eq!(
            events.recv_timeout(WAIT),
            Err(RecvTimeoutError::Disconnected),
            "the reader ends once the keepalive fails"
        );
        read.join().unwrap();
        keepalive.join().unwrap();
        drop(release);
        device.join().unwrap();
    }

    fn non_alive(packets: &[ReceivedPacket]) -> Vec<String> {
        packets
            .iter()
            .filter(|p| p.packet_id != "N_ALIVE")
            .map(|p| p.json.clone())
            .collect()
    }

    fn round(hand: Hand) -> Vec<String> {
        [
            Request::MatchEnded,
            Request::StartCamera,
            Request::ResyncToMat,
            Request::StopCamera,
            Request::SetHand(hand),
            Request::MatchBegin,
        ]
        .map(Request::to_json)
        .to_vec()
    }

    fn wait_for(session: &Session, want: &Event) {
        let deadline = Instant::now() + WAIT;
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            if session.events().recv_timeout(left).ok().as_ref() == Some(want) {
                return;
            }
        }
        panic!("no {want:?}");
    }

    /// A hand change after the finder thread has sent its hand, while the
    /// thread is still running, starts a new round with the new hand.
    #[test]
    fn restart_match_after_the_finder_sent_its_hand_starts_a_new_round() {
        let mock = MockDevice::spawn(MockConfig::default()).unwrap();
        let options = SessionOptions {
            port: mock.port(),
            mat_timeout: WAIT,
            ..SessionOptions::default()
        };
        let session = Session::start(IpAddr::V4(Ipv4Addr::LOCALHOST), &options).unwrap();
        wait_for(&session, &Event::PuttReady);
        let before = non_alive(&mock.received()).len();

        let pause = Arc::new(Barrier::new(2));
        *lock(&session.finder_pause) = Some(Arc::clone(&pause));
        session.find_mat().unwrap();
        // The finder thread has sent the hand and match begin and is paused.
        pause.wait();
        assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
        assert_eq!(non_alive(&mock.received())[before..], round(Hand::Right));

        let released = Arc::new(AtomicBool::new(false));
        let release = {
            let pause = Arc::clone(&pause);
            let released = Arc::clone(&released);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(100));
                released.store(true, Ordering::SeqCst);
                pause.wait();
            })
        };
        session.restart_match(Hand::Left).unwrap();
        // It waited for the paused finder thread rather than returning early.
        assert!(released.load(Ordering::SeqCst));
        release.join().unwrap();
        assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 12));
        assert_eq!(non_alive(&mock.received())[before + 6..], round(Hand::Left));
        session.close().unwrap();
    }
}
