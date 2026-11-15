//! The client: one connection to the device, stepped by its caller.

use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::event::Event;
use crate::packet::{Hand, Request};
use crate::zmtp::{self, FrameReader};

/// How long the device has to send its greeting, and to answer each
/// handshake request.
pub(crate) const REPLY_TIMEOUT: Duration = Duration::from_secs(3);

/// Longest wait for the device to report the mat found in a mat search.
/// Measuring then starts anyway.
pub(crate) const MAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Keepalive period.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2);

/// Which way a message crossed the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// From the device.
    Rx,
    /// To the device.
    Tx,
}

/// Where the client is in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// The client's greeting is out; waiting for the device's.
    Greeting,
    /// This handshake request is sent; waiting for the device's `N_OK`.
    Handshake(Request),
    /// The handshake is done; the session's mat search starts at the next
    /// poll.
    Joined,
    /// A mat search: the camera is on and `resyncToMat` is sent; waiting
    /// for the mat.
    FindingMat,
    /// Measuring putts.
    Measuring,
    /// The goodbye is sent.
    Closed,
}

impl State {
    /// How long the device has to answer in this state, if it is a wait.
    fn timeout(self) -> Option<Duration> {
        match self {
            Self::Greeting | Self::Handshake(_) => Some(REPLY_TIMEOUT),
            Self::FindingMat => Some(MAT_TIMEOUT),
            Self::Joined | Self::Measuring | Self::Closed => None,
        }
    }
}

/// A session with the device over a non-blocking stream, stepped by its
/// caller: nothing happens between two calls to
/// [`poll_at`](Self::poll_at), and every timer reads the `now` it is given.
/// No threads, no clock of its own.
///
/// The stream is the device's TCP port ([`DEVICE_PORT`](crate::DEVICE_PORT)),
/// connected and set non-blocking, or any `Read + Write` whose reads return
/// `WouldBlock` when nothing has arrived. The polls run the whole session:
///
/// 1. The ZMTP greeting, then `N_VERIFY_ID` and `N_REQ_CONNECT`, each
///    answered by the device's `N_OK` (3 s each, else
///    [`Error::Timeout`]).
/// 2. A keepalive every 2 s from then on.
/// 3. The mat search: `sendImage`, then `resyncToMat`, which makes the
///    device locate the mat and stream camera frames; once the device
///    reports the mat found (at most 5 s), `stopSendImage`, the putting
///    hand, and match begin. The device then measures putts.
///
/// [`find_mat`](Self::find_mat) runs step 3 again on a measuring session
/// (after a match end), and so does [`restart_match`](Self::restart_match)
/// with a new hand. [`close`](Self::close) says goodbye.
///
/// Every device message, from the handshake replies on, comes out of a poll
/// as an [`Event`], including each [`PuttResult`](crate::PuttResult). Camera
/// frames are dropped unread unless [`set_camera`](Self::set_camera) is on.
/// The [crate docs](crate) have an example.
pub struct Client<S> {
    frames: FrameReader<S>,
    /// Bytes not yet written: the stream would have blocked.
    outbox: Vec<u8>,
    state: State,
    /// When the wait of the current state gives up; set by the poll that
    /// enters the state, or by the next poll.
    deadline: Option<Instant>,
    /// When the next keepalive is due, once the handshake is done.
    next_alive: Option<Instant>,
    hand: Hand,
    camera: bool,
    /// Every message body that crossed the stream since the last take,
    /// while recording.
    wire: Option<Vec<(Direction, Vec<u8>)>>,
}

impl<S> std::fmt::Debug for Client<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state)
            .field("hand", &self.hand)
            .field("camera", &self.camera)
            .finish_non_exhaustive()
    }
}

impl<S: Read + Write> Client<S> {
    /// A session over `stream` with the putting hand `hand`. Sends nothing
    /// until the first poll, which sends the greeting.
    pub fn new(stream: S, hand: Hand) -> Self {
        Self {
            frames: FrameReader::new(stream),
            outbox: zmtp::client_greeting(),
            state: State::Greeting,
            deadline: None,
            next_alive: None,
            hand,
            camera: false,
            wire: None,
        }
    }

    /// Deliver the camera frames the device streams during a mat search as
    /// [`Event::CameraFrame`]. Off (the default), they are dropped without
    /// being decoded. The mat search turns the camera on and off either way.
    pub fn set_camera(&mut self, on: bool) {
        self.camera = on;
    }

    /// [`poll_at`](Self::poll_at) the current time.
    ///
    /// # Errors
    ///
    /// As [`poll_at`](Self::poll_at).
    pub fn poll(&mut self) -> Result<Option<Event>> {
        self.poll_at(Instant::now())
    }

    /// Send what is due at `now` and return the next device event, or
    /// `None` when nothing has arrived. Pass a `now` that never goes
    /// backward.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] when the device does not greet or answer a
    /// handshake request in time, [`Error::Closed`] when it closes the
    /// connection, [`Error::Greeting`], [`Error::FrameTooLarge`] and socket
    /// errors. Each ends the session.
    pub fn poll_at(&mut self, now: Instant) -> Result<Option<Event>> {
        self.flush()?;
        if self.state == State::Joined {
            self.send(Request::StartCamera)?;
            self.send(Request::ResyncToMat)?;
            self.enter(State::FindingMat, Some(now));
        }
        if let Some(timeout) = self.state.timeout() {
            let deadline = *self.deadline.get_or_insert(now + timeout);
            if now >= deadline {
                match self.state {
                    State::FindingMat => self.mat_search_over()?,
                    State::Greeting => return Err(Error::Timeout("the device's greeting")),
                    State::Handshake(Request::VerifyId) => {
                        return Err(Error::Timeout("the device's handshake reply"));
                    }
                    _ => return Err(Error::Timeout("the device's connection reply")),
                }
            }
        }
        if let Some(due) = self.next_alive
            && now >= due
        {
            self.send(Request::Alive)?;
            self.next_alive = Some(now + KEEPALIVE_INTERVAL);
        }

        if self.state == State::Greeting {
            match self.frames.read_greeting() {
                Ok(_) => {
                    self.send(Request::VerifyId)?;
                    self.enter(State::Handshake(Request::VerifyId), Some(now));
                }
                Err(e) => return nothing_yet(e),
            }
        }
        loop {
            let body = match self.frames.read_message() {
                Ok(body) => body,
                Err(e) => return nothing_yet(e),
            };
            self.see(Direction::Rx, &body);
            let Some(event) = Event::parse_filtered(&body, self.camera) else {
                continue;
            };
            match (self.state, &event) {
                (State::Handshake(asked), Event::Ack { request, .. })
                    if request == asked.packet_id() =>
                {
                    if asked == Request::VerifyId {
                        self.send(Request::Connect)?;
                        self.enter(State::Handshake(Request::Connect), Some(now));
                    } else {
                        self.enter(State::Joined, None);
                        self.next_alive = Some(now + KEEPALIVE_INTERVAL);
                    }
                }
                (State::FindingMat, Event::MatFound) => self.mat_search_over()?,
                _ => {}
            }
            return Ok(Some(event));
        }
    }

    /// Send a packet now.
    ///
    /// # Errors
    ///
    /// Socket errors.
    fn send(&mut self, request: Request) -> Result<()> {
        let body = request.to_json();
        self.outbox
            .extend_from_slice(&zmtp::encode_frame(body.as_bytes()));
        self.see(Direction::Tx, body.as_bytes());
        self.flush()
    }

    /// Locate the mat again, for example when the device locked onto the
    /// wrong spot because someone stood in its view as the session started.
    ///
    /// The device finds the mat only between rounds, so this ends the round
    /// (match end) and runs the mat search again, which ends with the hand
    /// last set and match begin. Does nothing while a mat search is under
    /// way or yet to come (before the session's own has run). The wait for
    /// the mat counts from the next poll.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn find_mat(&mut self) -> Result<()> {
        if self.state != State::Measuring {
            return Ok(());
        }
        self.send(Request::MatchEnded)?;
        self.send(Request::StartCamera)?;
        self.send(Request::ResyncToMat)?;
        self.enter(State::FindingMat, None);
        Ok(())
    }

    /// End the current round and start a new one with `hand`, for example
    /// after the player switches hands. A new round always locates the mat
    /// again: this is [`find_mat`](Self::find_mat) with `hand`, which later
    /// rounds keep. While a mat search is under way or yet to come, it
    /// ends with `hand` and nothing is sent.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn restart_match(&mut self, hand: Hand) -> Result<()> {
        self.hand = hand;
        self.find_mat()
    }

    /// Say goodbye: stop measuring (`stopSendImage` first during a mat
    /// search), then `N_APP_PAUSE`. Nothing is sent before the handshake is
    /// done. Polls go on delivering what the device still sends, without
    /// timers or keepalives. Shut down the sending direction of a TCP
    /// stream after this, and read until the device closes its end, so the
    /// connection ends rather than resets.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn close(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.state, State::Closed);
        self.deadline = None;
        self.next_alive = None;
        match state {
            State::Greeting | State::Handshake(_) | State::Closed => Ok(()),
            State::Joined | State::FindingMat | State::Measuring => {
                if state == State::FindingMat {
                    self.send(Request::StopCamera)?;
                }
                self.send(Request::MatchEnded)?;
                self.send(Request::AppPause)
            }
        }
    }

    /// The stream.
    pub fn get_ref(&self) -> &S {
        self.frames.get_ref()
    }

    /// Record every message body that crosses the stream from now on, or
    /// stop recording.
    #[cfg(feature = "driver")]
    pub(crate) fn record(&mut self, on: bool) {
        match (on, &self.wire) {
            (true, None) => self.wire = Some(Vec::new()),
            (false, Some(_)) => self.wire = None,
            _ => {}
        }
    }

    /// The bodies recorded since the last take, in order.
    #[cfg(feature = "driver")]
    pub(crate) fn take_wire(&mut self) -> Vec<(Direction, Vec<u8>)> {
        self.wire.as_mut().map(std::mem::take).unwrap_or_default()
    }

    fn see(&mut self, dir: Direction, body: &[u8]) {
        if let Some(wire) = &mut self.wire {
            wire.push((dir, body.to_vec()));
        }
    }

    /// Enter `state`, its wait counted from `now` (or from the next poll).
    fn enter(&mut self, state: State, now: Option<Instant>) {
        self.state = state;
        self.deadline = now.zip(state.timeout()).map(|(now, t)| now + t);
    }

    /// The mat was found, or the wait for it is over: camera off, the hand,
    /// match begin.
    fn mat_search_over(&mut self) -> Result<()> {
        self.send(Request::StopCamera)?;
        self.send(Request::SetHand(self.hand))?;
        self.send(Request::MatchBegin)?;
        self.enter(State::Measuring, None);
        Ok(())
    }

    /// Write what the stream takes of the outbox.
    fn flush(&mut self) -> Result<()> {
        while !self.outbox.is_empty() {
            match self.frames.get_mut().write(&self.outbox) {
                Ok(0) => return Err(Error::Io(io::ErrorKind::WriteZero.into())),
                Ok(n) => {
                    self.outbox.drain(..n);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(Error::Io(e)),
            }
        }
        match self.frames.get_mut().flush() {
            Err(e) if e.kind() != io::ErrorKind::WouldBlock => Err(Error::Io(e)),
            _ => Ok(()),
        }
    }
}

/// A read that found nothing waiting is no error.
fn nothing_yet(e: Error) -> Result<Option<Event>> {
    match e {
        Error::Io(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            Ok(None)
        }
        e => Err(e),
    }
}

/// The longest [`hang_up`] reads while waiting for the device to close its
/// end.
#[cfg(feature = "driver")]
const CLOSE_WAIT: Duration = Duration::from_millis(500);

/// The half-close after [`Client::close`] on TCP: shut down the sending
/// direction, then read and drop what the device still sends until it
/// closes its end, for at most [`CLOSE_WAIT`]. Data left unread when the
/// socket closes would make it reset the connection rather than end it.
#[cfg(feature = "driver")]
pub(crate) fn hang_up(stream: &std::net::TcpStream) {
    if stream.shutdown(std::net::Shutdown::Write).is_err() || stream.set_nonblocking(false).is_err()
    {
        return;
    }
    let give_up = Instant::now() + CLOSE_WAIT;
    let mut buf = [0u8; 16 * 1024];
    while let Some(left) = give_up
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
    {
        if stream.set_read_timeout(Some(left)).is_err() {
            return;
        }
        match (&*stream).read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests;
