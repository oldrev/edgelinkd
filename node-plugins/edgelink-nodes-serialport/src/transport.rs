//! One async owner per open serial port. Dropping the task closes the OS handle.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use tokio_serial::{ClearBuffer, SerialPort, SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;

use crate::{framing::Framer, settings::Settings};

pub enum Command {
    Write(Vec<u8>, Option<String>, oneshot::Sender<io::Result<()>>),
    Reconfigure(Settings, oneshot::Sender<io::Result<()>>),
    Stop(oneshot::Sender<()>),
    Start(oneshot::Sender<()>),
    Baud(u32, oneshot::Sender<io::Result<()>>),
    Purge(oneshot::Sender<io::Result<()>>),
    ClearRequest(String, oneshot::Sender<()>),
    SetRequestCount(Option<usize>, oneshot::Sender<io::Result<()>>),
    DrainPartial(oneshot::Sender<Option<Vec<u8>>>),
}

#[derive(Clone, Debug)]
pub enum Event {
    Connected,
    Frame(Vec<u8>, Option<String>),
    Disconnected(String),
}

fn request_split(
    default: crate::framing::Split,
    override_count: Option<usize>,
) -> Result<crate::framing::Split, io::Error> {
    let split = override_count.map(crate::framing::Split::Count).unwrap_or(default);
    Framer::new(split, None).map(|_| split).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

fn open(settings: &Settings) -> io::Result<SerialStream> {
    let mut port = tokio_serial::new(&settings.path, settings.baud)
        .data_bits(settings.data_bits)
        .parity(settings.parity)
        .stop_bits(settings.stop_bits)
        .open_native_async()?;
    if let Some(value) = settings.dtr {
        port.write_data_terminal_ready(value)?;
    }
    if let Some(value) = settings.rts {
        port.write_request_to_send(value)?;
    }
    Ok(port)
}

async fn publish(events: &mpsc::Sender<Event>, event: Event, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = events.send(event) => result.is_ok(),
    }
}

async fn write_bytes<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    bytes: &[u8],
    cancel: &CancellationToken,
) -> io::Result<()> {
    tokio::select! {
        _ = cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "serial write cancelled")),
        result = stream.write_all(bytes) => result,
    }
}

async fn read_frames<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    buffer: &mut [u8],
    framer: &mut Framer,
    sender: Option<String>,
    events: &mpsc::Sender<Event>,
    cancel: &CancellationToken,
) -> io::Result<usize> {
    let count = tokio::select! {
        _ = cancel.cancelled() => return Err(io::Error::new(io::ErrorKind::Interrupted, "serial read cancelled")),
        result = stream.read(buffer) => result?,
    };
    if count == 0 {
        return Ok(0);
    }
    for frame in framer.push(&buffer[..count], std::time::Instant::now()) {
        if !publish(events, Event::Frame(frame, sender.clone()), cancel).await {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "serial frame delivery cancelled"));
        }
    }
    Ok(count)
}

/// Run until cancelled or all command senders disappear. Failed devices retry.
pub async fn run(
    mut settings: Settings,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    reconnect: Duration,
    cancel: CancellationToken,
) {
    let mut enabled = true;
    loop {
        if cancel.is_cancelled() || commands.is_closed() {
            return;
        }
        if !enabled {
            tokio::select! {
                _ = cancel.cancelled() => return,
                command = commands.recv() => match command {
                    Some(Command::Start(reply)) => { enabled = true; let _ = reply.send(()); }
                    Some(Command::ClearRequest(_, reply)) => { let _ = reply.send(()); }
                    Some(Command::SetRequestCount(_, reply)) => { let _ = reply.send(Ok(())); }
                    Some(Command::DrainPartial(reply)) => { let _ = reply.send(None); }
                    Some(Command::Stop(reply)) => { let _ = reply.send(()); }
                    Some(Command::Reconfigure(next, reply)) => { settings = next; let _ = reply.send(Ok(())); }
                    Some(Command::Write(_, _, reply) | Command::Baud(_, reply) | Command::Purge(reply)) => {
                        let _ = reply.send(Err(io::Error::new(io::ErrorKind::NotConnected, "serial port is stopped")));
                    }
                    None => return,
                }
            }
            continue;
        }
        let opened = open(&settings);
        let mut port = match opened {
            Ok(port) => port,
            Err(error) => {
                if !publish(&events, Event::Disconnected(error.to_string()), &cancel).await {
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(reconnect) => {},
                    command = commands.recv() => match command {
                        Some(Command::Reconfigure(next, reply)) => { settings = next; let _ = reply.send(Ok(())); }
                        Some(Command::Stop(reply)) => { enabled = false; let _ = reply.send(()); }
                        Some(Command::Start(reply)) => { let _ = reply.send(()); }
                        Some(Command::Write(_, _, reply) | Command::Baud(_, reply) | Command::Purge(reply)) => {
                            let _ = reply.send(Err(io::Error::new(io::ErrorKind::NotConnected, "serial port is disconnected")));
                        }
                        Some(Command::ClearRequest(_, reply)) => { let _ = reply.send(()); }
                        Some(Command::DrainPartial(reply)) => { let _ = reply.send(None); }
                        Some(Command::SetRequestCount(_, reply)) => { let _ = reply.send(Err(io::Error::new(io::ErrorKind::NotConnected, "serial port is disconnected"))); }
                        None => return,
                    }
                }
                continue;
            }
        };
        if !publish(&events, Event::Connected, &cancel).await {
            return;
        }
        let mut framer = match Framer::new(settings.split, settings.wait_for) {
            Ok(framer) => framer,
            Err(error) => {
                publish(&events, Event::Disconnected(error.into()), &cancel).await;
                return;
            }
        };
        let mut active_split = settings.split;
        let mut bytes = [0; 4096];
        let mut pending_sender = None;
        let failure = loop {
            let deadline = framer.deadline().map(tokio::time::Instant::from_std);
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if let Some(frame) = framer.expire(std::time::Instant::now())
                        && !publish(&events, Event::Frame(frame, pending_sender.clone()), &cancel).await {
                        return;
                    } else if framer.deadline().is_none() {
                        pending_sender = None;
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    match command {
                        Command::Write(data, sender, reply) => {
                            let result = write_bytes(&mut port, &data, &cancel).await;
                            if result.as_ref().is_err_and(|error| error.kind() == io::ErrorKind::Interrupted) { return; }
                            let error = result.as_ref().err().map(ToString::to_string);
                            let _ = reply.send(result);
                            if sender.is_some() {
                                pending_sender = sender;
                            }
                            if let Some(error) = error { break error; }
                        }
                        Command::Reconfigure(next, reply) => {
                            if next.path != settings.path {
                                settings = next;
                                let _ = reply.send(Ok(()));
                                break "reconfigured".to_owned();
                            }
                            let result = reconfigure(&mut port, &next);
                            if result.is_ok() { settings = next; }
                            let _ = reply.send(result);
                        }
                        Command::Stop(reply) => { enabled = false; let _ = reply.send(()); break "serial port stopped".to_owned(); }
                        Command::Start(reply) => { let _ = reply.send(()); }
                        Command::Baud(baud, reply) => {
                            let result = port.set_baud_rate(baud).map_err(io::Error::from);
                            if result.is_ok() { settings.baud = baud; }
                            let _ = reply.send(result);
                        }
                        Command::Purge(reply) => {
                            let _ = reply.send(port.clear(ClearBuffer::All).map_err(io::Error::from));
                        }
                        Command::ClearRequest(sender, reply) => {
                            if pending_sender.as_deref() == Some(sender.as_str()) { pending_sender = None; }
                            let _ = reply.send(());
                        }
                        Command::SetRequestCount(count, reply) => {
                            let result = request_split(settings.split, count).and_then(|split| {
                                Framer::new(split, settings.wait_for)
                                    .map(|next| { active_split = split; framer = next; })
                                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
                            });
                            let _ = reply.send(result);
                        }
                        Command::DrainPartial(reply) => { let _ = reply.send(framer.take_partial()); }
                    }
                }
                read = read_frames(&mut port, &mut bytes, &mut framer, pending_sender.clone(), &events, &cancel) => match read {
                    Ok(0) => break "serial device closed".to_owned(),
                    Ok(_) => pending_sender = None,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => return,
                    Err(error) => break error.to_string(),
                }
            }
        };
        drop(port);
        if !publish(&events, Event::Disconnected(failure.clone()), &cancel).await {
            return;
        }
        if failure != "reconfigured" && failure != "serial port stopped" {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(reconnect) => {},
            }
        }
    }
}

fn reconfigure(port: &mut SerialStream, settings: &Settings) -> io::Result<()> {
    use tokio_serial::SerialPort;
    port.set_baud_rate(settings.baud)?;
    port.set_data_bits(settings.data_bits)?;
    port.set_parity(settings.parity)?;
    port.set_stop_bits(settings.stop_bits)?;
    if let Some(value) = settings.dtr {
        port.write_data_terminal_ready(value)?;
    }
    if let Some(value) = settings.rts {
        port.write_request_to_send(value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn duplex_stream_exercises_request_write_and_framed_response_delivery() {
        let (mut worker, mut device) = tokio::io::duplex(64);
        let cancel = CancellationToken::new();
        write_bytes(&mut worker, b"PING\n", &cancel).await.unwrap();
        let mut request = [0; 5];
        device.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"PING\n");

        device.write_all(b"OK\n").await.unwrap();
        let mut framer = Framer::new(crate::framing::Split::Character(b'\n'), None).unwrap();
        let (events, mut received) = mpsc::channel(2);
        let mut buffer = [0; 32];
        read_frames(&mut worker, &mut buffer, &mut framer, Some("request-1".into()), &events, &cancel).await.unwrap();
        assert!(
            matches!(received.recv().await, Some(Event::Frame(bytes, Some(sender))) if bytes == b"OK\n" && sender == "request-1")
        );
    }

    #[tokio::test]
    async fn ordinary_write_preserves_pending_request_response_owner() {
        let (mut worker, mut device) = tokio::io::duplex(64);
        let cancel = CancellationToken::new();
        let pending_sender = Some("expired-request".to_owned());
        let mut framer = Framer::new(crate::framing::Split::Character(b'\n'), None).unwrap();
        let (events, mut received) = mpsc::channel(2);
        let mut buffer = [0; 16];
        device.write_all(b"old\n").await.unwrap();
        read_frames(&mut worker, &mut buffer, &mut framer, pending_sender.clone(), &events, &cancel).await.unwrap();
        assert!(matches!(received.recv().await, Some(Event::Frame(_, Some(sender))) if sender == "expired-request"));

        write_bytes(&mut worker, b"ordinary\n", &cancel).await.unwrap();
        let mut written = [0; 9];
        device.read_exact(&mut written).await.unwrap();
        assert_eq!(pending_sender.as_deref(), Some("expired-request"));

        device.write_all(b"reply\n").await.unwrap();
        read_frames(&mut worker, &mut buffer, &mut framer, pending_sender, &events, &cancel).await.unwrap();
        assert!(
            matches!(received.recv().await, Some(Event::Frame(bytes, Some(sender))) if bytes == b"reply\n" && sender == "expired-request")
        );
    }

    #[tokio::test]
    async fn timed_frame_keeps_request_sender_until_interbyte_expiry() {
        let (mut worker, mut device) = tokio::io::duplex(64);
        let cancel = CancellationToken::new();
        let mut framer = Framer::new(crate::framing::Split::Interbyte(Duration::from_millis(5)), None).unwrap();
        let (events, mut received) = mpsc::channel(1);
        let mut buffer = [0; 16];
        device.write_all(b"OK").await.unwrap();
        read_frames(&mut worker, &mut buffer, &mut framer, Some("request-2".into()), &events, &cancel).await.unwrap();
        tokio::time::sleep(Duration::from_millis(6)).await;
        let frame = framer.expire(std::time::Instant::now()).unwrap();
        assert!(publish(&events, Event::Frame(frame, Some("request-2".into())), &cancel).await);
        assert!(
            matches!(received.recv().await, Some(Event::Frame(bytes, Some(sender))) if bytes == b"OK" && sender == "request-2")
        );
    }

    #[tokio::test]
    async fn request_count_override_frames_exact_length_and_restores_default() {
        let mut settings =
            Settings::parse(&serde_json::json!({"serialport":"COM1", "out":"count", "newline":"4"})).unwrap();
        let override_count = 2usize;
        let default_split = settings.split;
        let override_split = request_split(default_split, Some(override_count)).unwrap();
        let restored_split = request_split(default_split, None).unwrap();
        assert!(matches!(override_split, crate::framing::Split::Count(2)));
        assert!(matches!(restored_split, crate::framing::Split::Count(4)));
        let (mut device, mut worker) = tokio::io::duplex(64);
        worker.write_all(b"ab").await.unwrap();
        let mut request = [0; 2];
        device.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"ab");
        let mut framer = Framer::new(override_split, None).unwrap();
        let (events, mut received) = mpsc::channel(1);
        let mut bytes = [0; 8];
        device.write_all(b"xy").await.unwrap();
        read_frames(&mut worker, &mut bytes, &mut framer, Some("req".into()), &events, &CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(received.recv().await, Some(Event::Frame(frame, Some(sender))) if frame == b"xy" && sender == "req")
        );
        settings.split = restored_split;
    }

    #[tokio::test]
    async fn unavailable_port_reports_error_and_cancellation_interrupts_retry() {
        let settings =
            Settings::parse(&serde_json::json!({"serialport":"__edgelink_nonexistent_serial_device__"})).unwrap();
        let (commands, receiver) = mpsc::channel(1);
        let (events, mut output) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(settings, receiver, events, Duration::from_secs(60), cancel.clone()));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), output.recv()).await.unwrap(),
            Some(Event::Disconnected(_))
        ));
        let (reply, response) = oneshot::channel();
        commands.send(Command::Write(vec![1], None, reply)).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap_err().kind(), io::ErrorKind::NotConnected);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task).await.unwrap().unwrap();
    }
}
