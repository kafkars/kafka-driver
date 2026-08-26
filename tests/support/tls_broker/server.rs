//! Bounded blocking server scripts for TLS lifecycle scenarios.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    sync::mpsc,
    time::Duration,
};

use rustls::{ServerConnection, StreamOwned};

use super::{
    BrokerStep, TerminalScript, TerminalStep, TlsBroker,
    codec::{call_response, negotiation_response, read_frame},
};

impl TlsBroker {
    pub(super) fn serve(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        Self::negotiate(&mut stream, steps, BrokerStep::NegotiationResponded);
        Self::respond_to_call(&mut stream, steps);
    }

    pub(super) fn serve_expecting_no_sni(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        let correlation = read_frame(&mut stream);
        assert_eq!(stream.conn.server_name(), None, "IP identity must omit SNI");
        write_frame(
            &mut stream,
            &negotiation_response(correlation),
            "TLS negotiation response",
        );
        send_step(steps, BrokerStep::NegotiationRespondedWithoutSni);
        Self::respond_to_call(&mut stream, steps);
    }

    pub(super) fn serve_malformed_after_call(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        Self::negotiate(&mut stream, steps, BrokerStep::NegotiationResponded);
        let correlation = read_frame(&mut stream);
        let mut response = call_response(correlation);
        response.extend_from_slice(&(-1_i32).to_be_bytes());
        write_frame(&mut stream, &response, "TLS response and malformed frame");
        send_step(steps, BrokerStep::CallRespondedBeforeMalformedFrame);
    }

    pub(super) fn serve_truncating_after_call(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        Self::negotiate(&mut stream, steps, BrokerStep::NegotiationResponded);
        read_frame(&mut stream);
        send_step(steps, BrokerStep::CallReadBeforeTruncation);
        stream
            .sock
            .shutdown(Shutdown::Both)
            .unwrap_or_else(|error| panic!("truncate TLS broker socket: {error}"));
    }

    pub(super) fn serve_two_calls_before_truncation(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        Self::negotiate(&mut stream, steps, BrokerStep::NegotiationResponded);
        let first = read_frame(&mut stream);
        let second = read_frame(&mut stream);
        let mut responses = call_response(first);
        responses.extend_from_slice(&call_response(second));
        write_frame(&mut stream, &responses, "two TLS call responses");
        send_step(steps, BrokerStep::CallsRespondedBeforeTruncation);
        stream
            .sock
            .shutdown(Shutdown::Both)
            .unwrap_or_else(|error| panic!("truncate TLS broker after two responses: {error}"));
    }

    pub(super) fn serve_terminal_ordering(
        self,
        script: TerminalScript,
        steps: &mpsc::Sender<TerminalStep>,
        released: &mpsc::Receiver<()>,
    ) {
        let mut first = self.accept_stream();
        Self::negotiate_generation(&mut first, 0);
        let correlations = (0..script.generation_one_calls())
            .map(|_| read_frame(&mut first))
            .collect::<Vec<_>>();
        let mut plaintext = correlations
            .iter()
            .take(script.complete_responses())
            .flat_map(|correlation| call_response(*correlation))
            .collect::<Vec<_>>();
        if script == TerminalScript::PartialAfterOne {
            let trailing = call_response(correlations[1]);
            plaintext.extend_from_slice(&trailing[..6]);
        }
        write_frame(&mut first, &plaintext, "TLS terminal response prefix");
        match script {
            TerminalScript::CloseNotifyAfterOne => clean_close(&mut first),
            TerminalScript::TruncateAfterOne
            | TerminalScript::TruncateAfterTwo
            | TerminalScript::PartialAfterOne => truncate(&first),
        }
        drop(first);
        send_terminal_step(steps, TerminalStep::GenerationOneClosed);

        let mut second = self.accept_stream();
        Self::negotiate_generation(&mut second, 0);
        send_terminal_step(steps, TerminalStep::GenerationTwoNegotiated);
        let probe = read_frame(&mut second);
        assert_eq!(
            probe, 1,
            "fresh recovery epoch must restart correlation ownership"
        );
        write_frame(&mut second, &call_response(probe), "TLS recovery probe");
        send_terminal_step(steps, TerminalStep::ProbeResponded);
        released
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("release TLS recovery generation: {error}"));
    }

    pub(super) fn serve_observing_close_notify(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        Self::negotiate(&mut stream, steps, BrokerStep::NegotiationResponded);
        Self::respond_to_call(&mut stream, steps);
        let mut unexpected = [0_u8; 1];
        let read = stream
            .read(&mut unexpected)
            .unwrap_or_else(|error| panic!("read authenticated TLS shutdown: {error}"));
        assert_eq!(
            read, 0,
            "driver sent application bytes after graceful drain"
        );
        send_step(steps, BrokerStep::CloseNotifyObserved);
    }

    pub(super) fn observe_identity_rejection(self, steps: &mpsc::Sender<BrokerStep>) {
        let (mut socket, _) = self
            .listener
            .accept()
            .unwrap_or_else(|error| panic!("accept rejected TLS identity: {error}"));
        bound_socket(&socket);
        let mut session = ServerConnection::new(self.server)
            .unwrap_or_else(|error| panic!("start rejected TLS server session: {error}"));
        loop {
            match session.complete_io(&mut socket) {
                Ok(_) if session.is_handshaking() => {}
                Ok(_) => panic!("incorrect TLS identity completed its handshake"),
                Err(_) => {
                    assert!(
                        session.is_handshaking(),
                        "identity failure must precede Kafka admission"
                    );
                    send_step(steps, BrokerStep::TlsHandshakeRejectedBeforeKafka);
                    return;
                }
            }
        }
    }

    pub(super) fn reject_negotiation(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        read_frame(&mut stream);
        write_frame(
            &mut stream,
            &0_i32.to_be_bytes(),
            "rejected TLS negotiation",
        );
        send_step(steps, BrokerStep::NegotiationRejected);
    }

    fn negotiate(
        stream: &mut StreamOwned<ServerConnection, TcpStream>,
        steps: &mpsc::Sender<BrokerStep>,
        step: BrokerStep,
    ) {
        let correlation = read_frame(stream);
        write_frame(
            stream,
            &negotiation_response(correlation),
            "TLS negotiation response",
        );
        send_step(steps, step);
    }

    fn respond_to_call(
        stream: &mut StreamOwned<ServerConnection, TcpStream>,
        steps: &mpsc::Sender<BrokerStep>,
    ) {
        let correlation = read_frame(stream);
        write_frame(stream, &call_response(correlation), "TLS call response");
        send_step(steps, BrokerStep::CallResponded);
    }

    fn negotiate_generation(stream: &mut StreamOwned<ServerConnection, TcpStream>, expected: i32) {
        let correlation = read_frame(stream);
        assert_eq!(
            correlation, expected,
            "fresh TLS epoch must restart negotiation"
        );
        write_frame(
            stream,
            &negotiation_response(correlation),
            "TLS generation negotiation response",
        );
    }

    fn accept_stream(&self) -> StreamOwned<ServerConnection, TcpStream> {
        let (socket, _) = self
            .listener
            .accept()
            .unwrap_or_else(|error| panic!("accept TLS driver connection: {error}"));
        bound_socket(&socket);
        let session = ServerConnection::new(self.server.clone())
            .unwrap_or_else(|error| panic!("start TLS server session: {error}"));
        StreamOwned::new(session, socket)
    }
}

fn bound_socket(socket: &TcpStream) {
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound TLS broker read: {error}"));
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound TLS broker write: {error}"));
}

fn write_frame(stream: &mut impl Write, frame: &[u8], label: &str) {
    stream
        .write_all(frame)
        .unwrap_or_else(|error| panic!("write {label}: {error}"));
    stream
        .flush()
        .unwrap_or_else(|error| panic!("flush {label}: {error}"));
}

fn send_step(steps: &mpsc::Sender<BrokerStep>, step: BrokerStep) {
    steps
        .send(step)
        .unwrap_or_else(|error| panic!("report TLS broker step {step:?}: {error}"));
}

fn send_terminal_step(steps: &mpsc::Sender<TerminalStep>, step: TerminalStep) {
    steps
        .send(step)
        .unwrap_or_else(|error| panic!("report TLS terminal step {step:?}: {error}"));
}

fn clean_close(stream: &mut StreamOwned<ServerConnection, TcpStream>) {
    stream.conn.send_close_notify();
    while stream.conn.wants_write() {
        let written = stream
            .conn
            .write_tls(&mut stream.sock)
            .unwrap_or_else(|error| panic!("write TLS close-notify: {error}"));
        assert_ne!(written, 0, "TLS close-notify write must progress");
    }
    stream
        .sock
        .flush()
        .unwrap_or_else(|error| panic!("flush TLS close-notify: {error}"));
    stream
        .sock
        .shutdown(Shutdown::Write)
        .unwrap_or_else(|error| panic!("close TLS broker write side: {error}"));
}

fn truncate(stream: &StreamOwned<ServerConnection, TcpStream>) {
    stream
        .sock
        .shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("truncate TLS broker socket: {error}"));
}
