<p align="center">
  <img src="./kafka-driver-logo.svg" alt="kafka-driver" width="720">
</p>

<p align="center"><strong>Runtime-neutral Kafka broker and cluster RPC for Rust.</strong></p>
<p align="center">Deterministic policy. Bounded resources. Blocking and <code>Future</code> APIs over one engine.</p>

<p align="center">
  <a href="#model">Model</a> ·
  <a href="#use">Use</a> ·
  <a href="#routes">Routes</a> ·
  <a href="#crates">Crates</a> ·
  <a href="#proof">Proof</a> ·
  <a href="#status">Status</a>
</p>

<br />

`kafka-driver` owns the work between generated [`kafka-wire`](https://github.com/kafkars/kafka-wire)
messages and Kafka brokers: DNS, connect, TLS and SASL, API negotiation, FIFO
correlation, metadata and coordinator routing, deadlines, reconnects, and
semantic connection lanes. It has no async-runtime dependency and is not a
high-level producer or consumer client.

## Model

Policy and I/O have one explicit boundary:

```text
application       generated requests, routes, deadlines, Call<Response>
      │
kafka-driver      bounded admission, completion, embedded or dedicated host
      ├── core     State + Input -> Transition<State, Effect>
      ├── transport
      │            sans-I/O frame decoding and ordered write progress
      └── reactor  sockets, DNS, TLS, SASL, timers, and effect interpretation
      │
Kafka brokers
```

Machines own Kafka policy and receive driver-relative time. The reactor reports
external outcomes as events. One connection is FIFO: a response must match the
pending queue front, and a mismatch closes that connection epoch. Machine state
has one owner and does not use locks.

Every mailbox, wait queue, frame, write buffer, in-flight request, timer, and
reactor turn is bounded. A `Call<T>` can block with `wait()` or be awaited as a
`Future`; both observe the same runtime-neutral completion cell.

## Use

Start a dedicated driver host and issue a generated `ApiVersions` request:

```rust
use std::{net::SocketAddr, time::Duration};

use kafka_driver::Driver;
use kafka_wire::ApiVersionsRequest;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker: SocketAddr = "127.0.0.1:9092".parse()?;
    let (driver, host) = Driver::builder()
        .broker(broker)
        .client_id("example")
        .spawn()?;

    let _response = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))?
        .wait()??;

    driver.shutdown()?.wait()?;
    host.join()?;
    Ok(())
}
```

`spawn()` owns one dedicated reactor thread. `build_reactor()` returns the same
driver with a caller-driven `Reactor` for embedding in another event loop. The
embedded reactor is thread-affine: construct and drive it on its owner thread. No
public executor abstraction is required.

## Routes

Requests name Kafka semantics instead of sockets:

| Route | Destination |
| --- | --- |
| `AnyBroker` | The available seed connection. |
| `Controller` | The controller from current metadata. |
| `Broker` | One exact broker ID. |
| `Coordinator` | The coordinator for a group, transaction, or share key. |
| `PartitionLeader` | The current leader for one topic partition. |

Control, interactive, bulk, and long-poll traffic use separate semantic lanes
so a long-held Fetch does not head-of-line-block control work. Timeouts cover
the complete path from mailbox admission through routing, connection work,
write progress, and response completion.

## Crates

| Crate | Purpose |
| --- | --- |
| `kafka-driver` | Public request API, cluster routing, completion, reactor hosting, TLS, and SASL. |
| `kafka-driver-core` | Deterministic connection, broker, metadata, coordinator, authentication, and bootstrap machines. |
| `kafka-driver-transport` | Sans-I/O frame decoding and bounded FIFO write progress. |

`kafka-wire` remains the protocol authority for API keys, versions, messages,
headers, and codecs. The driver does not duplicate that vocabulary.

## Proof

The canonical repository gate runs formatting, Clippy with and without default
features, the complete workspace test suite, rustdoc with warnings denied, and
immutable-diff checks:

```sh
scripts/check
```

The test surface includes pure machine scenarios with virtual time,
fixed-seed frame fuzzing, loopback transport and TLS integration, bounded
concurrency and shutdown tests, and separate real-broker qualification against
Kafka 4.3.1. CI runs the same canonical gate on Linux, macOS, and Windows.

Local development expects the public wire repository at the pinned sibling
path:

```sh
git clone https://github.com/kafkars/kafka-wire ../kafka-protocol
scripts/check
```

## Status

`kafka-driver` 0.1.0-rc.2 is the second release candidate. Its protocol authority
is `kafka-wire` 0.1.0-rc.2. Public APIs may still change before 0.1.0.

## License

Apache-2.0. Apache Kafka is a trademark of the Apache Software Foundation. This
project is independent and is not endorsed by the Apache Software Foundation.

See [`LICENSE`](LICENSE).
