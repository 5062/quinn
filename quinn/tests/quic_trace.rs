//! Transfers that block a write on each limit, for the quic_trace capture check
//!
//! `moq_trace/test_trace.sh` runs this test under LTTng and requires a per-stream
//! `quic_send_blocked` interval for stream flow control, connection flow control,
//! and the send buffer. Without tracing it checks only that each transfer completes.
#![cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]

use std::{net::Ipv4Addr, sync::Arc};

use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

/// Bytes each transfer sends, many times the limit it runs against
const DATA_LEN: usize = 64 * 1024;
/// The window each transfer shrinks, so the sender's writes block repeatedly
const LIMIT: u32 = 4 * 1024;

#[tokio::test]
async fn writes_block_on_every_limit() {
    let mut stream_window = TransportConfig::default();
    stream_window.stream_receive_window(VarInt::from_u32(LIMIT));
    transfer(stream_window, TransportConfig::default()).await;

    let mut connection_window = TransportConfig::default();
    connection_window.receive_window(VarInt::from_u32(LIMIT));
    transfer(connection_window, TransportConfig::default()).await;

    let mut send_window = TransportConfig::default();
    send_window.send_window(LIMIT.into());
    transfer(TransportConfig::default(), send_window).await;
}

/// Send `DATA_LEN` bytes on one stream from a client to a server with these transports
async fn transfer(server_transport: TransportConfig, client_transport: TransportConfig) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = CertificateDer::from(certified.cert);
    let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    let mut server_config =
        ServerConfig::with_single_cert(vec![certificate.clone()], key.into()).unwrap();
    server_config.transport_config(Arc::new(server_transport));
    let server = Endpoint::server(server_config, (Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let address = server.local_addr().unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut client_config = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    client_config.transport_config(Arc::new(client_transport));
    let client = Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).unwrap();

    let receive = async {
        let connection = server.accept().await.unwrap().await.unwrap();
        let mut stream = connection.accept_uni().await.unwrap();
        let data = stream.read_to_end(DATA_LEN).await.unwrap();
        assert_eq!(data.len(), DATA_LEN);
        connection
    };
    let send = async {
        let connection = client
            .connect_with(client_config, address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let mut stream = connection.open_uni().await.unwrap();
        stream.write_all(&vec![7; DATA_LEN]).await.unwrap();
        stream.finish().unwrap();
        // Every byte is acknowledged, so no packet waits for a send at close.
        stream.stopped().await.unwrap();
        connection
    };
    let (server_connection, client_connection) = tokio::join!(receive, send);
    client_connection.close(0u32.into(), b"done");
    server_connection.closed().await;
    client.wait_idle().await;
    server.wait_idle().await;
}
