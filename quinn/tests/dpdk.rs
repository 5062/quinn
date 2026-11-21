//! A QUIC echo between two endpoints on a pair of DPDK ring ports, so the socket
//! runs through real DPDK queues without a NIC, hugepages, or root.
#![cfg(all(quinn_dpdk, feature = "rustls-ring"))]

use std::{net::Ipv4Addr, sync::Arc};

use quinn::{ClientConfig, Endpoint, EndpointConfig, ServerConfig, TokioRuntime, dpdk};
use rustls::pki_types::PrivatePkcs8KeyDer;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_over_ring_pair() {
    dpdk::init(&[
        "quinn-test",
        "--no-huge",
        "-m",
        "256",
        "--no-pci",
        "--in-memory",
        "--no-telemetry",
        "-l",
        "0",
        "--log-level",
        "*:warning",
    ])
    .unwrap();
    let (a, b) = dpdk::Device::ring_pair().unwrap();
    let server_ip = Ipv4Addr::new(10, 0, 0, 1);
    // The server learns the client's MAC from its first packet; only the client
    // needs a gateway, and broadcast reaches the other end of the pair.
    let server_device = dpdk::Device::start(a, server_ip, None).unwrap();
    let client_device =
        dpdk::Device::start(b, Ipv4Addr::new(10, 0, 0, 2), Some([0xff; 6])).unwrap();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key.into()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client_config = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();

    let server = Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        Some(server_config),
        server_device.bind(4433).unwrap(),
        Arc::new(TokioRuntime),
    )
    .unwrap();
    let mut client = Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        None,
        client_device.bind(5000).unwrap(),
        Arc::new(TokioRuntime),
    )
    .unwrap();
    client.set_default_client_config(client_config);

    let echo = tokio::spawn(async move {
        let conn = server.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let data = recv.read_to_end(usize::MAX).await.unwrap();
        send.write_all(&data).await.unwrap();
        send.finish().unwrap();
        conn.closed().await;
    });

    // Large enough to need many segmented transmits and the congestion window.
    let data: Vec<u8> = (0..4 << 20).map(|i: u32| i as u8).collect();
    let conn = client
        .connect((server_ip, 4433).into(), "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(&data).await.unwrap();
    send.finish().unwrap();
    let echoed = recv.read_to_end(usize::MAX).await.unwrap();
    assert!(echoed == data, "echo differs from what was sent");
    conn.close(0u32.into(), b"done");
    echo.await.unwrap();
}
