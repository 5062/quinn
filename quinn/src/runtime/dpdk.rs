//! A Quinn socket over a DPDK port, so an endpoint sends and receives without the
//! kernel's UDP stack.
//!
//! Ethernet, IPv4, and UDP headers are built and parsed here. The socket answers
//! ARP requests for its address and learns each peer's MAC address from the
//! frames it receives, so a server needs no ARP table; a socket that sends first
//! needs a gateway MAC. The port should be a NIC port dedicated to the bench:
//! no flow rules isolate the socket's traffic from the kernel's. One polling thread per port
//! busy-reads the receive queue on DPDK's main lcore (`-l`) and hands datagrams
//! to the bound sockets; sends go straight to the transmit queue under a lock.
//!
//! Built only with `QUINN_BUILD_DPDK=1` in the build environment, which needs
//! DPDK through `pkg-config` (libdpdk).
//!
//! [`TokioRuntime`](super::TokioRuntime) returns this socket from
//! `wrap_udp_socket` when `QUINN_DPDK_EAL` is set, so an application that builds
//! its endpoint through the runtime needs no changes. See [`from_env`].

use std::{
    collections::{HashMap, VecDeque},
    ffi::{CString, c_char, c_int},
    fmt, io,
    io::IoSliceMut,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
};

use udp::{EcnCodepoint, RecvMeta, Transmit};

use super::{AsyncUdpSocket, UdpPoller};

/// Bytes reserved per frame in the copy buffers: longer frames are truncated on
/// receive and refused on send.
const STRIDE: usize = 2048;
/// Frames per receive or transmit burst, matching the shim's limit.
const BURST: usize = 64;
const HEADERS: usize = 14 + 20 + 8;
/// Datagrams a socket holds before new ones are dropped, like a socket buffer.
const INBOX_LIMIT: usize = 8192;

mod ffi {
    use std::ffi::{c_char, c_int};

    extern "C" {
        pub(super) fn qd_init(argc: c_int, argv: *mut *mut c_char) -> c_int;
        pub(super) fn qd_pin_main_lcore() -> c_int;
        pub(super) fn qd_port_start(port: u16, mac: *mut u8) -> c_int;
        pub(super) fn qd_ring_pair(a: *mut u16, b: *mut u16) -> c_int;
        pub(super) fn qd_rx(port: u16, buf: *mut u8, stride: u16, lens: *mut u16, max: u16) -> u16;
        pub(super) fn qd_tx(
            port: u16,
            frames: *const u8,
            stride: u16,
            lens: *const u16,
            n: u16,
        ) -> u16;
    }
}

fn check(rc: c_int) -> io::Result<()> {
    match rc {
        0.. => Ok(()),
        _ => Err(io::Error::from_raw_os_error(-rc)),
    }
}

static INITIALIZED: Mutex<bool> = Mutex::new(false);

/// Initializes DPDK's environment with `args` (`args[0]` is the program name).
/// Later calls do nothing, since DPDK initializes once per process.
pub fn init(args: &[&str]) -> io::Result<()> {
    let mut done = INITIALIZED.lock().unwrap();
    if *done {
        return Ok(());
    }
    let args = args
        .iter()
        .map(|a| CString::new(*a).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)))
        .collect::<io::Result<Vec<_>>>()?;
    // DPDK may keep pointers into argv, so the strings live for the process.
    let mut argv: Vec<*mut c_char> = args.into_iter().map(CString::into_raw).collect();
    check(unsafe { ffi::qd_init(argv.len() as c_int, argv.as_mut_ptr()) })?;
    std::mem::forget(argv);
    *done = true;
    Ok(())
}

/// Builds the socket `wrap_udp_socket` returns when `QUINN_DPDK_EAL` is set.
///
/// - `QUINN_DPDK_EAL`: DPDK's arguments, separated by spaces.
/// - `QUINN_DPDK_PORT`: the DPDK port ID, 0 by default.
/// - `QUINN_DPDK_ADDR`: the IPv4 address this host answers on.
/// - `QUINN_DPDK_GATEWAY_MAC`: where to send to a peer not heard from yet.
///
/// The socket takes `socket`'s UDP port and keeps `socket` open so the kernel
/// does not hand that port to anyone else.
pub(super) fn from_env(socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
    static DEVICE: Mutex<Option<Arc<Device>>> = Mutex::new(None);
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidInput, what.to_owned());
    let mut device = DEVICE.lock().unwrap();
    let device = match &*device {
        Some(d) => d.clone(),
        None => {
            let eal = std::env::var("QUINN_DPDK_EAL").unwrap_or_default();
            let mut args = vec!["quinn"];
            args.extend(eal.split_whitespace());
            init(&args)?;
            let port = match std::env::var("QUINN_DPDK_PORT") {
                Ok(p) => p
                    .parse()
                    .map_err(|_| invalid("QUINN_DPDK_PORT is not a port ID"))?,
                Err(_) => 0,
            };
            let ip = std::env::var("QUINN_DPDK_ADDR")
                .map_err(|_| invalid("QUINN_DPDK_ADDR is required with QUINN_DPDK_EAL"))?
                .parse()
                .map_err(|_| invalid("QUINN_DPDK_ADDR is not an IPv4 address"))?;
            let gateway = match std::env::var("QUINN_DPDK_GATEWAY_MAC") {
                Ok(m) => Some(parse_mac(&m).ok_or_else(|| invalid("bad QUINN_DPDK_GATEWAY_MAC"))?),
                Err(_) => None,
            };
            device.insert(Device::start(port, ip, gateway)?).clone()
        }
    };
    let local = socket.local_addr()?;
    let socket = device.bind_kernel(local.port(), local.is_ipv6(), Some(socket))?;
    Ok(socket)
}

fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let mut mac = [0; 6];
    let mut parts = s.split(':');
    for byte in &mut mac {
        *byte = u8::from_str_radix(parts.next()?, 16).ok()?;
    }
    parts.next().is_none().then_some(mac)
}

/// A started DPDK port, shared by the sockets bound to it.
pub struct Device {
    port: u16,
    mac: [u8; 6],
    ip: Ipv4Addr,
    gateway: Option<[u8; 6]>,
    /// Serializes sends, since a DPDK transmit queue is not thread safe.
    tx: Mutex<()>,
    sockets: Mutex<HashMap<u16, Arc<Inbox>>>,
    neighbors: Mutex<HashMap<Ipv4Addr, [u8; 6]>>,
}

impl fmt::Debug for Device {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("port", &self.port)
            .field("ip", &self.ip)
            .finish_non_exhaustive()
    }
}

impl Device {
    /// Starts DPDK port `port` answering on `ip`, and its polling thread, which
    /// runs for the rest of the process. Call [`init`] first.
    pub fn start(port: u16, ip: Ipv4Addr, gateway: Option<[u8; 6]>) -> io::Result<Arc<Self>> {
        let mut mac = [0; 6];
        check(unsafe { ffi::qd_port_start(port, mac.as_mut_ptr()) })?;
        let device = Arc::new(Self {
            port,
            mac,
            ip,
            gateway,
            tx: Mutex::new(()),
            sockets: Mutex::new(HashMap::new()),
            neighbors: Mutex::new(HashMap::new()),
        });
        let polled = device.clone();
        thread::Builder::new()
            .name(format!("quinn-dpdk-{port}"))
            .spawn(move || polled.poll())?;
        Ok(device)
    }

    /// Creates two ports wired back to back, for tests without a NIC.
    pub fn ring_pair() -> io::Result<(u16, u16)> {
        let (mut a, mut b) = (0, 0);
        check(unsafe { ffi::qd_ring_pair(&mut a, &mut b) })?;
        Ok((a, b))
    }

    /// Binds a socket to UDP port `port` on this device.
    pub fn bind(self: &Arc<Self>, port: u16) -> io::Result<Arc<DpdkSocket>> {
        self.bind_kernel(port, false, None)
    }

    fn bind_kernel(
        self: &Arc<Self>,
        port: u16,
        ipv6: bool,
        kernel: Option<std::net::UdpSocket>,
    ) -> io::Result<Arc<DpdkSocket>> {
        let inbox = Arc::new(Inbox::default());
        let mut sockets = self.sockets.lock().unwrap();
        if sockets.contains_key(&port) {
            return Err(io::ErrorKind::AddrInUse.into());
        }
        sockets.insert(port, inbox.clone());
        Ok(Arc::new(DpdkSocket {
            device: self.clone(),
            inbox,
            port,
            ipv6,
            kernel,
        }))
    }

    fn poll(&self) {
        // Busy polling would starve whatever else shares its CPU, so it takes the
        // main lcore that DPDK's arguments name, apart from the application's threads.
        if let Err(e) = check(unsafe { ffi::qd_pin_main_lcore() }) {
            tracing::warn!("DPDK polling thread keeps the application's CPUs: {e}");
        }
        let mut buf = vec![0u8; STRIDE * BURST];
        let mut lens = [0u16; BURST];
        loop {
            let n = unsafe {
                ffi::qd_rx(
                    self.port,
                    buf.as_mut_ptr(),
                    STRIDE as u16,
                    lens.as_mut_ptr(),
                    BURST as u16,
                )
            } as usize;
            if n == 0 {
                std::hint::spin_loop();
                continue;
            }
            let at = quic_trace::global().now_ns_if_enabled();
            for (i, &len) in lens[..n].iter().enumerate() {
                let frame = &buf[i * STRIDE..i * STRIDE + len as usize];
                if let Some(frame) = parse(frame) {
                    self.deliver(frame, at);
                } else if let Some((reply, asker, asker_mac)) = arp_reply(frame, self.mac, self.ip)
                {
                    self.neighbors.lock().unwrap().insert(asker, asker_mac);
                    let _tx = self.tx.lock().unwrap();
                    let len = reply.len() as u16;
                    unsafe { ffi::qd_tx(self.port, reply.as_ptr(), len, &len, 1) };
                }
            }
        }
    }

    fn deliver(&self, frame: Frame<'_>, at: Option<u64>) {
        if frame.dst_ip != self.ip || (frame.dst_mac != self.mac && frame.dst_mac != [0xff; 6]) {
            return;
        }
        self.neighbors
            .lock()
            .unwrap()
            .insert(*frame.src.ip(), frame.src_mac);
        let Some(inbox) = self.sockets.lock().unwrap().get(&frame.dst_port).cloned() else {
            return;
        };
        let mut state = inbox.state.lock().unwrap();
        if state.queue.len() >= INBOX_LIMIT {
            return;
        }
        // shortcut: one allocation per datagram; pool the buffers if it shows in profiles.
        state.queue.push_back(Datagram {
            src: frame.src,
            dst: frame.dst_ip,
            ecn: frame.ecn,
            data: frame.payload.to_vec(),
            at,
        });
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[derive(Default)]
struct Inbox {
    state: Mutex<InboxState>,
}

#[derive(Default)]
struct InboxState {
    queue: VecDeque<Datagram>,
    waker: Option<Waker>,
}

struct Datagram {
    src: SocketAddrV4,
    dst: Ipv4Addr,
    ecn: u8,
    data: Vec<u8>,
    at: Option<u64>,
}

/// A UDP socket on a [`Device`].
pub struct DpdkSocket {
    device: Arc<Device>,
    inbox: Arc<Inbox>,
    port: u16,
    /// Reports addresses as IPv4-mapped IPv6, as a dual-stack socket would.
    ipv6: bool,
    /// Holds the port reserved in the kernel.
    kernel: Option<std::net::UdpSocket>,
}

impl fmt::Debug for DpdkSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DpdkSocket")
            .field("device", &self.device)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Drop for DpdkSocket {
    fn drop(&mut self) {
        self.device.sockets.lock().unwrap().remove(&self.port);
    }
}

impl DpdkSocket {
    fn report(&self, addr: SocketAddrV4) -> SocketAddr {
        match self.ipv6 {
            true => SocketAddrV6::new(addr.ip().to_ipv6_mapped(), addr.port(), 0, 0).into(),
            false => addr.into(),
        }
    }
}

#[derive(Debug)]
struct AlwaysWritable;

impl UdpPoller for AlwaysWritable {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncUdpSocket for DpdkSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(AlwaysWritable)
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        let dst = match transmit.destination {
            SocketAddr::V4(a) => a,
            SocketAddr::V6(a) => match a.ip().to_ipv4_mapped() {
                Some(ip) => SocketAddrV4::new(ip, a.port()),
                // shortcut: IPv4 only; add IPv6 headers when a bench needs them.
                None => return Err(io::ErrorKind::Unsupported.into()),
            },
        };
        let device = &*self.device;
        let dst_mac = device
            .neighbors
            .lock()
            .unwrap()
            .get(dst.ip())
            .copied()
            .or(device.gateway)
            .ok_or(io::ErrorKind::HostUnreachable)?;
        let segment = transmit.segment_size.unwrap_or(transmit.contents.len());
        if HEADERS + segment > STRIDE {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let ecn = transmit.ecn.map_or(0, |e| e as u8);
        let mut frames = vec![0u8; STRIDE * transmit.contents.len().div_ceil(segment).max(1)];
        let mut lens = Vec::with_capacity(BURST);
        for (i, payload) in transmit.contents.chunks(segment).enumerate() {
            let frame = &mut frames[i * STRIDE..];
            let header = Header {
                dst_mac,
                src_mac: device.mac,
                src: SocketAddrV4::new(device.ip, self.port),
                dst,
                ecn,
            };
            lens.push(write(frame, &header, payload) as u16);
        }
        let _tx = device.tx.lock().unwrap();
        // shortcut: frames the NIC queue refuses are dropped, like a full socket buffer;
        // quinn recovers them as loss. Add backpressure if drops show in benches.
        unsafe {
            ffi::qd_tx(
                device.port,
                frames.as_ptr(),
                STRIDE as u16,
                lens.as_ptr(),
                lens.len() as u16,
            )
        };
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.inbox.state.lock().unwrap();
        if state.queue.is_empty() {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut n = 0;
        for (buf, meta) in bufs.iter_mut().zip(meta.iter_mut()) {
            let Some(d) = state.queue.pop_front() else {
                break;
            };
            let len = d.data.len().min(buf.len());
            buf[..len].copy_from_slice(&d.data[..len]);
            *meta = RecvMeta::default();
            meta.addr = self.report(d.src);
            meta.len = len;
            meta.stride = len;
            meta.ecn = EcnCodepoint::from_bits(d.ecn);
            meta.dst_ip = Some(match self.ipv6 {
                true => IpAddr::V6(d.dst.to_ipv6_mapped()),
                false => IpAddr::V4(d.dst),
            });
            meta.quic_trace_read_ns = d.at;
            n += 1;
        }
        Poll::Ready(Ok(n))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.kernel {
            Some(k) => k.local_addr(),
            None => Ok(SocketAddrV4::new(self.device.ip, self.port).into()),
        }
    }

    fn max_transmit_segments(&self) -> usize {
        BURST
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

struct Header {
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    src: SocketAddrV4,
    dst: SocketAddrV4,
    ecn: u8,
}

/// Writes an Ethernet, IPv4, and UDP frame carrying `payload`; returns its length.
fn write(out: &mut [u8], h: &Header, payload: &[u8]) -> usize {
    out[0..6].copy_from_slice(&h.dst_mac);
    out[6..12].copy_from_slice(&h.src_mac);
    out[12..14].copy_from_slice(&0x0800u16.to_be_bytes());
    let ip = &mut out[14..34];
    ip[0] = 0x45;
    ip[1] = h.ecn & 0b11;
    ip[2..4].copy_from_slice(&((20 + 8 + payload.len()) as u16).to_be_bytes());
    ip[4..6].fill(0);
    ip[6..8].copy_from_slice(&0x4000u16.to_be_bytes()); // don't fragment
    ip[8] = 64;
    ip[9] = 17;
    ip[10..12].fill(0);
    ip[12..16].copy_from_slice(&h.src.ip().octets());
    ip[16..20].copy_from_slice(&h.dst.ip().octets());
    let sum = checksum(ip);
    ip[10..12].copy_from_slice(&sum.to_be_bytes());
    let udp = &mut out[34..42];
    udp[0..2].copy_from_slice(&h.src.port().to_be_bytes());
    udp[2..4].copy_from_slice(&h.dst.port().to_be_bytes());
    let udp_len = (8 + payload.len()) as u16;
    udp[4..6].copy_from_slice(&udp_len.to_be_bytes());
    udp[6..8].fill(0);
    out[HEADERS..HEADERS + payload.len()].copy_from_slice(payload);
    // Zero would be valid over IPv4, but Linux UDP GRO refuses such datagrams, so
    // receivers would take every segment separately.
    let pseudo = word_sum(&out[26..34]) + 17 + u64::from(udp_len);
    let udp_sum = fold(pseudo + word_sum(&out[34..HEADERS + payload.len()]));
    // An all-zero result is sent as all ones, since zero means "no checksum".
    let udp_sum = if udp_sum == 0 { 0xffff } else { udp_sum };
    out[40..42].copy_from_slice(&udp_sum.to_be_bytes());
    HEADERS + payload.len()
}

/// The Internet checksum of `header`.
fn checksum(header: &[u8]) -> u16 {
    fold(word_sum(header))
}

/// Sums `data` as big-endian 16-bit words, padding an odd tail with a zero byte.
fn word_sum(data: &[u8]) -> u64 {
    data.chunks(2)
        .map(|w| u64::from(u16::from_be_bytes([w[0], *w.get(1).unwrap_or(&0)])))
        .sum()
}

/// Folds a word sum to 16 bits and complements it.
fn fold(mut sum: u64) -> u16 {
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Answers an ARP request for `ip`, so peers on the kernel stack can reach this
/// address. Returns the reply and the asker's address and MAC.
fn arp_reply(f: &[u8], mac: [u8; 6], ip: Ipv4Addr) -> Option<([u8; 60], Ipv4Addr, [u8; 6])> {
    // Ethernet/IPv4 ARP: hardware 1, protocol 0x0800, lengths 6 and 4, request 1.
    const REQUEST: [u8; 8] = [0, 1, 8, 0, 6, 4, 0, 1];
    if f.len() < 42 || f[12..14] != [8, 6] || f[14..22] != REQUEST || f[38..42] != ip.octets() {
        return None;
    }
    let asker_mac: [u8; 6] = f[22..28].try_into().unwrap();
    let asker: [u8; 4] = f[28..32].try_into().unwrap();
    // Padded to Ethernet's 60-byte minimum, which not every driver adds.
    let mut reply = [0u8; 60];
    reply[0..6].copy_from_slice(&asker_mac);
    reply[6..12].copy_from_slice(&mac);
    reply[12..14].copy_from_slice(&[8, 6]);
    reply[14..21].copy_from_slice(&REQUEST[..7]);
    reply[21] = 2; // reply
    reply[22..28].copy_from_slice(&mac);
    reply[28..32].copy_from_slice(&ip.octets());
    reply[32..38].copy_from_slice(&asker_mac);
    reply[38..42].copy_from_slice(&asker);
    Some((reply, asker.into(), asker_mac))
}

#[derive(Debug, PartialEq)]
struct Frame<'a> {
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    src: SocketAddrV4,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    ecn: u8,
    payload: &'a [u8],
}

/// Parses an unfragmented IPv4 UDP frame; anything else is `None`.
fn parse(f: &[u8]) -> Option<Frame<'_>> {
    let be16 = |b: &[u8], i: usize| u16::from_be_bytes([b[i], b[i + 1]]);
    if f.len() < HEADERS || be16(f, 12) != 0x0800 {
        return None;
    }
    let ip = &f[14..];
    let ihl = usize::from(ip[0] & 0x0f) * 4;
    let total = usize::from(be16(ip, 2));
    if ip[0] >> 4 != 4 || ihl < 20 || ip[9] != 17 || be16(ip, 6) & 0x3fff != 0 {
        return None;
    }
    if total > ip.len() || total < ihl + 8 {
        return None;
    }
    let udp = &ip[ihl..total];
    let udp_len = usize::from(be16(udp, 4));
    if udp_len < 8 || udp_len > udp.len() {
        return None;
    }
    let octets = |b: &[u8]| Ipv4Addr::new(b[0], b[1], b[2], b[3]);
    Some(Frame {
        dst_mac: f[0..6].try_into().unwrap(),
        src_mac: f[6..12].try_into().unwrap(),
        src: SocketAddrV4::new(octets(&ip[12..16]), be16(udp, 0)),
        dst_ip: octets(&ip[16..20]),
        dst_port: be16(udp, 2),
        ecn: ip[1] & 0b11,
        payload: &udp[8..udp_len],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let header = Header {
            dst_mac: [1, 2, 3, 4, 5, 6],
            src_mac: [7, 8, 9, 10, 11, 12],
            src: "10.0.0.1:4433".parse().unwrap(),
            dst: "10.0.0.2:5000".parse().unwrap(),
            ecn: 0b10,
        };
        let mut out = [0u8; STRIDE];
        let len = write(&mut out, &header, b"hello");
        let frame = parse(&out[..len]).unwrap();
        assert_eq!(
            frame,
            Frame {
                dst_mac: header.dst_mac,
                src_mac: header.src_mac,
                src: header.src,
                dst_ip: *header.dst.ip(),
                dst_port: 5000,
                ecn: 0b10,
                payload: b"hello",
            }
        );
        // A valid header sums to zero once its checksum is included.
        assert_eq!(checksum(&out[14..34]), 0);
        // So does the UDP datagram with its pseudo-header, worked out here word by
        // word: addresses, protocol, UDP length, then the datagram padded to even.
        let mut words = out[26..34].to_vec();
        words.extend([0, 17, 0, 13]);
        words.extend(&out[34..len]);
        words.push(0);
        let total: u32 = words
            .chunks(2)
            .map(|w| u32::from(w[0]) << 8 | u32::from(w[1]))
            .sum();
        assert_eq!(total % 0xffff, 0, "UDP checksum does not verify");
        assert_ne!(&out[40..42], &[0, 0]);
        // Truncated and fragmented frames are rejected.
        assert!(parse(&out[..len - 1]).is_none());
        out[14 + 7] = 1;
        assert!(parse(&out[..len]).is_none());
    }

    #[test]
    fn arp_requests_for_this_address_are_answered() {
        let mac = [2, 0, 0, 0, 0, 1];
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        let mut request = [0u8; 42];
        request[0..6].fill(0xff);
        request[6..12].copy_from_slice(&[2, 0, 0, 0, 0, 2]);
        request[12..14].copy_from_slice(&[8, 6]);
        request[14..22].copy_from_slice(&[0, 1, 8, 0, 6, 4, 0, 1]);
        request[22..28].copy_from_slice(&[2, 0, 0, 0, 0, 2]);
        request[28..32].copy_from_slice(&[10, 0, 0, 2]);
        request[38..42].copy_from_slice(&ip.octets());

        let (reply, asker, asker_mac) = arp_reply(&request, mac, ip).unwrap();
        assert_eq!(
            (asker, asker_mac),
            (Ipv4Addr::new(10, 0, 0, 2), [2, 0, 0, 0, 0, 2])
        );
        assert_eq!(&reply[0..6], &asker_mac);
        assert_eq!(&reply[14..22], &[0, 1, 8, 0, 6, 4, 0, 2]);
        assert_eq!(&reply[22..32], &[2, 0, 0, 0, 0, 1, 10, 0, 0, 1]);
        assert_eq!(&reply[32..42], &[2, 0, 0, 0, 0, 2, 10, 0, 0, 2]);
        // A request for another address, or a reply, is left alone.
        assert!(arp_reply(&request, mac, Ipv4Addr::new(10, 0, 0, 9)).is_none());
        request[21] = 2;
        assert!(arp_reply(&request, mac, ip).is_none());
    }

    #[test]
    fn mac_parsing() {
        assert_eq!(
            parse_mac("ff:00:0a:0B:1c:2d"),
            Some([0xff, 0, 0x0a, 0x0b, 0x1c, 0x2d])
        );
        assert_eq!(parse_mac("ff:00:0a:0b:1c"), None);
        assert_eq!(parse_mac("ff:00:0a:0b:1c:2d:3e"), None);
    }
}
