// A thin C layer over the DPDK calls that are static inline in its headers, so
// the Rust socket in src/runtime/dpdk.rs can call them. Frames are copied in and
// out of mbufs; the Rust side never sees an mbuf.
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_eth_ring.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ring.h>

#define QD_MAX_PORTS 32
#define QD_RING_SIZE 1024
#define QD_BURST 64

static struct rte_mempool *pools[QD_MAX_PORTS];

// Returns 0 or a negative errno. DPDK pins the calling thread to its main lcore;
// the caller is an application thread, so its own CPU set is put back, and only
// the polling thread takes the main lcore (qd_pin_main_lcore).
int qd_init(int argc, char **argv) {
    cpu_set_t caller;
    int rc = pthread_getaffinity_np(pthread_self(), sizeof(caller), &caller);
    if (rc != 0) return -rc;
    if (rte_eal_init(argc, argv) < 0) {
        return -rte_errno;
    }
    rc = pthread_setaffinity_np(pthread_self(), sizeof(caller), &caller);
    return -rc;
}

// Pins the calling thread to the CPUs of DPDK's main lcore (`-l` or `--main-lcore`).
int qd_pin_main_lcore(void) {
    rte_cpuset_t cpus = rte_lcore_cpuset(rte_get_main_lcore());
    return -pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
}

// Configures one RX and one TX queue on `port`, starts it, and writes its MAC.
int qd_port_start(uint16_t port, uint8_t mac_out[6]) {
    if (port >= QD_MAX_PORTS || !rte_eth_dev_is_valid_port(port)) {
        return -ENODEV;
    }
    char name[RTE_MEMPOOL_NAMESIZE];
    snprintf(name, sizeof(name), "quinn_dpdk_%u", port);
    pools[port] = rte_pktmbuf_pool_create(name, 8191, 256, 0, RTE_MBUF_DEFAULT_BUF_SIZE,
                                          rte_eth_dev_socket_id(port));
    if (pools[port] == NULL) {
        return -rte_errno;
    }
    struct rte_eth_conf conf;
    memset(&conf, 0, sizeof(conf));
    int rc = rte_eth_dev_configure(port, 1, 1, &conf);
    if (rc < 0) return rc;
    int socket = rte_eth_dev_socket_id(port);
    rc = rte_eth_rx_queue_setup(port, 0, QD_RING_SIZE, socket, NULL, pools[port]);
    if (rc < 0) return rc;
    rc = rte_eth_tx_queue_setup(port, 0, QD_RING_SIZE, socket, NULL);
    if (rc < 0) return rc;
    rc = rte_eth_dev_start(port);
    if (rc < 0) return rc;
    // Not every driver supports it; the socket filters by MAC itself.
    (void)rte_eth_promiscuous_enable(port);
    struct rte_ether_addr addr;
    rc = rte_eth_macaddr_get(port, &addr);
    if (rc < 0) return rc;
    memcpy(mac_out, addr.addr_bytes, 6);
    return 0;
}

// Creates two ring ports wired back to back, for tests without a NIC.
int qd_ring_pair(uint16_t *a, uint16_t *b) {
    static unsigned pair_count;
    char name[RTE_RING_NAMESIZE];
    unsigned n = pair_count++;
    snprintf(name, sizeof(name), "qd_ab_%u", n);
    struct rte_ring *ab = rte_ring_create(name, QD_RING_SIZE, SOCKET_ID_ANY, 0);
    snprintf(name, sizeof(name), "qd_ba_%u", n);
    struct rte_ring *ba = rte_ring_create(name, QD_RING_SIZE, SOCKET_ID_ANY, 0);
    if (ab == NULL || ba == NULL) {
        return -rte_errno;
    }
    snprintf(name, sizeof(name), "qd_a_%u", n);
    int pa = rte_eth_from_rings(name, &ba, 1, &ab, 1, SOCKET_ID_ANY);
    snprintf(name, sizeof(name), "qd_b_%u", n);
    int pb = rte_eth_from_rings(name, &ab, 1, &ba, 1, SOCKET_ID_ANY);
    if (pa < 0 || pb < 0) {
        return -rte_errno;
    }
    *a = (uint16_t)pa;
    *b = (uint16_t)pb;
    return 0;
}

// Receives up to `max` frames into `buf` at `stride`-byte slots, writing each
// frame's length to `lens`. A frame longer than `stride` is truncated, and a
// chained mbuf contributes only its first segment. Returns the frame count.
uint16_t qd_rx(uint16_t port, uint8_t *buf, uint16_t stride, uint16_t *lens, uint16_t max) {
    struct rte_mbuf *mbufs[QD_BURST];
    if (max > QD_BURST) max = QD_BURST;
    uint16_t n = rte_eth_rx_burst(port, 0, mbufs, max);
    for (uint16_t i = 0; i < n; i++) {
        uint16_t len = rte_pktmbuf_data_len(mbufs[i]);
        if (len > stride) len = stride;
        memcpy(buf + (size_t)i * stride, rte_pktmbuf_mtod(mbufs[i], void *), len);
        lens[i] = len;
        rte_pktmbuf_free(mbufs[i]);
    }
    return n;
}

// Sends `n` frames laid out at `stride`-byte slots in `frames`. Returns how many
// the driver accepted; the rest are freed, as a full NIC queue drops them.
// Callers serialize calls per port: a DPDK TX queue is not thread safe.
uint16_t qd_tx(uint16_t port, const uint8_t *frames, uint16_t stride, const uint16_t *lens,
               uint16_t n) {
    struct rte_mbuf *mbufs[QD_BURST];
    if (n > QD_BURST) n = QD_BURST;
    uint16_t built = 0;
    for (; built < n; built++) {
        struct rte_mbuf *m = rte_pktmbuf_alloc(pools[port]);
        if (m == NULL) break;
        char *data = rte_pktmbuf_append(m, lens[built]);
        if (data == NULL) {
            rte_pktmbuf_free(m);
            break;
        }
        memcpy(data, frames + (size_t)built * stride, lens[built]);
        mbufs[built] = m;
    }
    uint16_t sent = rte_eth_tx_burst(port, 0, mbufs, built);
    for (uint16_t i = sent; i < built; i++) {
        rte_pktmbuf_free(mbufs[i]);
    }
    return sent;
}
