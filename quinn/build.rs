use cfg_aliases::cfg_aliases;

fn main() {
    // Setup cfg aliases
    cfg_aliases! {
        // Convenience aliases
        wasm_browser: { all(target_family = "wasm", target_os = "unknown") },
    }

    // The DPDK socket is switched on by the environment rather than a feature:
    // the relays depend on upstream Quinn when built on their own, and Cargo
    // rejects a reference to a feature that upstream lacks, even an unused one.
    println!("cargo:rustc-check-cfg=cfg(quinn_dpdk)");
    println!("cargo:rerun-if-env-changed=QUINN_BUILD_DPDK");
    if std::env::var_os("QUINN_BUILD_DPDK").is_some_and(|v| v == "1") {
        println!("cargo:rustc-cfg=quinn_dpdk");
        build_dpdk_shim();
    }
}

/// Compiles `dpdk/shim.c` against the DPDK that `pkg-config` finds.
fn build_dpdk_shim() {
    println!("cargo:rerun-if-changed=dpdk/shim.c");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    let flags = |arg: &str| -> Vec<String> {
        let out = std::process::Command::new("pkg-config")
            .args([arg, "libdpdk"])
            .output()
            .expect("QUINN_BUILD_DPDK: pkg-config is not installed");
        assert!(
            out.status.success(),
            "QUINN_BUILD_DPDK: pkg-config cannot find libdpdk: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout)
            .unwrap()
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    };
    let mut build = cc::Build::new();
    build.file("dpdk/shim.c").warnings(false);
    for flag in flags("--cflags") {
        build.flag(&flag);
    }
    build.compile("quinn_dpdk_shim");
    // Drivers stay out of `--libs`; the shim calls the ring driver directly, and a
    // NIC's driver loads at runtime from DPDK's plugin directory.
    println!("cargo:rustc-link-lib=rte_net_ring");
    for flag in flags("--libs") {
        if let Some(dir) = flag.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={dir}");
        } else if let Some(lib) = flag.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={lib}");
        } else {
            println!("cargo:rustc-link-arg={flag}");
        }
    }
}
