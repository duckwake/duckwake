//! What the worker process uses: CPU time, memory, threads and open files, read from the OS (Linux and macOS),
//! for the `process_*` series Prometheus clients conventionally export.

#[derive(Debug, Default, Clone, Copy)]
pub struct Usage {
    /// User plus system CPU time since the process started.
    pub cpu_seconds: f64,
    pub resident_bytes: Option<u64>,
    pub max_resident_bytes: Option<u64>,
    pub threads: Option<u64>,
    pub open_fds: Option<u64>,
}

/// Reads the process's usage now.
pub fn usage() -> Usage {
    let mut u = Usage::default();
    // SAFETY: getrusage fills the struct it is given and reads nothing else.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } == 0 {
        let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        u.cpu_seconds = secs(ru.ru_utime) + secs(ru.ru_stime);
        // Linux reports the peak in KiB, macOS in bytes.
        let max = ru.ru_maxrss.max(0) as u64;
        u.max_resident_bytes = Some(if cfg!(target_os = "macos") { max } else { max * 1024 });
    }
    platform(&mut u);
    u
}

#[cfg(target_os = "linux")]
fn platform(u: &mut Usage) {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let field = |name: &str| {
            status.lines().find_map(|l| l.strip_prefix(name)).and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        };
        u.resident_bytes = field("VmRSS:").map(|kb| kb * 1024);
        u.threads = field("Threads:");
    }
    u.open_fds = std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count() as u64);
}

#[cfg(target_os = "macos")]
fn platform(u: &mut Usage) {
    // SAFETY: proc_pidinfo writes at most `size` bytes into the struct it is given.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let n = unsafe {
        libc::proc_pidinfo(libc::getpid(), libc::PROC_PIDTASKINFO, 0, &mut info as *mut _ as *mut libc::c_void, size)
    };
    if n == size {
        u.resident_bytes = Some(info.pti_resident_size);
        u.threads = Some(info.pti_threadnum.max(0) as u64);
    }
    u.open_fds = std::fs::read_dir("/dev/fd").ok().map(|d| d.count() as u64);
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform(_: &mut Usage) {}

/// Parses a DuckDB memory setting as `current_setting('memory_limit')` shows it (`25.5 GiB`, `244.1 MiB`,
/// `512 bytes`) into bytes.
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(s.len());
    let n: f64 = s[..split].trim().parse().ok()?;
    let unit = match s[split..].trim().to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((n * unit) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_this_process() {
        let mut spin = 0u64;
        for i in 0..2_000_000u64 {
            spin = spin.wrapping_add(i * i);
        }
        assert!(spin > 0);
        let u = usage();
        assert!(u.cpu_seconds > 0.0);
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(u.resident_bytes.unwrap() > 1 << 20, "{u:?}");
            assert!(u.threads.unwrap() >= 1);
            assert!(u.open_fds.unwrap() >= 3);
            assert!(u.max_resident_bytes.unwrap() >= u.resident_bytes.unwrap() / 2);
        }
    }

    #[test]
    fn duckdb_sizes() {
        assert_eq!(parse_size("61.0 MiB"), Some(63_963_136));
        assert_eq!(parse_size("25.5 GiB"), Some(27_380_416_512));
        assert_eq!(parse_size("512 bytes"), Some(512));
        assert_eq!(parse_size("2GB"), Some(2_000_000_000));
        assert_eq!(parse_size("lots"), None);
    }
}
