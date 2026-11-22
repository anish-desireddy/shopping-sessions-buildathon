//! Shopping Sessions pipeline: exact implementation of rules.md.
//! Multi-threaded: parse -> partitioned dedup -> per-person grouping -> sessions/attribution -> Parquet.

mod pq;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;


// ---------------------------------------------------------------- allocator
// Big allocations come straight from mmap with transparent huge pages requested: fewer page
// faults on fresh memory and much cheaper teardown.
#[cfg(target_os = "linux")]
mod halloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    pub struct HugeAlloc;
    const BIG: usize = 1 << 20;
    const HP: usize = 2 << 20;
    #[inline]
    fn round(n: usize) -> usize {
        (n + HP - 1) & !(HP - 1)
    }
    #[inline]
    fn big(size: usize, align: usize) -> bool {
        size >= BIG && align <= 4096
    }
    unsafe impl GlobalAlloc for HugeAlloc {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            if big(l.size(), l.align()) {
                // map 2 MB extra and trim, so the block starts on a 2 MB boundary and every
                // 2 MB of it can be a transparent huge page
                let sz = round(l.size());
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    sz + HP,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                );
                if p == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                let a = (p as usize + HP - 1) & !(HP - 1);
                let head = a - p as usize;
                if head != 0 {
                    libc::munmap(p, head);
                }
                let tail = HP - head;
                if tail != 0 {
                    libc::munmap((a + sz) as *mut libc::c_void, tail);
                }
                libc::madvise(a as *mut libc::c_void, sz, libc::MADV_HUGEPAGE);
                a as *mut u8
            } else {
                System.alloc(l)
            }
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            if big(l.size(), l.align()) {
                self.alloc(l)
            } else {
                System.alloc_zeroed(l)
            }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            if big(l.size(), l.align()) {
                libc::munmap(p as *mut libc::c_void, round(l.size()));
            } else {
                System.dealloc(p, l)
            }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
            let ob = big(l.size(), l.align());
            let nb = big(new_size, l.align());
            if ob && nb {
                let q = libc::mremap(p as *mut libc::c_void, round(l.size()), round(new_size), libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(q, round(new_size), libc::MADV_HUGEPAGE);
                q as *mut u8
            } else if !ob && !nb {
                System.realloc(p, l, new_size)
            } else {
                let nl = Layout::from_size_align_unchecked(new_size, l.align());
                let q = self.alloc(nl);
                if !q.is_null() {
                    std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
                    self.dealloc(p, l);
                }
                q
            }
        }
    }
}
#[cfg(target_os = "linux")]
#[global_allocator]
static GA: halloc::HugeAlloc = halloc::HugeAlloc;

/// Records staged per partition before a write-combined flush.
const WK: usize = 8;

/// Per-thread partitioned buffer: one big allocation, fixed capacity per partition, overflow Vecs.
/// Pushes are staged WK at a time in a small cache-resident buffer and flushed as whole cache
/// lines (non-temporal stores on Linux/AVX2 builds), so scattering does not stall on misses.
struct PartBuf<T: Copy> {
    mem: Vec<T>,
    cap: usize,
    lens: Vec<u32>,
    over: Vec<Vec<T>>,
    /// staging area: WK slots per partition at mem[sbase..] (inside the same big, huge-page
    /// backed allocation as the partitions, so the scattered stage writes stay TLB-friendly)
    sbase: usize,
    slen: Vec<u32>,
    nt: bool,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn nt_copy(src: *const u8, dst: *mut u8, bytes: usize) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i < bytes {
        let v = _mm256_loadu_si256(src.add(i) as *const __m256i);
        _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
        i += 32;
    }
}

impl<T: Copy> PartBuf<T> {
    fn new(nparts: usize, cap: usize) -> Self {
        let cap = (cap + WK - 1) / WK * WK;
        let mem: Vec<T> = Vec::with_capacity(nparts * cap + nparts * WK);
        let nt = cfg!(all(target_arch = "x86_64", target_feature = "avx2"))
            && (mem.as_ptr() as usize) % 32 == 0
            && (std::mem::size_of::<T>() * WK) % 32 == 0;
        PartBuf {
            mem,
            cap,
            lens: vec![0; nparts],
            over: (0..nparts).map(|_| Vec::new()).collect(),
            sbase: nparts * cap,
            slen: vec![0; nparts],
            nt,
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        unsafe {
            let n = *self.slen.get_unchecked(p) as usize;
            self.mem.as_mut_ptr().add(self.sbase + p * WK + n).write(v);
            if n + 1 < WK {
                *self.slen.get_unchecked_mut(p) = (n + 1) as u32;
            } else {
                *self.slen.get_unchecked_mut(p) = 0;
                self.flush_full(p);
            }
        }
    }
    #[inline(always)]
    unsafe fn flush_full(&mut self, p: usize) {
        let l = *self.lens.get_unchecked(p) as usize;
        let src = self.mem.as_ptr().add(self.sbase + p * WK);
        if l + WK <= self.cap {
            let dst = self.mem.as_mut_ptr().add(p * self.cap + l);
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                if self.nt {
                    nt_copy(src as *const u8, dst as *mut u8, WK * std::mem::size_of::<T>());
                } else {
                    std::ptr::copy_nonoverlapping(src, dst, WK);
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
            std::ptr::copy_nonoverlapping(src, dst, WK);
            *self.lens.get_unchecked_mut(p) = (l + WK) as u32;
        } else {
            self.over.get_unchecked_mut(p).extend_from_slice(std::slice::from_raw_parts(src, WK));
        }
    }
    /// Flush staged records; must be called before reading.
    fn finish(&mut self) {
        unsafe {
            for p in 0..self.lens.len() {
                let n = self.slen[p] as usize;
                if n == 0 {
                    continue;
                }
                let l = self.lens[p] as usize;
                let src = self.mem.as_ptr().add(self.sbase + p * WK);
                if l + n <= self.cap {
                    std::ptr::copy_nonoverlapping(src, self.mem.as_mut_ptr().add(p * self.cap + l), n);
                    self.lens[p] = (l + n) as u32;
                } else {
                    self.over[p].extend_from_slice(std::slice::from_raw_parts(src, n));
                }
                self.slen[p] = 0;
            }
            #[cfg(target_arch = "x86_64")]
            std::arch::x86_64::_mm_sfence();
        }
    }
    #[inline(always)]
    fn main(&self, p: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.mem.as_ptr().add(p * self.cap), self.lens[p] as usize) }
    }
    #[inline(always)]
    fn over(&self, p: usize) -> &[T] {
        &self.over[p]
    }
    fn len(&self, p: usize) -> usize {
        self.lens[p] as usize + self.over[p].len()
    }
}

/// Bytes per phase-2 output chunk: a multiple of 192, so chunks hold whole flushes of both
/// output record types and every flush is whole cache lines.
const CHUNK: usize = 7680;
const SLAB: usize = 2 << 20;

/// Chunk memory for phase-2 output. Regions of the phase-1 buffers are handed over once they
/// have been fully read (they are already faulted in); fresh slabs cover the start-up.
struct Pool {
    free: Vec<(*mut u8, usize)>,
    slabs: Vec<Vec<u8>>,
}
unsafe impl Send for Pool {}

impl Pool {
    fn new() -> Self {
        Pool { free: Vec::new(), slabs: Vec::new() }
    }
    /// Hand over `bytes` at `p`; the caller guarantees nobody reads or writes them any more
    /// and that they stay allocated for as long as the chunks are used.
    fn give(&mut self, p: *mut u8, bytes: usize) {
        let a = (p as usize + 63) & !63;
        let pad = a - p as usize;
        if bytes >= pad + CHUNK {
            self.free.push((a as *mut u8, bytes - pad));
        }
    }
    /// A 64-byte-aligned chunk of CHUNK bytes (most recently released memory first).
    #[inline(never)]
    fn take(&mut self) -> *mut u8 {
        loop {
            if let Some(l) = self.free.last_mut() {
                if l.1 >= CHUNK {
                    let r = l.0;
                    l.0 = unsafe { r.add(CHUNK) };
                    l.1 -= CHUNK;
                    return r;
                }
                self.free.pop();
                continue;
            }
            let mut v: Vec<u8> = Vec::with_capacity(SLAB);
            let p = v.as_mut_ptr();
            self.slabs.push(v);
            self.give(p, SLAB);
        }
    }
}

/// Per-thread partitioned output made of pool chunks; pushes are staged W at a time and
/// flushed as whole cache lines like `PartBuf`.
struct ChunkBuf<T: Copy, const W: usize> {
    stage: Vec<T>,
    slen: Vec<u32>,
    cur: Vec<*mut T>,
    room: Vec<u32>,
    /// chunks per partition: (start, records); the last one's count is fixed up by `finish`
    segs: Vec<Vec<(*const T, u32)>>,
}
unsafe impl<T: Copy + Send, const W: usize> Send for ChunkBuf<T, W> {}
unsafe impl<T: Copy + Sync, const W: usize> Sync for ChunkBuf<T, W> {}

impl<T: Copy, const W: usize> ChunkBuf<T, W> {
    const CH: usize = CHUNK / std::mem::size_of::<T>();
    fn new(nparts: usize) -> Self {
        assert!(CHUNK % (W * std::mem::size_of::<T>()) == 0 && (W * std::mem::size_of::<T>()) % 64 == 0);
        assert!(std::mem::align_of::<T>() <= 64);
        ChunkBuf {
            stage: Vec::with_capacity(nparts * W),
            slen: vec![0; nparts],
            cur: vec![std::ptr::null_mut(); nparts],
            room: vec![0; nparts],
            segs: (0..nparts).map(|_| Vec::new()).collect(),
        }
    }
    /// Branch-free conditional push: the record is always staged, but only kept if `keep`.
    #[inline(always)]
    fn push_if(&mut self, pool: &mut Pool, p: usize, v: T, keep: bool) {
        unsafe {
            let n = *self.slen.get_unchecked(p) as usize;
            self.stage.as_mut_ptr().add(p * W + n).write(v);
            let n2 = n + keep as usize;
            if n2 < W {
                *self.slen.get_unchecked_mut(p) = n2 as u32;
            } else {
                *self.slen.get_unchecked_mut(p) = 0;
                self.flush_full(pool, p);
            }
        }
    }
    #[inline(always)]
    unsafe fn open(&mut self, pool: &mut Pool, p: usize) {
        let c = pool.take() as *mut T;
        *self.cur.get_unchecked_mut(p) = c;
        *self.room.get_unchecked_mut(p) = Self::CH as u32;
        self.segs.get_unchecked_mut(p).push((c as *const T, Self::CH as u32));
    }
    #[inline(always)]
    unsafe fn flush_full(&mut self, pool: &mut Pool, p: usize) {
        if *self.room.get_unchecked(p) == 0 {
            self.open(pool, p);
        }
        let src = self.stage.as_ptr().add(p * W);
        let dst = *self.cur.get_unchecked(p);
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        nt_copy(src as *const u8, dst as *mut u8, W * std::mem::size_of::<T>());
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        std::ptr::copy_nonoverlapping(src, dst, W);
        *self.cur.get_unchecked_mut(p) = dst.add(W);
        *self.room.get_unchecked_mut(p) -= W as u32;
    }
    /// Flush staged records; must be called before reading, and no pushes may follow.
    fn finish(&mut self, pool: &mut Pool) {
        unsafe {
            for p in 0..self.slen.len() {
                let n = self.slen[p] as usize;
                if n != 0 {
                    if self.room[p] == 0 {
                        self.open(pool, p);
                    }
                    std::ptr::copy_nonoverlapping(self.stage.as_ptr().add(p * W), self.cur[p], n);
                    self.cur[p] = self.cur[p].add(n);
                    self.room[p] -= n as u32;
                    self.slen[p] = 0;
                }
                let room = self.room[p];
                if let Some(l) = self.segs[p].last_mut() {
                    l.1 = Self::CH as u32 - room;
                }
            }
            #[cfg(target_arch = "x86_64")]
            std::arch::x86_64::_mm_sfence();
        }
        self.stage = Vec::new();
    }
    fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        self.segs[p].iter().map(|&(s, n)| unsafe { std::slice::from_raw_parts(s, n as usize) })
    }
    fn len(&self, p: usize) -> usize {
        self.segs[p].iter().map(|x| x.1 as usize).sum()
    }
}

/// CPUs this process may run on (read once, before any thread is pinned).
static CPUS: OnceLock<Vec<usize>> = OnceLock::new();

fn init_cpus() {
    CPUS.get_or_init(|| {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
                return (0..libc::CPU_SETSIZE as usize).filter(|&c| libc::CPU_ISSET(c, &set)).collect();
            }
        }
        Vec::new()
    });
}

/// Pin worker t to the t-th allowed CPU (keeps its staging buffers and partitions in one core's caches).
fn pin_thread(t: usize) {
    #[cfg(target_os = "linux")]
    {
        let cpus = match CPUS.get() {
            Some(c) => c,
            None => return,
        };
        if cpus.len() < 2 || std::env::var_os("SS_NOPIN").is_some() {
            return;
        }
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpus[t % cpus.len()], &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = t;
}

/// Release big structures on several threads at once (process teardown is otherwise serial).
fn par_drop<T: Send>(items: Vec<T>) {
    std::thread::scope(|s| {
        for x in items {
            s.spawn(move || drop(x));
        }
    });
}

// ---------------------------------------------------------------- logging
static T0: OnceLock<Instant> = OnceLock::new();
static LOG_ON: OnceLock<bool> = OnceLock::new();
/// With SS_LOG on Linux: log resident and huge-page anonymous memory.
fn log_mem(tag: &str) {
    #[cfg(target_os = "linux")]
    {
        if *LOG_ON.get_or_init(|| std::env::var_os("SS_LOG").is_some()) {
            let sm = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
            let pick = |k: &str| sm.lines().find(|l| l.starts_with(k)).map(|l| l.split_whitespace().nth(1).unwrap_or("0").to_string()).unwrap_or_default();
            log(&format!("mem {tag}: rss {} kB, anon {} kB, anon-huge {} kB", pick("Rss:"), pick("Anonymous:"), pick("AnonHugePages:")));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = tag;
}

fn log(msg: &str) {
    if *LOG_ON.get_or_init(|| std::env::var_os("SS_LOG").is_some()) {
        let t = T0.get().unwrap().elapsed().as_secs_f64() * 1000.0;
        eprintln!("[{t:8.1} ms] {msg}");
    }
}

// ---------------------------------------------------------------- string refs
// A string inside a leaked mmap, packed into 64 bits: 47-bit pointer, 17-bit length.
// Lengths >= LONG go through a registry.
const LONG: u64 = (1 << 17) - 1;
static LONG_STRS: Mutex<Vec<&'static [u8]>> = Mutex::new(Vec::new());

#[inline]
fn sref(s: &'static [u8]) -> u64 {
    let len = s.len() as u64;
    if len < LONG {
        (s.as_ptr() as u64) | (len << 47)
    } else {
        let mut g = LONG_STRS.lock().unwrap();
        g.push(s);
        ((g.len() - 1) as u64) | (LONG << 47)
    }
}

#[inline]
fn sget(r: u64) -> &'static [u8] {
    let len = r >> 47;
    let p = r & ((1u64 << 47) - 1);
    if len < LONG {
        unsafe { std::slice::from_raw_parts(p as *const u8, len as usize) }
    } else {
        LONG_STRS.lock().unwrap()[p as usize]
    }
}

// ---------------------------------------------------------------- CSV
/// Split a line into exactly N fields per the rules.md dialect. Returns false if malformed.
#[inline(always)]
fn split_fields<const N: usize>(line: &'static [u8], out: &mut [&'static [u8]; N]) -> bool {
    let mut k = 0usize;
    let mut s = 0usize;
    let mut quotes = false;
    let n = line.len();
    let mut i = 0usize;
    while i < n {
        let c = line[i];
        if c == b',' {
            if k + 1 >= N {
                return false;
            }
            out[k] = &line[s..i];
            k += 1;
            s = i + 1;
        } else if c == b'"' {
            quotes = true;
        }
        i += 1;
    }
    if k != N - 1 {
        return false;
    }
    out[N - 1] = &line[s..n];
    if quotes {
        for f in out.iter_mut() {
            let v: &'static [u8] = f;
            if !v.is_empty() && v[0] == b'"' {
                if v.len() < 2 || v[v.len() - 1] != b'"' {
                    return false;
                }
                let inner = &v[1..v.len() - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                *f = inner;
            } else if memchr::memchr(b'"', v).is_some() {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------- SIMD line scanner
/// Masks of '\n', ',' and '"' for 64 bytes at p (all 64 bytes must be readable).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let m = |x: __m256i, y: __m256i| -> u64 {
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64;
        lo
    };
    let n = m(a, nl) | (m(b, nl) << 32);
    let c = m(a, cm) | (m(b, cm) << 32);
    let q = m(a, qt) | (m(b, qt) << 32);
    (n, c, q)
}

/// Line callback: (line_start, line_end, comma offsets relative to line_start, comma count, quote count).
/// Calls f for every line that starts in [start, end) of data. Lines are split on '\n' only; a final
/// line without '\n' is a line.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt,bmi1")]
unsafe fn scan_lines<F: FnMut(usize, usize, &[u32; 8], usize, u32)>(data: &[u8], start: usize, end: usize, f: &mut F) {
    let len = data.len();
    let mut pos = start;
    if pos > 0 && *data.get_unchecked(pos - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos += i + 1,
            None => return,
        }
    }
    if pos >= end || pos >= len {
        return;
    }
    let mut line_start = pos;
    let mut seps = [0u32; 8];
    let mut nsep = 0usize;
    let mut qcount = 0u32;
    let mut base = pos;
    let mut pad: [u8; 64];
    loop {
        let (mn, mc, mq) = if base + 64 <= len {
            masks64(data.as_ptr().add(base))
        } else {
            let rem = len - base;
            pad = [0u8; 64];
            pad[..rem].copy_from_slice(&data[base..]);
            masks64(pad.as_ptr())
        };
        let mut sep = mn | mc;
        let mut consumed = 0u32; // bits below belong to finished lines
        while sep != 0 {
            let b = sep.trailing_zeros();
            sep &= sep - 1;
            if (mn >> b) & 1 != 0 {
                let below = if b == 0 { 0 } else { u64::MAX >> (64 - b) };
                let from = if consumed == 0 { u64::MAX } else { !(u64::MAX >> (64 - consumed)) };
                let lq = qcount + (mq & below & from).count_ones();
                let le = base + b as usize;
                f(line_start, le, &seps, nsep, lq);
                line_start = le + 1;
                nsep = 0;
                qcount = 0;
                consumed = b + 1;
                if line_start >= end || line_start >= len {
                    return;
                }
            } else {
                if nsep < 8 {
                    *seps.get_unchecked_mut(nsep) = (base + b as usize - line_start) as u32;
                }
                nsep += 1;
            }
        }
        let from = if consumed == 0 { u64::MAX } else if consumed >= 64 { 0 } else { !(u64::MAX >> (64 - consumed)) };
        qcount += (mq & from).count_ones();
        base += 64;
        if base >= len {
            // last line without trailing newline
            if line_start < len {
                f(line_start, len, &seps, nsep, qcount);
            }
            return;
        }
    }
}

/// Line-at-a-time kernel: for each line starting in [start, end), find its newline, commas and
/// quotes from 128-byte bitmask windows, resolve exactly N fields (quotes stripped, branch-free
/// validation from the quote mask) and call f. Malformed lines are skipped. Lines of 128+ bytes
/// take the exact scalar path.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn scan_fields<const N: usize, F: FnMut(&[&'static [u8]; N])>(
    data: &'static [u8],
    start: usize,
    end: usize,
    f: &mut F,
) {
    let len = data.len();
    let mut s = start;
    if s > 0 && *data.get_unchecked(s - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s += i + 1,
            None => return,
        }
    }
    let mut pad: [u8; 128];
    let pf = PF_DIST.load(Ordering::Relaxed);
    while s < end && s < len {
        let avail = len - s;
        if pf != 0 && avail > pf + 64 {
            prefetch(data.as_ptr().add(s + pf));
        }
        let p: *const u8 = if avail >= 128 {
            data.as_ptr().add(s)
        } else {
            pad = [0u8; 128];
            pad[..avail].copy_from_slice(&data[s..]);
            pad.as_ptr()
        };
        let (n0, c0, q0) = masks64(p);
        let e: usize;
        let cm: u128;
        let qm: u128;
        if n0 != 0 {
            let ee = n0.trailing_zeros();
            let below = (1u64 << ee) - 1;
            e = ee as usize;
            cm = (c0 & below) as u128;
            qm = (q0 & below) as u128;
        } else {
            let (n1, c1, q1) = masks64(p.add(64));
            if n1 != 0 {
                let ee = n1.trailing_zeros();
                let below = (1u64 << ee) - 1;
                e = 64 + ee as usize;
                cm = (c0 as u128) | (((c1 & below) as u128) << 64);
                qm = (q0 as u128) | (((q1 & below) as u128) << 64);
            } else if avail < 128 {
                // final line of the file, no trailing newline
                e = avail;
                cm = (c0 as u128) | ((c1 as u128) << 64);
                qm = (q0 as u128) | ((q1 as u128) << 64);
            } else {
                // long line: exact scalar path
                let le = match memchr::memchr(b'\n', &data[s..]) {
                    Some(i) => s + i,
                    None => len,
                };
                let mut fl: [&'static [u8]; N] = [&[]; N];
                if split_fields(&data[s..le], &mut fl) {
                    f(&fl);
                }
                s = le + 1;
                continue;
            }
        }
        if cm.count_ones() as usize == N - 1 {
            let base = data.as_ptr().add(s);
            let mut fl: [&'static [u8]; N] = [&[]; N];
            let mut bits = cm;
            let mut st = 0usize;
            let q = qm.count_ones();
            if q == 0 {
                for k in 0..N {
                    let en = if k + 1 < N {
                        let c = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        c
                    } else {
                        e
                    };
                    *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st), en - st);
                    st = en + 1;
                }
                f(&fl);
            } else {
                let mut expect = 0u32;
                let mut bad = 0u32;
                for k in 0..N {
                    let en = if k + 1 < N {
                        let c = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        c
                    } else {
                        e
                    };
                    let n = en - st;
                    let qd = ((qm >> st) & 1) as u32;
                    let ql = ((qm >> (en.wrapping_sub(1) & 127)) & 1) as u32;
                    bad |= qd & (((n < 2) as u32) | (ql ^ 1));
                    expect += 2 * qd;
                    let o = qd as usize;
                    *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st + o), n.saturating_sub(2 * o));
                    st = en + 1;
                }
                if bad == 0 && expect == q {
                    f(&fl);
                }
            }
        }
        s += e + 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
unsafe fn scan_fields_px<const N: usize, F: FnMut(&[&'static [u8]; N])>(
    data: &'static [u8],
    start: usize,
    end: usize,
    f: &mut F,
) {
    let len = data.len();
    let mut s = start;
    if s > 0 && *data.get_unchecked(s - 1) != b'\n' {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s += i + 1,
            None => return,
        }
    }
    let mut pad: [u8; 128];
    let pf = PF_DIST.load(Ordering::Relaxed);
    while s < end && s < len {
        let avail = len - s;
        if pf != 0 && avail > pf + 64 {
            prefetch(data.as_ptr().add(s + pf));
        }
        let p: *const u8 = if avail >= 128 {
            data.as_ptr().add(s)
        } else {
            pad = [0u8; 128];
            pad[..avail].copy_from_slice(&data[s..]);
            pad.as_ptr()
        };
        let (n0, c0, q0) = masks64(p);
        let e: usize;
        let cm: u128;
        let qm: u128;
        if N >= 7 && avail >= 128 && !SCAN_BRANCHY.load(Ordering::Relaxed) {
            // event lines straddle the 64-byte mark unpredictably: take both windows, no branch
            let (n1, c1, q1) = masks64(p.add(64));
            let nl = (n0 as u128) | ((n1 as u128) << 64);
            if nl == 0 {
                let le = match memchr::memchr(b'\n', &data[s..]) {
                    Some(i) => s + i,
                    None => len,
                };
                let mut fl: [&'static [u8]; N] = [&[]; N];
                if split_fields(&data[s..le], &mut fl) {
                    f(&fl);
                }
                s = le + 1;
                continue;
            }
            let ee = nl.trailing_zeros();
            e = ee as usize;
            // keep the bits below the newline (bzhi leaves the word whole for an index >= 64)
            use std::arch::x86_64::_bzhi_u64;
            let hi = ee.saturating_sub(64);
            cm = (_bzhi_u64(c0, ee) as u128) | ((_bzhi_u64(c1, hi) as u128) << 64);
            qm = (_bzhi_u64(q0, ee) as u128) | ((_bzhi_u64(q1, hi) as u128) << 64);
        } else if n0 != 0 {
            let ee = n0.trailing_zeros();
            let below = (1u64 << ee) - 1;
            e = ee as usize;
            cm = (c0 & below) as u128;
            qm = (q0 & below) as u128;
        } else {
            let (n1, c1, q1) = masks64(p.add(64));
            if n1 != 0 {
                let ee = n1.trailing_zeros();
                let below = (1u64 << ee) - 1;
                e = 64 + ee as usize;
                cm = (c0 as u128) | (((c1 & below) as u128) << 64);
                qm = (q0 as u128) | (((q1 & below) as u128) << 64);
            } else if avail < 128 {
                // final line of the file, no trailing newline
                e = avail;
                cm = (c0 as u128) | ((c1 as u128) << 64);
                qm = (q0 as u128) | ((q1 as u128) << 64);
            } else {
                // long line: exact scalar path
                let le = match memchr::memchr(b'\n', &data[s..]) {
                    Some(i) => s + i,
                    None => len,
                };
                let mut fl: [&'static [u8]; N] = [&[]; N];
                if split_fields(&data[s..le], &mut fl) {
                    f(&fl);
                }
                s = le + 1;
                continue;
            }
        }
        let clo = cm as u64;
        let chi = (cm >> 64) as u64;
        let plo = clo.count_ones();
        if (plo + chi.count_ones()) as usize == N - 1 && (clo & 1) == 0 && e > 0 {
            use std::arch::x86_64::_pext_u64;
            let base = data.as_ptr().add(s);
            // comma offsets: the first N-1 set bits of the low word, then those of the high word
            // written from slot plo on (surplus slots hold junk that is overwritten or unused)
            let mut pos = [0usize; 16];
            let mut m = clo;
            for k in 0..N - 1 {
                *pos.get_unchecked_mut(k) = m.trailing_zeros() as usize;
                m &= m.wrapping_sub(1);
            }
            let mut m = chi;
            let pl = plo as usize;
            let quick = if N - 1 < 3 { N - 1 } else { 3 };
            for k in 0..quick {
                *pos.get_unchecked_mut(pl + k) = 64 + m.trailing_zeros() as usize;
                m &= m.wrapping_sub(1);
            }
            if m != 0 {
                for k in quick..N - 1 {
                    *pos.get_unchecked_mut(pl + k) = 64 + m.trailing_zeros() as usize;
                    m &= m.wrapping_sub(1);
                }
            }
            *pos.get_unchecked_mut(N - 1) = e;
            // Quotes, one path for every line. In comma order: field k+1 opens with a quote iff a
            // quote follows comma k, field k closes with one iff a quote precedes comma k; the first
            // field's opening and the last field's closing quote are read directly. The line is
            // well formed iff every field opens and closes alike, each quoted field holds both of
            // its quotes (length >= 2), and the line has no other quote (count == 2 per quoted field).
            let qlo = qm as u64;
            let qhi = (qm >> 64) as u64;
            let opens = _pext_u64((qlo >> 1) | (qhi << 63), clo) | (_pext_u64(qhi >> 1, chi) << plo);
            let closes = _pext_u64(qlo << 1, clo) | (_pext_u64((qhi << 1) | (qlo >> 63), chi) << plo);
            let last = (*p.add(e - 1) == b'"') as u64;
            let fq = (opens << 1) | (qlo & 1);
            let mut fl: [&'static [u8]; N] = [&[]; N];
            let mut st = 0usize;
            let mut neg = 0usize;
            for k in 0..N {
                let en = *pos.get_unchecked(k);
                let o = ((fq >> k) & 1) as usize;
                let n = (en - st).wrapping_sub(2 * o);
                neg |= n;
                *fl.get_unchecked_mut(k) = std::slice::from_raw_parts(base.add(st + o), n);
                st = en + 1;
            }
            if fq == (closes | (last << (N - 1))) && (neg as isize) >= 0 && qlo.count_ones() + qhi.count_ones() == 2 * fq.count_ones() {
                f(&fl);
            }
        }
        s += e + 1;
    }
}

/// Resolve N fields of a scanned line. Returns false if malformed.
#[inline(always)]
fn fields_from_scan<const N: usize>(
    data: &'static [u8],
    ls: usize,
    le: usize,
    seps: &[u32; 8],
    nsep: usize,
    q: u32,
    out: &mut [&'static [u8]; N],
) -> bool {
    if nsep != N - 1 {
        return false;
    }
    let mut s = ls;
    for k in 0..N {
        let e = if k + 1 < N { ls + seps[k] as usize } else { le };
        out[k] = unsafe { data.get_unchecked(s..e) };
        s = e + 1;
    }
    if q != 0 {
        let mut expect = 0u32;
        for fl in out.iter_mut() {
            let v: &'static [u8] = fl;
            if !v.is_empty() && v[0] == b'"' {
                if v.len() < 2 || v[v.len() - 1] != b'"' {
                    return false;
                }
                expect += 2;
                *fl = &v[1..v.len() - 1];
            }
        }
        if expect != q {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------- timestamps
#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as u32 * 10 + c as u32)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// "YYYY-MM-DDTHH:MM:SS" (first 19 bytes) -> seconds since epoch
#[inline(always)]
fn parse_dt(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = (d2(b, 0)? * 100 + d2(b, 2)?) as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(days_from_civil(y, mo, d) * 86400 + (h * 3600 + mi * 60 + s) as i64)
}

/// Event/click ts -> epoch ms, None if bad form.
#[inline(always)]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return Some(parse_dt(b)? * 1000);
    }
    if b[0].is_ascii_digit() {
        // F3 if all digits
        let mut v: i64 = 0;
        let mut all = true;
        let mut overflow = false;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                all = false;
                break;
            }
            if !overflow {
                match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
                    Some(x) => v = x,
                    None => overflow = true,
                }
            }
        }
        if all {
            return if overflow { None } else { Some(v) };
        }
    }
    if n == 25 {
        let sign = b[19];
        if (sign != b'+' && sign != b'-') || b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 3600 + om * 60) as i64;
        let local = parse_dt(b)?;
        let utc = if sign == b'+' { local - off } else { local + off };
        return Some(utc * 1000);
    }
    None
}

// ---- fast timestamp paths (exact; anything unusual falls back to the slow parsers above)
#[inline(always)]
fn ld8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// "HH:MM:SS" at b[i..i+8] -> seconds of day (validated)
#[inline(always)]
fn hms(b: &[u8], i: usize) -> Option<i64> {
    let w = ld8(b, i);
    const HM: u64 = 0xF0F0_00F0_F000_F0F0;
    const EX: u64 = 0x3030_0030_3000_3030;
    const SEPM: u64 = 0x0000_FF00_00FF_0000;
    const SEPV: u64 = 0x0000_3A00_003A_0000;
    if (w & SEPM) != SEPV || (w & HM) != EX || ((w.wrapping_add(0x0606_0006_0600_0606)) & HM) != EX {
        return None;
    }
    let d = w - EX;
    let h = (d & 0xFF) * 10 + ((d >> 8) & 0xFF);
    let m = ((d >> 24) & 0xFF) * 10 + ((d >> 32) & 0xFF);
    let s = ((d >> 48) & 0xFF) * 10 + ((d >> 56) & 0xFF);
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some((h * 3600 + m * 60 + s) as i64)
}

/// 8 ASCII digits (first digit most significant) -> value
#[inline(always)]
fn digits8(w: u64) -> Option<u64> {
    const H: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const Z: u64 = 0x3030_3030_3030_3030;
    if (w & H) != Z || ((w.wrapping_add(0x0606_0606_0606_0606)) & H) != Z {
        return None;
    }
    let mut v = w - Z;
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    Some(v)
}

/// Months around the window: "YYYY-MM-" prefix (LE u64) -> (epoch day of the 1st, days in month)
struct DateTab {
    pre: [u64; 8],
    day0: [i64; 8],
    dim: [u32; 8],
    n: usize,
    // the same months, direct-mapped by month number mod 16 (empty slot: pre 0, dim 0)
    pre16: [u64; 16],
    day16: [i64; 16], // epoch day of the 1st, minus 1
    dim16: [u32; 16],
}

impl DateTab {
    fn new(lo_ms: i64, hi_ms: i64) -> Self {
        let mut t = DateTab { pre: [0; 8], day0: [0; 8], dim: [0; 8], n: 0, pre16: [0; 16], day16: [0; 16], dim16: [0; 16] };
        let civil = |days: i64| -> (i64, u32, u32) {
            // inverse of days_from_civil
            let z = days + 719468;
            let era = if z >= 0 { z } else { z - 146096 } / 146097;
            let doe = z - era * 146097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
            let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
            (if m <= 2 { y + 1 } else { y }, m, d)
        };
        let mut day = lo_ms.div_euclid(DAY_MS);
        let last = hi_ms.div_euclid(DAY_MS);
        while day <= last && t.n < 8 {
            let (y, m, _) = civil(day);
            if (0..=9999).contains(&y) {
                let s = format!("{:04}-{:02}-", y, m);
                let p = u64::from_le_bytes(s.as_bytes()[..8].try_into().unwrap());
                if !t.pre[..t.n].contains(&p) {
                    t.pre[t.n] = p;
                    t.day0[t.n] = days_from_civil(y, m, 1);
                    t.dim[t.n] = days_in_month(y, m);
                    let k = (m & 15) as usize;
                    if t.dim16[k] == 0 {
                        t.pre16[k] = p;
                        t.day16[k] = t.day0[t.n] - 1;
                        t.dim16[k] = t.dim[t.n];
                    }
                    t.n += 1;
                }
            }
            day += 1;
        }
        t
    }
    /// Epoch day of "YYYY-MM-DD" at b[0..10]: Some(Some(day)) valid, Some(None) invalid, None = not in table.
    #[inline(always)]
    fn day(&self, b: &[u8]) -> Option<Option<i64>> {
        let p = ld8(b, 0);
        for k in 0..self.n {
            if self.pre[k] == p {
                let dd = match d2(b, 8) {
                    Some(v) => v,
                    None => return Some(None),
                };
                if dd < 1 || dd > self.dim[k] {
                    return Some(None);
                }
                return Some(Some(self.day0[k] + dd as i64 - 1));
            }
        }
        None
    }
}

// ---- lean F1 kernel for the common case. It reports ok only for a field it has fully validated;
// everything else (other forms, dates outside the table, any bad value) goes to the exact parsers.
/// "HH:MM:SS" as a LE word -> (valid, seconds of day)
#[inline(always)]
fn hms_k(w: u64) -> (bool, i64) {
    const M1: u64 = 0xF0F0_FFF0_F0FF_F0F0; // digit high nibbles and the two separators
    const V1: u64 = 0x3030_3A30_303A_3030;
    const HM: u64 = 0xF0F0_00F0_F000_F0F0;
    const EX: u64 = 0x3030_0030_3000_3030;
    let fmt = ((w & M1) == V1) & ((w.wrapping_add(0x0606_0006_0600_0606) & HM) == EX);
    let d = w & 0x0F0F_000F_0F00_0F0F;
    // bytes 0, 3, 6 = HH, MM, SS
    let u = d.wrapping_mul(10).wrapping_add(d >> 8) & 0x00FF_0000_FF00_00FF;
    // HH <= 23, MM <= 59, SS <= 59: adding (127 - limit) must not reach bit 7
    let rng = (u.wrapping_add(0x0044_0000_4400_0068) & 0x0080_0000_8000_0080) == 0;
    // HH*3600 + MM*60 + SS lands in bits 48..72 of the 128-bit product
    const K: u128 = (3600u128 << 48) + (60u128 << 24) + 1;
    let secs = (((u as u128) * K) >> 48) as u64 & 0xFF_FFFF;
    (fmt & rng, secs as i64)
}

/// 8 ASCII digits as a LE word (first digit most significant) -> (valid, value)
#[inline(always)]
fn digits8_k(w: u64) -> (bool, u64) {
    const H: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const Z: u64 = 0x3030_3030_3030_3030;
    let ok = ((w & H) == Z) & (((w.wrapping_add(0x0606_0606_0606_0606)) & H) == Z);
    let mut v = w.wrapping_sub(Z);
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    (ok, v)
}

impl DateTab {
    /// "YYYY-MM-" word + the two day characters -> (valid and in table, epoch day).
    /// One probe: slot = month number mod 16 (taken from the raw characters), then the whole
    /// 8-byte prefix is compared, so a wrong or garbage slot can only miss.
    #[inline(always)]
    fn day_k(&self, w0: u64, c0: u8, c1: u8) -> (bool, i64) {
        let k = ((((w0 >> 40) & 0xFF) * 10 + ((w0 >> 48) & 0xFF)) & 15) as usize;
        let d0 = c0.wrapping_sub(b'0') as u32;
        let d1 = c1.wrapping_sub(b'0') as u32;
        let dd = d0 * 10 + d1;
        let ok = (self.pre16[k] == w0) & (d0 < 10) & (d1 < 10) & (dd.wrapping_sub(1) < self.dim16[k]);
        (ok, self.day16[k] + dd as i64)
    }
}

/// Strict F1 in a 20-byte field -> (handled, epoch seconds).
#[inline(always)]
fn f1_kernel(b: &[u8], tab: &DateTab) -> (bool, i64) {
    debug_assert!(b.len() == 20);
    let (dok, day) = tab.day_k(ld8(b, 0), b[8], b[9]);
    let (hok, secs) = hms_k(ld8(b, 11));
    (dok & hok & (b[10] == b'T') & (b[19] == b'Z'), day * 86400 + secs)
}

/// F1/F2/F3 -> epoch ms, exact.
#[inline(always)]
fn parse_ts_fast(b: &[u8], tab: &DateTab) -> Option<i64> {
    let n = b.len();
    if n == 20 {
        let (ok, v) = f1_kernel(b, tab);
        if ok {
            return Some(v * 1000);
        }
        if b[19] == b'Z' && b[10] == b'T' {
            return match tab.day(b) {
                Some(Some(day)) => Some((day * 86400 + hms(b, 11)?) * 1000),
                Some(None) => None,
                None => parse_ts(b),
            };
        }
        return parse_ts(b);
    }
    if n == 13 {
        // the first 8 digits, then the last 5 (bytes 5..8 are validated by the first word)
        let (aok, a) = digits8_k(ld8(b, 0));
        let (cok, c) = digits8_k((ld8(b, 5) & !0xFF_FFFF) | 0x30_3030);
        if aok & cok {
            return Some((a * 100_000 + c) as i64);
        }
        return parse_ts(b);
    }
    if n == 25 && b[10] == b'T' && b[22] == b':' {
        let sign = b[19];
        if sign == b'+' || sign == b'-' {
            return match tab.day(b) {
                Some(Some(day)) => {
                    let oh = d2(b, 20)?;
                    let om = d2(b, 23)?;
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    let off = (oh * 3600 + om * 60) as i64;
                    let local = day * 86400 + hms(b, 11)?;
                    Some((if sign == b'+' { local - off } else { local + off }) * 1000)
                }
                Some(None) => None,
                None => parse_ts(b),
            };
        }
    }
    parse_ts(b)
}

/// ingest_ts (strict F1) -> epoch seconds, exact.
#[inline(always)]
fn parse_f1_fast(b: &[u8], tab: &DateTab) -> Option<i64> {
    if b.len() != 20 {
        return None;
    }
    let (ok, v) = f1_kernel(b, tab);
    if ok {
        return Some(v);
    }
    if b[19] != b'Z' || b[10] != b'T' {
        return None;
    }
    match tab.day(b) {
        Some(Some(day)) => Some(day * 86400 + hms(b, 11)?),
        Some(None) => None,
        None => parse_dt(b),
    }
}


#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- hash maps
#[inline(always)]
fn nz(h: u64) -> u64 {
    if h == 0 {
        1
    } else {
        h
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Open addressing u64 (nonzero) -> u32, interleaved slots (one cache line per probe).
#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    key: u64,
    val: u32,
    _pad: u32,
}

struct U64Map {
    slots: Vec<Slot>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![Slot { key: 0, val: 0, _pad: 0 }; cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    fn slot_ptr(&self, k: u64) -> *const Slot {
        unsafe { self.slots.as_ptr().add((k as usize) & self.mask) }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.key == k {
                return Some(s.val);
            }
            if s.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// returns (value, inserted)
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.key == k {
                return (s.val, false);
            }
            if s.key == 0 {
                s.key = k;
                s.val = v;
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![Slot { key: 0, val: 0, _pad: 0 }; cap];
        self.mask = cap - 1;
        for s in old {
            if s.key != 0 {
                let mut i = (s.key as usize) & self.mask;
                while self.slots[i].key != 0 {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = s;
            }
        }
    }
}

/// Exact string -> u32 map (verifies bytes).
struct StrMap {
    hashes: Vec<u64>,
    keys: Vec<&'static [u8]>,
    vals: Vec<u32>,
    mask: usize,
}

impl StrMap {
    fn new(items: &[(&'static [u8], u32)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let mut m = StrMap { hashes: vec![0; cap], keys: vec![&[][..]; cap], vals: vec![0; cap], mask: cap - 1 };
        for &(k, v) in items {
            let h = nz(xxh3_64(k));
            let mut i = (h as usize) & m.mask;
            loop {
                if m.hashes[i] == 0 {
                    m.hashes[i] = h;
                    m.keys[i] = k;
                    m.vals[i] = v;
                    break;
                }
                if m.hashes[i] == h && m.keys[i] == k {
                    m.vals[i] = v; // last wins
                    break;
                }
                i = (i + 1) & m.mask;
            }
        }
        m
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = nz(xxh3_64(k));
        let mut i = (h as usize) & self.mask;
        loop {
            let hh = self.hashes[i];
            if hh == 0 {
                return None;
            }
            if hh == h && self.keys[i] == k {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- input files
static MAPS: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());
static PROF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static PF_DIST: AtomicUsize = AtomicUsize::new(1024);
static SCAN_BRANCHY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
// debug accumulators (ns, summed over threads)
static TACC: [std::sync::atomic::AtomicU64; 8] = [const { std::sync::atomic::AtomicU64::new(0) }; 8];
#[inline(always)]
fn tadd(i: usize, t: Instant) {
    TACC[i].fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
}


/// Unmap inputs and release memory on all threads, then leave without the serial teardown.
fn fast_exit(nthreads: usize) -> ! {
    #[cfg(target_os = "linux")]
    {
        let maps = std::mem::take(&mut *MAPS.lock().unwrap());
        let next = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= maps.len() {
                        break;
                    }
                    unsafe {
                        libc::munmap(maps[i].0 as *mut libc::c_void, maps[i].1);
                    }
                });
            }
        });
        unsafe { libc::_exit(0) }
    }
    #[allow(unreachable_code)]
    {
        let _ = nthreads;
        std::process::exit(0)
    }
}
fn map_file(p: &Path) -> &'static [u8] {
    let f = match fs::File::open(p) {
        Ok(f) => f,
        Err(_) => return &[],
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    match unsafe { memmap2::Mmap::map(&f) } {
        Ok(m) => {
            // The scorer's /data mount is cold for each run. Ask the kernel to start reading the
            // whole file now; parsing then faults in pages that are already in flight instead of
            // paying the read latency inside the parse loop.
            #[cfg(target_os = "linux")]
            unsafe {
                libc::madvise(m.as_ptr() as *mut libc::c_void, m.len(), libc::MADV_WILLNEED);
            }
            let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
            MAPS.lock().unwrap().push((m.as_ptr() as usize, m.len()));
            &m[..]
        }
        Err(_) => {
            let v = fs::read(p).unwrap_or_default();
            Box::leak(v.into_boxed_slice())
        }
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

/// Body of a CSV (after the header line).
fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Event,
    Click,
    Link,
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: Kind,
    data: &'static [u8], // file body
    start: usize,
    end: usize,
}

fn make_chunks(kind: Kind, files: &[&'static [u8]], target: usize, out: &mut Vec<Chunk>) {
    for &f in files {
        let b = body(f);
        if b.is_empty() {
            continue;
        }
        let n = b.len();
        let parts = (n + target - 1) / target;
        for i in 0..parts {
            out.push(Chunk { kind, data: b, start: i * n / parts, end: (i + 1) * n / parts });
        }
    }
}

/// Iterate lines that START in [start, end) of data.
#[inline(always)]
fn for_lines(data: &'static [u8], start: usize, end: usize, mut f: impl FnMut(&'static [u8])) {
    let mut pos = start;
    if pos > 0 && data[pos - 1] != b'\n' {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos += i + 1,
            None => return,
        }
    }
    while pos < end {
        let nl = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        f(&data[pos..nl]);
        pos = nl + 1;
    }
}

// ---------------------------------------------------------------- records
/// Event copy, partitioned by event_id hash.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvA {
    key: u64,  // event_id fingerprint (low 64 bits of xxh3_128; partition uses high bits)
    user: u32, // dense user index
    ts: u32,   // ms since base (ws - 7d); u32::MAX = invalid copy
    ingest: u32,
    meta: u32, // price (24 bits) | cat << 24 ; price 0 = not a purchase
}

/// Surviving event, partitioned by person range.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvB {
    person: u32,
    ts: u32,
    meta: u32,
}

/// Valid click (before person mapping).
#[derive(Clone, Copy)]
#[repr(C)]
struct ClA {
    cid: u64, // sref
    user: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
}

/// Valid click, partitioned by person range.
#[derive(Clone, Copy)]
#[repr(C)]
struct ClB {
    cid: u64,
    person: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
}

struct Ctx {
    base: i64,  // ws - 7d (ms)
    ev_lo: u32, // ws - base
    ev_hi: u32, // we - base
    cl_hi: u32, // we - base
    ingest_base: i64, // seconds
    products: StrMap,
    campaigns: StrMap,
    unknown_cat: u32,
    dates: DateTab,
    pshift: u32,
}

#[inline(always)]
fn part_of(h: u64, shift: u32) -> usize {
    (h >> shift) as usize
}

const RING: usize = 16;

/// Per-thread parse state: output buffers, user-index allocator, and small pipelines that
/// prefetch a row's user-table slot and resolve it RING rows later.
struct PState {
    ev: PartBuf<EvA>,
    cl: PartBuf<ClA>, // single partition; write-combined like the event scatter
    alloc: IdxAlloc,
    pe: [(EvA, u64, u64, u32); RING],
    pe_pos: usize,
    pc: [(ClA, u64, u64); RING],
    pc_pos: usize,
}

impl PState {
    fn new(npart: usize, per_part: usize, cl_cap: usize) -> Self {
        let ze = EvA { key: 0, user: 0, ts: 0, ingest: 0, meta: 0 };
        let zc = ClA { cid: 0, user: 0, ts: 0, camp: 0, _pad: 0 };
        PState {
            ev: PartBuf::new(npart, per_part),
            cl: PartBuf::new(1, cl_cap),
            alloc: IdxAlloc { next: 0, end: 0 },
            pe: [(ze, 0, 0, 0); RING],
            pe_pos: 0,
            pc: [(zc, 0, 0); RING],
            pc_pos: 0,
        }
    }
    #[inline(always)]
    fn ev_pending(&mut self, ut: &UTab, rec: EvA, uid_h: u64, uref: u64, part: u32) {
        prefetch(ut.slot_ptr(uid_h));
        let i = self.pe_pos & (RING - 1);
        if self.pe_pos >= RING {
            let (mut r, h, s, p) = self.pe[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.ev.push(p as usize, r);
        }
        self.pe[i] = (rec, uid_h, uref, part);
        self.pe_pos += 1;
    }
    #[inline(always)]
    fn cl_pending(&mut self, ut: &UTab, rec: ClA, uid_h: u64, uref: u64) {
        prefetch(ut.slot_ptr(uid_h));
        let i = self.pc_pos & (RING - 1);
        if self.pc_pos >= RING {
            let (mut r, h, s) = self.pc[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.cl.push(0, r);
        }
        self.pc[i] = (rec, uid_h, uref);
        self.pc_pos += 1;
    }
    fn flush(&mut self, ut: &UTab) {
        let n = self.pe_pos.min(RING);
        for k in 0..n {
            let i = (self.pe_pos - n + k) & (RING - 1);
            let (mut r, h, s, p) = self.pe[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.ev.push(p as usize, r);
        }
        self.pe_pos = 0;
        let n = self.pc_pos.min(RING);
        for k in 0..n {
            let i = (self.pc_pos - n + k) & (RING - 1);
            let (mut r, h, s) = self.pc[i];
            r.user = ut.get_or_insert(h, s, &mut self.alloc);
            self.cl.push(0, r);
        }
        self.pc_pos = 0;
    }
}

#[inline(always)]
fn parse_event_line(line: &'static [u8], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let mut f: [&'static [u8]; 7] = [&[]; 7];
    if !split_fields(line, &mut f) {
        return;
    }
    event_fields(&f, cx, st, ut);
}

#[inline(always)]
fn event_fields(f: &[&'static [u8]; 7], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let eid = f[0];
    if eid.is_empty() {
        return;
    }
    let h128 = xxh3_128(eid);
    let key = h128 as u64;
    let part = part_of((h128 >> 64) as u64, cx.pshift);
    let ingest_s = parse_f1_fast(f[3], &cx.dates);
    let ingest = match ingest_s {
        Some(s) => (s - cx.ingest_base).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut rec = EvA { key, user: 0, ingest, ts: u32::MAX, meta: 0 };
    'valid: {
        let uid = f[1];
        if uid.is_empty() {
            break 'valid;
        }
        let ts = match parse_ts_fast(f[2], &cx.dates) {
            Some(t) => t,
            None => break 'valid,
        };
        let rel = ts - cx.base;
        if rel < cx.ev_lo as i64 || rel >= cx.ev_hi as i64 {
            break 'valid;
        }
        let is_purchase = match f[4] {
            b"view" | b"cart" => false,
            b"purchase" => true,
            _ => break 'valid,
        };
        let prod = f[5];
        if prod.is_empty() {
            break 'valid;
        }
        let mut meta = 0u32;
        if is_purchase {
            let price = match parse_price(f[6]) {
                Some(p) => p,
                None => break 'valid,
            };
            let cat = cx.products.get(prod).unwrap_or(cx.unknown_cat);
            meta = price | (cat << 24);
        }
        let ingest_s = match ingest_s {
            Some(s) => s,
            None => break 'valid,
        };
        if ingest_s * 1000 - ts > LATE_MS {
            break 'valid;
        }
        rec.ts = rel as u32;
        rec.meta = meta;
        st.ev_pending(ut, rec, nz(xxh3_64(uid)), sref(uid), part as u32);
        return;
    }
    st.ev.push(part, rec);
}

#[inline(always)]
fn parse_click_line(line: &'static [u8], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let mut f: [&'static [u8]; 4] = [&[]; 4];
    if !split_fields(line, &mut f) {
        return;
    }
    click_fields(&f, cx, st, ut);
}

#[inline(always)]
fn click_fields(f: &[&'static [u8]; 4], cx: &Ctx, st: &mut PState, ut: &UTab) {
    let cid = f[0];
    if cid.is_empty() {
        return;
    }
    let uid = f[1];
    if uid.is_empty() {
        return;
    }
    let ts = match parse_ts_fast(f[3], &cx.dates) {
        Some(t) => t,
        None => return,
    };
    let rel = ts - cx.base;
    if rel < 0 || rel >= cx.cl_hi as i64 {
        return;
    }
    let camp = match cx.campaigns.get(f[2]) {
        Some(c) => c,
        None => return,
    };
    st.cl_pending(ut, ClA { cid: sref(cid), user: 0, ts: rel as u32, camp, _pad: 0 }, nz(xxh3_64(uid)), sref(uid));
}

// ---------------------------------------------------------------- small CSV tables
fn read_table2(p: &Path) -> Vec<(&'static [u8], &'static [u8])> {
    let data = map_file(p);
    let b = body(data);
    let mut v = Vec::new();
    for_lines(b, 0, b.len(), |line| {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        if split_fields(line, &mut f) {
            v.push((f[0], f[1]));
        }
    });
    v
}

fn manifest_value(text: &str, key: &str) -> Option<String> {
    let i = text.find(&format!("\"{key}\""))?;
    let rest = &text[i + key.len() + 2..];
    let c = rest.find(':')?;
    let rest = &rest[c + 1..];
    let q1 = rest.find('"')?;
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"')?;
    Some(rest[..q2].to_string())
}

// ---------------------------------------------------------------- users and links
/// Zero-initialised vector (T must be valid as all-zero bytes). Big ones come from mmap lazily.
fn zeroed_vec<T>(n: usize) -> Vec<T> {
    if n == 0 {
        return Vec::new();
    }
    unsafe {
        let layout = std::alloc::Layout::array::<T>(n).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Vec::from_raw_parts(p, n, n)
    }
}

#[repr(C)]
struct USlot {
    key: AtomicU64,
    idx: AtomicU32, // user index + 1; 0 = being published
    _pad: u32,
}

const UBLOCK: u32 = 1024;
const MAXPROBE: usize = 256;

struct IdxAlloc {
    next: u32,
    end: u32,
}

/// Concurrent, insert-only table: uid hash -> dense user index (linear probing, CAS on the key).
/// Keys whose probe window is full go to a locked spill list (never expected in practice).
struct UTab {
    slots: Vec<USlot>,
    mask: usize,
    blocks: AtomicU32,
    uref: Vec<AtomicU64>, // user index -> sref of the uid text
    spill: Mutex<std::collections::HashMap<u64, u32>>,
}

impl UTab {
    fn new(cap_slots: usize, cap_users: usize) -> Self {
        UTab {
            slots: zeroed_vec(cap_slots),
            mask: cap_slots - 1,
            blocks: AtomicU32::new(0),
            uref: zeroed_vec(cap_users),
            spill: Mutex::new(std::collections::HashMap::new()),
        }
    }
    #[inline(always)]
    fn slot_ptr(&self, h: u64) -> *const USlot {
        unsafe { self.slots.as_ptr().add((h as usize) & self.mask) }
    }
    #[inline(always)]
    fn new_index(&self, a: &mut IdxAlloc) -> u32 {
        if a.next == a.end {
            let b = self.blocks.fetch_add(1, Ordering::Relaxed);
            a.next = b * UBLOCK;
            a.end = a.next + UBLOCK;
        }
        let i = a.next;
        a.next += 1;
        assert!((i as usize) < self.uref.len(), "user table overflow");
        i
    }
    #[inline(always)]
    fn wait(slot: &USlot) -> u32 {
        loop {
            let v = slot.idx.load(Ordering::Acquire);
            if v != 0 {
                return v - 1;
            }
            std::hint::spin_loop();
        }
    }
    #[inline(always)]
    fn get_or_insert(&self, h: u64, s: u64, a: &mut IdxAlloc) -> u32 {
        let mut i = (h as usize) & self.mask;
        for _ in 0..MAXPROBE {
            let slot = unsafe { self.slots.get_unchecked(i) };
            let k = slot.key.load(Ordering::Acquire);
            if k == h {
                return Self::wait(slot);
            }
            if k == 0 {
                match slot.key.compare_exchange(0, h, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        let u = self.new_index(a);
                        self.uref[u as usize].store(s, Ordering::Relaxed);
                        slot.idx.store(u + 1, Ordering::Release);
                        return u;
                    }
                    Err(actual) => {
                        if actual == h {
                            return Self::wait(slot);
                        }
                    }
                }
            }
            i = (i + 1) & self.mask;
        }
        self.spill_get(h, s, a)
    }
    #[cold]
    fn spill_get(&self, h: u64, s: u64, a: &mut IdxAlloc) -> u32 {
        let mut g = self.spill.lock().unwrap();
        if let Some(&u) = g.get(&h) {
            return u;
        }
        let u = self.new_index(a);
        self.uref[u as usize].store(s, Ordering::Relaxed);
        g.insert(h, u);
        u
    }
    /// Upper bound on user indices handed out.
    fn n_users(&self) -> usize {
        self.blocks.load(Ordering::Relaxed) as usize * UBLOCK as usize
    }
    #[inline(always)]
    fn name(&self, u: u32) -> u64 {
        self.uref[u as usize].load(Ordering::Relaxed)
    }
}

/// A parsed link line waiting for its user-table slots: both side hashes, string refs and
/// leading-bytes keys.
#[derive(Clone, Copy)]
#[repr(C)]
struct LinkRec {
    ha: u64,
    hb: u64,
    a: u64,
    b: u64,
    ka: u64,
    kb: u64,
}

/// A link as user indices, with the first 8 bytes of each ID (big-endian, zero padded) so the
/// union-find orders most names without touching their text.
#[derive(Clone, Copy)]
#[repr(C)]
struct Edge {
    a: u32,
    b: u32,
    ka: u64,
    kb: u64,
}

#[inline(always)]
fn key8(s: &[u8]) -> u64 {
    if s.len() >= 8 {
        return u64::from_be(unsafe { std::ptr::read_unaligned(s.as_ptr() as *const u64) });
    }
    let mut buf = [0u8; 8];
    buf[..s.len()].copy_from_slice(s);
    u64::from_be_bytes(buf)
}

const LRING: usize = 16;

/// Per-chunk link pipeline: a parsed line prefetches its two user-table slots and is interned
/// LRING lines later.
struct LinkSt {
    ring: [LinkRec; LRING],
    pos: usize,
    edges: Vec<Edge>,
}

impl LinkSt {
    fn new(cap: usize) -> Self {
        LinkSt { ring: [LinkRec { ha: 0, hb: 0, a: 0, b: 0, ka: 0, kb: 0 }; LRING], pos: 0, edges: Vec::with_capacity(cap) }
    }
    #[inline(always)]
    fn resolve(&mut self, i: usize, ut: &UTab, al: &mut IdxAlloc) {
        let r = self.ring[i];
        let ua = ut.get_or_insert(r.ha, r.a, al);
        let ub = ut.get_or_insert(r.hb, r.b, al);
        self.edges.push(Edge { a: ua, b: ub, ka: r.ka, kb: r.kb });
    }
    #[inline(always)]
    fn line(&mut self, f: &[&'static [u8]; 2], ut: &UTab, al: &mut IdxAlloc) {
        let (a, b) = (f[0], f[1]);
        if a.is_empty() || b.is_empty() {
            return;
        }
        let ha = nz(xxh3_64(a));
        let hb = nz(xxh3_64(b));
        prefetch(ut.slot_ptr(ha));
        prefetch(ut.slot_ptr(hb));
        let i = self.pos & (LRING - 1);
        if self.pos >= LRING {
            self.resolve(i, ut, al);
        }
        self.ring[i] = LinkRec { ha, hb, a: sref(a), b: sref(b), ka: key8(a), kb: key8(b) };
        self.pos += 1;
    }
    /// Intern the lines still in the ring, in line order, and hand back the chunk's edges.
    fn finish(mut self, ut: &UTab, al: &mut IdxAlloc) -> Vec<Edge> {
        let n = self.pos.min(LRING);
        for k in 0..n {
            let i = (self.pos - n + k) & (LRING - 1);
            self.resolve(i, ut, al);
        }
        self.edges
    }
}

/// Union-find over user indices. parent[u] = 0 for a root (and for every user in no link),
/// else root + 1 (flattened). Each root is the byte-wise smallest ID of its person.
/// `n_linked` bounds the user indices that can appear in an edge.
fn build_dsu(edges: &[Vec<Edge>], ut: &UTab, cap_users: usize, n_linked: usize) -> Vec<u32> {
    let mut parent: Vec<u32> = zeroed_vec(cap_users);
    let n_linked = n_linked.min(cap_users);
    // key8 of every root that has absorbed another ID; a root that has not is an endpoint of
    // the edge at hand, which carries its key
    let mut rkey: Vec<u64> = zeroed_vec(n_linked);
    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        unsafe {
            loop {
                let p = *parent.get_unchecked(x as usize);
                if p == 0 {
                    return x;
                }
                let px = p - 1;
                let gp = *parent.get_unchecked(px as usize);
                if gp != 0 {
                    *parent.get_unchecked_mut(x as usize) = gp;
                }
                x = px;
            }
        }
    }
    let mut total = 0usize;
    for v in edges {
        let m = v.len();
        total += m;
        for i in 0..m {
            // two-stage prefetch: the endpoints' entries 16 edges ahead, their parents' 8 ahead
            if i + 16 < m {
                let e = v[i + 16];
                prefetch(unsafe { parent.as_ptr().add(e.a as usize) });
                prefetch(unsafe { parent.as_ptr().add(e.b as usize) });
            }
            if i + 8 < m {
                let e = v[i + 8];
                // an entry is 0 (prefetches slot 0, harmless) or root/parent + 1
                let pa = parent[e.a as usize] as usize;
                let pb = parent[e.b as usize] as usize;
                prefetch(unsafe { parent.as_ptr().add(pa.saturating_sub(1)) });
                prefetch(unsafe { parent.as_ptr().add(pb.saturating_sub(1)) });
            }
            let e = v[i];
            let ra = find(&mut parent, e.a);
            let rb = find(&mut parent, e.b);
            if ra != rb {
                let ka = if ra == e.a { e.ka } else { rkey[ra as usize] };
                let kb = if rb == e.b { e.kb } else { rkey[rb as usize] };
                let a_smaller = if ka != kb { ka < kb } else { sget(ut.name(ra)) < sget(ut.name(rb)) };
                // which side wins is a coin flip: select, don't branch
                let (win, lose, kw) = if a_smaller { (ra, rb, ka) } else { (rb, ra, kb) };
                parent[lose as usize] = win + 1;
                rkey[win as usize] = kw;
            }
        }
    }
    // flatten: in index order when the linked users are dense, else by edge endpoint
    if n_linked <= total.saturating_mul(8) {
        for u in 0..n_linked as u32 {
            if (u as usize) + 16 < n_linked {
                let q = parent[u as usize + 16] as usize;
                prefetch(unsafe { parent.as_ptr().add(q.saturating_sub(1)) });
            }
            // most entries are roots or already point at one; both cases fall through untaken
            let p = parent[u as usize];
            let g = parent[p.max(1) as usize - 1];
            if (g != 0) & (p != 0) {
                let r = find(&mut parent, u);
                parent[u as usize] = r + 1;
            }
        }
    } else {
        for v in edges {
            for e in v.iter() {
                for u in [e.a, e.b] {
                    let r = find(&mut parent, u);
                    if r != u {
                        parent[u as usize] = r + 1;
                    }
                }
            }
        }
    }
    parent
}

#[inline(always)]
fn person_of(parent: &[u32], u: u32) -> u32 {
    let p = unsafe { *parent.get_unchecked(u as usize) };
    if p == 0 {
        u
    } else {
        p - 1
    }
}

// ---------------------------------------------------------------- per-person processing
/// user_daily rows, stored as the PLAIN-encoded Parquet page bodies of their columns so the
/// file is written without a separate encode pass.
#[derive(Default)]
struct UserCols {
    name: Vec<u8>, // per row: u32 length + bytes
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top: Vec<u32>, // u32::MAX = null
}

/// Credited-campaign accumulator for one (campaign, day).
#[derive(Clone, Copy)]
#[repr(C)]
struct CStat {
    sessions: u32,
    purchasing: u32,
    revenue: u64,
}

/// Click as stored per person: sorted by ts
#[derive(Clone, Copy)]
#[repr(C)]
struct ClC {
    cid: u64,
    ts: u32,
    camp: u32,
}

/// One 30-minute cell of the current person. Resting state (between persons) is CELL0, so the
/// per-event update needs no "first event of the cell" branch.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct Cell {
    min: u32,
    max: u32,
    cnt: u32,
    sess: u32, // session index of the cell (valid for the cells of the current person)
    rev: u64,
}
const CELL0: Cell = Cell { min: u32::MAX, max: 0, cnt: 0, sess: 0, rev: 0 };

/// One session of the current person.
#[derive(Clone, Copy)]
#[repr(C)]
struct Sess {
    start: u32,
    day: u32,
    rev: u64,
    run: u32, // index into runs
    _pad: u32,
}

struct Worker {
    // compact, cache-friendly campaign accumulators over event-window days only (added to the
    // shared export table by finalize_stats)
    cstat: Vec<CStat>,
    casg: Vec<u32>,
    nd: usize,
    dshift: u32,
    ndays: usize,
    ncat: usize,
    aend: Vec<u64>, // per campaign: person stamp << 32 | end of its last assist range
    stamp: u32,
    sess: Vec<Sess>, // sessions of the current person: [0, sess_n)
    sess_n: usize,
    runs: Vec<(u32, u32)>, // (day, first session idx)
    rows: UserCols,
    // grouping scratch
    pmap: U64Map,
    pname: Vec<u64>,
    pnh: Vec<u64>,
    pcount: Vec<u32>,
    pcl: Vec<u32>,
    pidx: Vec<u32>,
    ev_sorted: Vec<u64>,
    cl_sorted: Vec<ClC>,
    cl_pidx: Vec<u32>,
    cl_a: Vec<u32>, // click radix scratch: positions in the chunk list, in key order
    rhist: Vec<u32>,
    cl_hi: u32, // exclusive bound of click ts
    // 30-minute cells (sessions can only break between non-empty cells)
    cells: Vec<Cell>,
    cbits: Vec<u64>,
    purch: Vec<u64>, // purchase events of the current person
    dcat: Vec<u64>,  // [day * ncat + cat] purchase revenue
}

impl Worker {
    fn new(ncamp: usize, ndays: usize, ncat: usize) -> Self {
        Worker {
            cstat: Vec::new(),
            casg: Vec::new(),
            nd: 0,
            dshift: 0,
            ndays,
            ncat,
            aend: vec![0; ncamp + 1],
            stamp: 0,
            sess: Vec::new(),
            sess_n: 0,
            runs: Vec::new(),
            rows: UserCols::default(),
            pmap: U64Map::with_capacity(1024),
            pname: Vec::new(),
            pnh: Vec::new(),
            pcount: Vec::new(),
            pcl: Vec::new(),
            pidx: Vec::new(),
            ev_sorted: Vec::new(),
            cl_sorted: Vec::new(),
            cl_pidx: Vec::new(),
            cl_a: Vec::new(),
            rhist: Vec::new(),
            cl_hi: u32::MAX,
            cells: Vec::new(),
            cbits: Vec::new(),
            purch: Vec::new(),
            dcat: vec![0; ndays * ncat],
        }
    }

    /// Event-window day range: base-relative day of the window's first day, and its day count.
    fn set_window(&mut self, ncamp: usize, dshift: u32, nd: usize) {
        self.dshift = dshift;
        self.nd = nd;
        self.cstat = vec![CStat { sessions: 0, purchasing: 0, revenue: 0 }; (ncamp + 1) * nd];
        self.casg = vec![0; (ncamp + 1) * nd];
    }

    /// Add the compact accumulators to the export table used by the writer:
    /// [camp * ndays + day] -> sessions, purchasing, revenue, assisted.
    fn finalize_stats(&self, stats: &mut [[u64; 4]]) {
        let nd = self.nd;
        let ncamp1 = self.cstat.len() / nd.max(1);
        for c in 0..ncamp1 {
            for d in 0..nd {
                let cs = self.cstat[c * nd + d];
                let a = self.casg[c * nd + d];
                if cs.sessions != 0 || a != 0 {
                    let st = &mut stats[c * self.ndays + d + self.dshift as usize];
                    st[0] += cs.sessions as u64;
                    st[1] += cs.purchasing as u64;
                    st[2] += cs.revenue;
                    st[3] += a as u64;
                }
            }
        }
    }

    fn ensure_cells(&mut self, ncells: usize) {
        if self.cells.len() < ncells {
            self.cells = vec![CELL0; ncells];
            // a session starts at a non-empty cell, so there are at most ncells of them
            self.sess = vec![Sess { start: 0, day: 0, rev: 0, run: 0, _pad: 0 }; ncells + 1];
            self.cbits = vec![0; (ncells + 63) / 64];
        }
    }

    /// Sessions from unsorted events (ts << 32 | meta) via 30-minute cells; fills sess and runs,
    /// and emits the user_daily rows. With `direct` (the person has no clicks) every session is
    /// organic and the campaign stats are written here, one update per day.
    fn sessionize_cells(&mut self, name: u64, evs: &[u64], base: i64, base_day_off: i64, direct: bool, organic: usize) {
        self.runs.clear();
        if self.purch.len() <= evs.len() {
            self.purch.resize(evs.len() + 1, 0);
        }
        let cells = self.cells.as_mut_ptr();
        let bits = self.cbits.as_mut_ptr();
        let purch = self.purch.as_mut_ptr();
        let sess = self.sess.as_mut_ptr();
        let mut np = 0usize;
        let mut cmin_c = u32::MAX;
        let mut cmax_c = 0u32;
        let mut wmask = 0u64; // non-empty bitmap words (exact when there are at most 64 words)
        for &e in evs {
            let ts = (e >> 32) as u32;
            let price = e as u32 & 0xFF_FFFF;
            let c = ts / GAP_MS;
            unsafe {
                let cell = &mut *cells.add(c as usize);
                cell.min = cell.min.min(ts);
                cell.max = cell.max.max(ts);
                cell.cnt += 1;
                cell.rev += price as u64;
                *bits.add((c >> 6) as usize) |= 1u64 << (c & 63);
                *purch.add(np) = e;
            }
            np += (price != 0) as usize;
            wmask |= 1u64 << ((c >> 6) & 63);
            cmin_c = cmin_c.min(c);
            cmax_c = cmax_c.max(c);
        }
        // walk cells in time order
        let row0 = self.rows.day.len();
        let nm = sget(name);
        // Row storage is reserved once per person and written through raw pointers. A day row
        // needs a session start on that day, and the cells span cells.len() / 48 + 2 days at most.
        let max_rows = evs.len().min(self.cells.len() / 48 + 2);
        let nstep = 4 + nm.len();
        // length prefix + name, copied per row as one fixed 64-byte block when it fits
        let mut nbuf = [0u8; 64];
        let nfast = nm.len() <= 60;
        if nfast {
            nbuf[..4].copy_from_slice(&(nm.len() as u32).to_le_bytes());
            nbuf[4..nstep].copy_from_slice(nm);
        }
        self.rows.name.reserve(max_rows * nstep + 64);
        self.rows.day.reserve(max_rows);
        self.rows.sessions.reserve(max_rows);
        self.rows.events.reserve(max_rows);
        self.rows.revenue.reserve(max_rows);
        self.rows.top.reserve(max_rows);
        let p_name = unsafe { self.rows.name.as_mut_ptr().add(self.rows.name.len()) };
        let p_day = unsafe { self.rows.day.as_mut_ptr().add(row0) };
        let p_sessions = unsafe { self.rows.sessions.as_mut_ptr().add(row0) };
        let p_events = unsafe { self.rows.events.as_mut_ptr().add(row0) };
        let p_revenue = unsafe { self.rows.revenue.as_mut_ptr().add(row0) };
        let p_top = unsafe { self.rows.top.as_mut_ptr().add(row0) };
        let mut nr = 0usize; // rows written
        let org = organic * self.nd;
        let mut k = 0usize; // sessions so far
        let mut prev_max: u32 = 0;
        let mut day: u32 = 0;
        let mut day_lim: i64 = i64::MIN; // first ts after the current day
        let mut s_rev = 0u64; // revenue of the open session
        let mut r_first = 0usize; // open day run: first session, events, revenue, purchasing sessions
        let mut r_events = 0u32;
        let mut r_rev = 0u64;
        let mut r_purch = 0u32;
        macro_rules! flush_run {
            () => {
                debug_assert!(nr < max_rows);
                // SAFETY: nr < max_rows rows were reserved above (the name column with 64
                // bytes of slack for the fixed-size copy)
                unsafe {
                    let d = p_name.add(nr * nstep);
                    if nfast {
                        std::ptr::copy_nonoverlapping(nbuf.as_ptr(), d, 64);
                    } else {
                        std::ptr::copy_nonoverlapping((nm.len() as u32).to_le_bytes().as_ptr(), d, 4);
                        std::ptr::copy_nonoverlapping(nm.as_ptr(), d.add(4), nm.len());
                    }
                    *p_day.add(nr) = (base_day_off + day as i64) as i32;
                    *p_sessions.add(nr) = (k - r_first) as i32;
                    *p_events.add(nr) = r_events as i32;
                    *p_revenue.add(nr) = r_rev as i64;
                    *p_top.add(nr) = u32::MAX;
                }
                nr += 1;
                if direct {
                    let st = unsafe { self.cstat.get_unchecked_mut(org + (day - self.dshift) as usize) };
                    st.sessions += (k - r_first) as u32;
                    st.purchasing += r_purch;
                    st.revenue += r_rev;
                }
            };
        }
        let small = self.cbits.len() <= 64;
        let mut w_next = (cmin_c >> 6) as usize;
        let w_last = (cmax_c >> 6) as usize;
        loop {
            let w;
            if small {
                if wmask == 0 {
                    break;
                }
                w = wmask.trailing_zeros() as usize;
                wmask &= wmask - 1;
            } else {
                if w_next > w_last {
                    break;
                }
                w = w_next;
                w_next += 1;
            }
            let mut b = unsafe { *bits.add(w) };
            unsafe { *bits.add(w) = 0 };
            while b != 0 {
                let c = (w << 6) | b.trailing_zeros() as usize;
                b &= b - 1;
                let cell = unsafe { &mut *cells.add(c) };
                let lo = cell.min;
                if k == 0 || lo - prev_max > GAP_MS {
                    if k > 0 {
                        unsafe { (*sess.add(k - 1)).rev = s_rev };
                        r_rev += s_rev;
                        r_purch += (s_rev > 0) as u32;
                    }
                    if lo as i64 >= day_lim {
                        if k > 0 {
                            flush_run!();
                        }
                        let ad = (base + lo as i64).div_euclid(DAY_MS);
                        day = (ad - base_day_off) as u32;
                        day_lim = (ad + 1) * DAY_MS - base;
                        self.runs.push((day, k as u32));
                        r_first = k;
                        r_events = 0;
                        r_rev = 0;
                        r_purch = 0;
                    }
                    unsafe {
                        *sess.add(k) = Sess { start: lo, day, rev: 0, run: (self.runs.len() - 1) as u32, _pad: 0 };
                    }
                    k += 1;
                    s_rev = 0;
                }
                s_rev += cell.rev;
                r_events += cell.cnt;
                prev_max = cell.max;
                *cell = Cell { min: u32::MAX, max: 0, cnt: 0, sess: (k - 1) as u32, rev: 0 };
            }
        }
        if k > 0 {
            unsafe { (*sess.add(k - 1)).rev = s_rev };
            r_rev += s_rev;
            r_purch += (s_rev > 0) as u32;
            flush_run!();
        }
        // SAFETY: nr rows were initialised above, within the reserved capacity
        unsafe {
            let nl = self.rows.name.len();
            self.rows.name.set_len(nl + nr * nstep);
            self.rows.day.set_len(row0 + nr);
            self.rows.sessions.set_len(row0 + nr);
            self.rows.events.set_len(row0 + nr);
            self.rows.revenue.set_len(row0 + nr);
            self.rows.top.set_len(row0 + nr);
        }
        self.sess_n = k;
        // purchases -> top category of the session's day
        if np == 1 {
            let e = self.purch[0];
            let ts = (e >> 32) as u32;
            let r = unsafe { (*sess.add((*cells.add((ts / GAP_MS) as usize)).sess as usize)).run } as usize;
            self.rows.top[row0 + r] = e as u32 >> 24;
        } else if np > 1 {
            let ncat = self.ncat;
            for i in 0..np {
                let e = self.purch[i];
                let meta = e as u32;
                let ts = (e >> 32) as u32;
                let d = unsafe { (*sess.add((*cells.add((ts / GAP_MS) as usize)).sess as usize)).day } as usize;
                self.dcat[d * ncat + (meta >> 24) as usize] += (meta & 0xFF_FFFF) as u64;
            }
            for r in 0..self.runs.len() {
                if self.rows.revenue[row0 + r] > 0 {
                    let day = self.runs[r].0 as usize;
                    let row = &mut self.dcat[day * ncat..(day + 1) * ncat];
                    let mut best = 0u64;
                    let mut top = u32::MAX;
                    for (ci, v) in row.iter_mut().enumerate() {
                        if *v > best {
                            best = *v;
                            top = ci as u32;
                        }
                        *v = 0;
                    }
                    self.rows.top[row0 + r] = top;
                }
            }
        }
    }

    #[inline]
    fn close(&mut self, c: usize, k0: usize, k1: usize) {
        if k0 >= k1 {
            return;
        }
        let mut r = self.sess[k0].run as usize;
        let nruns = self.runs.len();
        while r < nruns {
            let (day, first) = self.runs[r];
            let first = first as usize;
            if first >= k1 {
                break;
            }
            let end = if r + 1 < nruns { self.runs[r + 1].1 as usize } else { self.sess_n };
            let lo = k0.max(first);
            let hi = k1.min(end);
            if hi > lo {
                unsafe {
                    *self.casg.get_unchecked_mut(c * self.nd + (day - self.dshift) as usize) += (hi - lo) as u32;
                }
            }
            r += 1;
        }
    }

    /// Process one partition of persons: person indices [qbase, qbase + qsize).
    fn partition(
        &mut self,
        evp: &[&[EvB]],
        clp: &[&[ClB]],
        qbase: u32,
        qsize: usize,
        base: i64,
        base_day_off: i64,
        organic: usize,
        ut: &UTab,
    ) {
        let nev: usize = evp.iter().map(|s| s.len()).sum();
        if nev == 0 {
            return;
        }
        let t_g = Instant::now();
        // counting sort of events by person
        self.pcount.clear();
        self.pcount.resize(qsize, 0);
        for s in evp.iter().take(PF_AHEAD) {
            prefetch_slice(s);
        }
        for (i, s) in evp.iter().enumerate() {
            if let Some(nx) = evp.get(i + PF_AHEAD) {
                prefetch_slice(nx);
            }
            for e in s.iter() {
                unsafe { *self.pcount.get_unchecked_mut((e.person - qbase) as usize) += 1 };
            }
        }
        self.pidx.clear();
        let mut acc = 0u32;
        for c in self.pcount.iter_mut() {
            self.pidx.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        self.pidx.push(acc);
        self.ev_sorted.clear();
        self.ev_sorted.resize(nev, 0);
        for s in evp {
            for e in s.iter() {
                let p = (e.person - qbase) as usize;
                unsafe {
                    let w = *self.pcount.get_unchecked(p) as usize;
                    *self.pcount.get_unchecked_mut(p) += 1;
                    *self.ev_sorted.get_unchecked_mut(w) = ((e.ts as u64) << 32) | e.meta as u64;
                }
            }
        }
        // clicks by person
        let ncl: usize = clp.iter().map(|s| s.len()).sum();
        // a click's position in the chunk list as one u32: chunk << jbits | index in the chunk
        let jmax = clp.iter().map(|s| s.len()).max().unwrap_or(0);
        let jbits = usize::BITS - jmax.leading_zeros();
        let radix = ncl >= CL_RADIX_MIN && ((clp.len() as u64) << jbits) <= u32::MAX as u64;
        self.pcl.clear();
        self.pcl.resize(qsize, 0);
        // radix key: the top CL_RBITS bits of ts (every click ts is below cl_hi)
        let sh = (32 - self.cl_hi.leading_zeros()).saturating_sub(CL_RBITS);
        if radix {
            // one pass: per-person counts and the key histogram
            self.rhist.clear();
            self.rhist.resize(1 << CL_RBITS, 0);
            let h = self.rhist.as_mut_ptr();
            for s in clp.iter().take(PF_AHEAD) {
                prefetch_slice(s);
            }
            for (i, s) in clp.iter().enumerate() {
                if let Some(nx) = clp.get(i + PF_AHEAD) {
                    prefetch_slice(nx);
                }
                for c in s.iter() {
                    unsafe {
                        *self.pcl.get_unchecked_mut((c.person - qbase) as usize) += 1;
                        *h.add((c.ts >> sh) as usize) += 1;
                    }
                }
            }
        } else {
            for s in clp {
                for c in s.iter() {
                    unsafe { *self.pcl.get_unchecked_mut((c.person - qbase) as usize) += 1 };
                }
            }
        }
        self.cl_pidx.clear();
        let mut acc = 0u32;
        for c in self.pcl.iter_mut() {
            self.cl_pidx.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        self.cl_pidx.push(acc);
        self.cl_sorted.clear();
        self.cl_sorted.resize(acc as usize, ClC { cid: 0, ts: 0, camp: 0 });
        if radix {
            // One stable counting pass over the partition's clicks on the top bits of ts, then
            // the stable scatter by person: each person's clicks come out ordered by ts up to
            // the dropped low bits, which fix_clicks repairs. The counting pass moves 4-byte
            // positions; the records themselves are gathered once, by the scatter by person.
            // every slot of the buffer is written below (the cursors are exact prefix sums)
            self.cl_a.clear();
            self.cl_a.reserve(ncl);
            unsafe { self.cl_a.set_len(ncl) };
            let mut acc = 0u32;
            for c in self.rhist.iter_mut() {
                let n = *c;
                *c = acc;
                acc += n;
            }
            let h = self.rhist.as_mut_ptr();
            for (ci, s) in clp.iter().enumerate() {
                let pos0 = (ci as u32) << jbits;
                for (j, c) in s.iter().enumerate() {
                    unsafe {
                        let d = h.add((c.ts >> sh) as usize);
                        *self.cl_a.get_unchecked_mut(*d as usize) = pos0 | j as u32;
                        *d += 1;
                    }
                }
            }
            let jmask = (1u32 << jbits) - 1;
            let at = |pos: u32| unsafe {
                clp.get_unchecked((pos >> jbits) as usize).as_ptr().add((pos & jmask) as usize)
            };
            for (i, &x) in self.cl_a.iter().enumerate() {
                unsafe {
                    if i + 8 < ncl {
                        prefetch(at(*self.cl_a.get_unchecked(i + 8)));
                    }
                    let c = &*at(x);
                    let d = self.pcl.get_unchecked_mut((c.person - qbase) as usize);
                    *self.cl_sorted.get_unchecked_mut(*d as usize) = ClC { cid: c.cid, ts: c.ts, camp: c.camp };
                    *d += 1;
                }
            }
        } else {
            for s in clp {
                for c in s.iter() {
                    let p = (c.person - qbase) as usize;
                    unsafe {
                        let w = *self.pcl.get_unchecked(p) as usize;
                        *self.pcl.get_unchecked_mut(p) += 1;
                        *self.cl_sorted.get_unchecked_mut(w) = ClC { cid: c.cid, ts: c.ts, camp: c.camp };
                    }
                }
            }
        }
        tadd(3, t_g);
        let t_p = Instant::now();
        let ev_sorted = std::mem::take(&mut self.ev_sorted);
        let mut cl_sorted = std::mem::take(&mut self.cl_sorted);
        let starts = std::mem::take(&mut self.pidx);
        let cstarts = std::mem::take(&mut self.cl_pidx);
        for p in 0..qsize {
            let (e0, e1) = (starts[p] as usize, starts[p + 1] as usize);
            if e0 == e1 {
                continue;
            }
            let cls = &mut cl_sorted[cstarts[p] as usize..cstarts[p + 1] as usize];
            if radix {
                fix_clicks(cls);
            } else {
                sort_clicks(cls);
            }
            let name = ut.name(qbase + p as u32);
            // the name bytes (in the input text) are copied into the user_daily column
            if let Some(r) = ut.uref.get(qbase as usize + p + 6) {
                prefetch((r.load(Ordering::Relaxed) & ((1u64 << 47) - 1)) as *const u8);
            }
            self.person(name, &ev_sorted[e0..e1], cls, base, base_day_off, organic);
        }
        self.ev_sorted = ev_sorted;
        self.cl_sorted = cl_sorted;
        self.pidx = starts;
        self.cl_pidx = cstarts;
        tadd(4, t_p);
    }

    /// evs: one person's events (ts << 32 | meta), unsorted; cls: clicks sorted by ts.
    fn person(&mut self, name: u64, evs: &[u64], cls: &[ClC], base: i64, base_day_off: i64, organic: usize) {
        let direct = cls.is_empty();
        if PROF.load(Ordering::Relaxed) {
            let t = Instant::now();
            self.sessionize_cells(name, evs, base, base_day_off, direct, organic);
            tadd(5, t);
            if !direct {
                let t = Instant::now();
                self.attribute(cls, organic);
                tadd(6, t);
            }
        } else {
            self.sessionize_cells(name, evs, base, base_day_off, direct, organic);
            if !direct {
                self.attribute(cls, organic);
            }
        }
    }

    fn attribute(&mut self, cls: &[ClC], organic: usize) {
        let s_n = self.sess_n;
        let nc = cls.len();
        let nd = self.nd;
        let dshift = self.dshift;
        // Assists. Click i (at t) assists the sessions starting in [t, t + 7d]: the index range
        // [a, b), and both ends only move forward with i. Per campaign the ranges are merged by
        // remembering where its previous range ended (aend, valid when its stamp is this person's).
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.aend.iter_mut().for_each(|x| *x = 0);
            self.stamp = 1;
        }
        let stamp = (self.stamp as u64) << 32;
        let mut a = 0usize;
        let mut b = 0usize;
        for i in 0..nc {
            let cl = unsafe { *cls.get_unchecked(i) };
            let t = cl.ts;
            unsafe {
                while a < s_n && self.sess.get_unchecked(a).start < t {
                    a += 1;
                }
                if a == s_n {
                    break;
                }
                if b < a {
                    b = a;
                }
                let lim = t as i64 + WEEK_MS;
                while b < s_n && self.sess.get_unchecked(b).start as i64 <= lim {
                    b += 1;
                }
                if b > a {
                    let c = cl.camp as usize;
                    let e = self.aend.get_unchecked_mut(c);
                    let from = if *e >> 32 == stamp >> 32 { (*e as u32 as usize).max(a) } else { a };
                    *e = stamp | b as u64;
                    self.close(c, from, b);
                }
            }
        }
        // Credit: the latest click at or before the session start, if within 7 days; credited
        // stats are accumulated per run of equal (campaign, day).
        let mut hi = 0usize;
        let mut tie_q = usize::MAX; // last resolved group of equal-ts clicks and its campaign
        let mut tie_camp = 0usize;
        let mut a_key = usize::MAX;
        let mut a_sess = 0u32;
        let mut a_purch = 0u32;
        let mut a_rev = 0u64;
        for k in 0..s_n {
            let se = unsafe { *self.sess.get_unchecked(k) };
            let s = se.start;
            let credited = unsafe {
                while hi < nc && cls.get_unchecked(hi).ts <= s {
                    hi += 1;
                }
                if hi > 0 && cls.get_unchecked(hi - 1).ts as i64 >= s as i64 - WEEK_MS {
                    let t = cls.get_unchecked(hi - 1).ts;
                    let mut q = hi - 1;
                    while q > 0 && cls.get_unchecked(q - 1).ts == t {
                        q -= 1;
                    }
                    // cls[q..hi] share the timestamp, in no particular order: the smallest
                    // click_id wins. The ids are only compared when the campaigns differ.
                    if q + 1 == hi {
                        cls.get_unchecked(q).camp as usize
                    } else {
                        if q != tie_q {
                            tie_q = q;
                            tie_camp = tie_campaign(cls.get_unchecked(q..hi));
                        }
                        tie_camp
                    }
                } else {
                    organic
                }
            };
            let key = credited * nd + (se.day - dshift) as usize;
            if key != a_key {
                if a_sess != 0 {
                    let st = unsafe { self.cstat.get_unchecked_mut(a_key) };
                    st.sessions += a_sess;
                    st.purchasing += a_purch;
                    st.revenue += a_rev;
                }
                a_key = key;
                a_sess = 0;
                a_purch = 0;
                a_rev = 0;
            }
            a_sess += 1;
            a_purch += (se.rev > 0) as u32;
            a_rev += se.rev;
        }
        if a_sess != 0 {
            let st = unsafe { self.cstat.get_unchecked_mut(a_key) };
            st.sessions += a_sess;
            st.purchasing += a_purch;
            st.revenue += a_rev;
        }
    }
}

/// The phase-2 output chunks are small and scattered, and were written past the cache: the
/// first pass over a partition fetches each chunk this many chunks ahead of its use.
const PF_AHEAD: usize = 4;

#[inline(always)]
fn prefetch_slice<T>(s: &[T]) {
    let p = s.as_ptr() as *const u8;
    let n = std::mem::size_of_val(s);
    let mut o = 0;
    while o < n {
        prefetch(p.wrapping_add(o));
        o += 64;
    }
}

/// Bits of the click radix key.
const CL_RBITS: u32 = 11;

/// Partitions with at least this many clicks sort them by radix (see Worker::partition).
const CL_RADIX_MIN: usize = 512;

/// Campaign of the smallest click_id (bytes) among clicks of equal timestamp.
#[cold]
#[inline(never)]
fn tie_campaign(g: &[ClC]) -> usize {
    let c0 = g[0].camp;
    if g.iter().all(|c| c.camp == c0) {
        return c0 as usize;
    }
    let mut best = 0usize;
    for j in 1..g.len() {
        if sget(g[j].cid) < sget(g[best].cid) {
            best = j;
        }
    }
    g[best].camp as usize
}

/// Finish the timestamp order of one person's nearly sorted clicks: insertion with a small step
/// budget, and the general sort when an element is far from its place. Clicks of equal
/// timestamp stay in arbitrary order (attribute resolves them, see tie_campaign).
#[inline]
fn fix_clicks(cls: &mut [ClC]) {
    for i in 1..cls.len() {
        if cls[i].ts < cls[i - 1].ts {
            let x = cls[i];
            let mut j = i;
            let mut steps = 0;
            while j > 0 && x.ts < cls[j - 1].ts {
                if steps == 16 {
                    cls[j] = x;
                    cls.sort_unstable_by_key(|c| c.ts);
                    return;
                }
                cls[j] = cls[j - 1];
                j -= 1;
                steps += 1;
            }
            cls[j] = x;
        }
    }
}

/// Sort one person's clicks by ts (equal timestamps in any order). Small inputs: branch-free
/// rank sort on ts (rank = number of smaller ts); equal ts values collide on a rank and take
/// the general path.
#[inline]
fn sort_clicks(cls: &mut [ClC]) {
    const SMALL: usize = 64;
    let n = cls.len();
    if n < 2 {
        return;
    }
    if n <= SMALL {
        let mut ts = [std::mem::MaybeUninit::<u32>::uninit(); SMALL];
        let mut tmp = [std::mem::MaybeUninit::<ClC>::uninit(); SMALL];
        let m = (n + 7) & !7;
        for i in 0..n {
            ts[i].write(cls[i].ts);
            tmp[i].write(cls[i]);
        }
        for i in n..m {
            ts[i].write(u32::MAX); // never smaller than a real ts
        }
        // SAFETY: the first m (<= SMALL) entries of ts and the first n of tmp are initialised
        let ts: &[u32] = unsafe { std::slice::from_raw_parts(ts.as_ptr() as *const u32, m) };
        let tmp: &[ClC] = unsafe { std::slice::from_raw_parts(tmp.as_ptr() as *const ClC, n) };
        let mut used = 0u64;
        for i in 0..n {
            let t = ts[i];
            let mut r = 0u32;
            for ch in ts.chunks_exact(8) {
                for &x in ch {
                    r += (x < t) as u32;
                }
            }
            // r < n: at most n - 1 entries are smaller
            used |= 1u64 << r;
            unsafe { *cls.get_unchecked_mut(r as usize) = tmp[i] };
        }
        if used.count_ones() as usize == n {
            return; // all ranks distinct: a permutation, sorted by ts
        }
        cls.copy_from_slice(tmp);
    }
    cls.sort_unstable_by_key(|c| c.ts);
}

/// Sort u64 keys: insertion sort for small, radix (by the high 32 bits) for large.
fn sort_u64(v: &mut [u64]) {
    let n = v.len();
    if n <= 32 {
        for i in 1..n {
            let x = v[i];
            let mut j = i;
            while j > 0 && v[j - 1] > x {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    } else {
        v.sort_unstable();
    }
}

// ---------------------------------------------------------------- main
fn main() {
    T0.get_or_init(Instant::now);
    init_cpus();

    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let nthreads: usize = std::env::var("SS_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| std::env::var("NCPU").ok().and_then(|v| v.parse().ok()))
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(2);

    // manifest
    let mtext = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = parse_ts(manifest_value(&mtext, "window_start").expect("window_start").as_bytes()).expect("ws");
    let we = parse_ts(manifest_value(&mtext, "window_end").expect("window_end").as_bytes()).expect("we");
    let base = ws - WEEK_MS;

    // products / categories
    let prows = read_table2(&data_dir.join("products.csv"));
    // Distinct category names through a small exact hash table (sorting one name per product was
    // the slowest part of start-up); at most 256 categories, "unknown" included.
    let mut uniq: Vec<&'static [u8]> = Vec::new();
    let mut pcat: Vec<u32> = Vec::with_capacity(prows.len());
    let unk = {
        const TS: usize = 1024;
        let mut tab: Vec<u32> = vec![u32::MAX; TS];
        let mut intern = |c: &'static [u8], uniq: &mut Vec<&'static [u8]>| -> u32 {
            let mut i = (xxh3_64(c) as usize) & (TS - 1);
            loop {
                let u = tab[i];
                if u == u32::MAX {
                    assert!(uniq.len() < 256, "too many categories");
                    tab[i] = uniq.len() as u32;
                    uniq.push(c);
                    return tab[i];
                }
                if uniq[u as usize] == c {
                    return u;
                }
                i = (i + 1) & (TS - 1);
            }
        };
        for r in prows.iter() {
            pcat.push(intern(r.1, &mut uniq));
        }
        intern(b"unknown", &mut uniq)
    };
    let mut corder: Vec<u32> = (0..uniq.len() as u32).collect();
    corder.sort_by(|&a, &b| uniq[a as usize].cmp(uniq[b as usize]));
    let cats: Vec<&'static [u8]> = corder.iter().map(|&i| uniq[i as usize]).collect();
    let mut crank = vec![0u32; uniq.len()];
    for (r, &i) in corder.iter().enumerate() {
        crank[i as usize] = r as u32;
    }
    let unknown_cat = crank[unk as usize];
    let pitems: Vec<(&'static [u8], u32)> = prows.iter().zip(pcat.iter()).map(|(r, &c)| (r.0, crank[c as usize])).collect();
    let products = StrMap::new(&pitems);

    // campaigns (dedup by id, last wins)
    let crows = read_table2(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    {
        let tmp: Vec<(&'static [u8], u32)> = crows.iter().enumerate().map(|(i, r)| (r.0, i as u32)).collect();
        let m = StrMap::new(&tmp);
        for (i, r) in crows.iter().enumerate() {
            if m.get(r.0) == Some(i as u32) {
                camp_ids.push(r.0);
                camp_ch.push(r.1);
            }
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp;
    let citems: Vec<(&'static [u8], u32)> = camp_ids.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let campaigns = StrMap::new(&citems);

    let base_day_off = base.div_euclid(DAY_MS);
    let last_day = (we - 1).div_euclid(DAY_MS);
    let ndays = (last_day - base_day_off + 1) as usize;

    let cx = Ctx {
        base,
        ev_lo: (ws - base) as u32,
        ev_hi: (we - base) as u32,
        cl_hi: (we - base) as u32,
        ingest_base: ws / 1000 - (1i64 << 30),
        products,
        campaigns,
        unknown_cat,
        dates: DateTab::new(ws - 9 * DAY_MS, we + 7 * DAY_MS),
        pshift: 0,
    };
    let mut cx = cx;

    // files
    let ev_files: Vec<&'static [u8]> = csv_files(&data_dir.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<&'static [u8]> = csv_files(&data_dir.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<&'static [u8]> = csv_files(&data_dir.join("links")).iter().map(|p| map_file(p)).collect();
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    // partitions sized to stay L2-resident: ~12K event rows each
    let est_rows = ev_bytes / 75 + 1;
    let mut pbits = 8u32;
    let prows: usize = std::env::var("SS_PROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(12_000);
    while pbits < 12 && (est_rows >> pbits) > prows {
        pbits += 1;
    }
    let npart: usize = 1 << pbits;
    let pshift = 64 - pbits;
    cx.pshift = pshift;
    // person partitions: 4x coarser (grouping tables stay small; fewer scatter streams)
    let pbits2: u32 = std::env::var("SS_P2BITS").ok().and_then(|v| v.parse().ok()).unwrap_or(pbits.saturating_sub(2).max(6));
    let npart2: usize = 1 << pbits2;
    let pshift2 = 64 - pbits2;
    let cx = cx;
    let mut chunks = Vec::new();
    make_chunks(Kind::Event, &ev_files, 2 << 20, &mut chunks);
    make_chunks(Kind::Click, &cl_files, 2 << 20, &mut chunks);
    let mut lchunks = Vec::new();
    make_chunks(Kind::Link, &ln_files, 256 << 10, &mut lchunks);
    log(&format!("setup done: {} chunks, {} threads, ndays {}", chunks.len(), nthreads, ndays));
    if let Some(d) = std::env::var("SS_PF").ok().and_then(|v| v.parse().ok()) {
        PF_DIST.store(d, Ordering::Relaxed);
    }
    if std::env::var_os("SS_BRANCHY").is_some() {
        SCAN_BRANCHY.store(true, Ordering::Relaxed);
    }
    if std::env::var_os("SS_PROF").is_some() {
        PROF.store(true, Ordering::Relaxed);
    }
    if let Ok(mode) = std::env::var("SS_BENCH") {
        // debug micro-benchmark: one thread parses every event chunk (full work incl. interning)
        for &f in ev_files.iter() {
            let mut acc = 0u8;
            for k in (0..f.len()).step_by(4096) {
                acc ^= f[k];
            }
            std::hint::black_box(acc);
        }
        let est_ev = ev_bytes / 75 + 1024;
        let ut = UTab::new(((est_ev / 10) * 3 / 2).next_power_of_two().max(1 << 16), est_ev);
        let mut st = PState::new(npart, est_ev * 3 / 2 / npart + 64, 1024);
        let mut lines = 0usize;
        let t = Instant::now();
        for c in chunks.iter().filter(|c| c.kind == Kind::Event) {
            #[cfg(target_arch = "x86_64")]
            unsafe {
                let mut cb = |f: &[&'static [u8]; 7]| {
                    lines += 1;
                    if mode == "split" {
                        std::hint::black_box(f);
                        return;
                    }
                    event_fields(f, &cx, &mut st, &ut);
                };
                if std::env::var_os("SS_PX").is_some() {
                    scan_fields_px::<7, _>(c.data, c.start, c.end, &mut cb);
                } else {
                    scan_fields::<7, _>(c.data, c.start, c.end, &mut cb);
                }
            }
        }
        st.flush(&ut);
        st.ev.finish();
        let el = t.elapsed().as_secs_f64() * 1e9;
        eprintln!("bench {mode}: {lines} lines, {:.1} ns/line, users {}", el / lines as f64, ut.n_users());
        std::process::exit(0);
    }
    if std::env::var_os("SS_PRETOUCH").is_some() {
        // debug: fault in all input pages first, to separate page-fault cost from parse cost
        let all: Vec<&'static [u8]> = ev_files.iter().chain(cl_files.iter()).chain(ln_files.iter()).copied().collect();
        let sum = AtomicUsize::new(0);
        let nxt = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                s.spawn(|| loop {
                    let i = nxt.fetch_add(1, Ordering::Relaxed);
                    if i >= all.len() {
                        break;
                    }
                    let mut acc = 0usize;
                    for k in (0..all[i].len()).step_by(4096) {
                        acc = acc.wrapping_add(all[i][k] as usize);
                    }
                    sum.fetch_add(acc, Ordering::Relaxed);
                });
            }
        });
        log(&format!("pretouch done {}", sum.load(Ordering::Relaxed) & 1));
    }

    // ---------------- phase 1: parse (thread 0 builds the union-find from links first)
    // arena slack is kept small: with huge pages the whole arena gets committed (and zeroed);
    // rare excess spills into per-partition overflow vectors
    let est_ev = ev_bytes / 80 + 1024;
    let per_part = est_ev * 23 / 20 / npart / nthreads + 32;
    let cl_bytes: usize = cl_files.iter().map(|f| f.len()).sum();
    let ln_bytes: usize = ln_files.iter().map(|f| f.len()).sum();
    let est_cl = cl_bytes / 52 + 1024;
    let cl_cap = est_cl * 13 / 10 / nthreads + 1024;
    // distinct user IDs are ~8% of event rows; size for ~2.5x that (load stays < 0.45)
    // link IDs are almost all event/click users too, so they add little
    let est_users = (est_ev + est_cl) / 12 + ln_bytes / 64 + 4096;
    let cap_slots = est_users.next_power_of_two().max(1 << 16);
    // User indices are dense. Size the per-user arrays from a hard upper bound on distinct IDs
    // (a valid row is at least ~24 bytes and names at most two IDs), so they can never overflow;
    // on Linux untouched pages cost only address space.
    let bound_users = ev_bytes / 24 + cl_bytes / 16 + ln_bytes / 3 + 1024;
    let cap_users = if cfg!(target_os = "linux") {
        bound_users.max(cap_slots * 4)
    } else {
        (cap_slots * 4).min(bound_users.max(cap_slots))
    } + (nthreads + 8) * UBLOCK as usize;
    let ut = UTab::new(cap_slots, cap_users);
    let next = AtomicUsize::new(0);
    let parent_cell: OnceLock<Vec<u32>> = OnceLock::new();
    let lnext = AtomicUsize::new(0);
    let ldone = AtomicUsize::new(0);
    let ledges: Mutex<Vec<(usize, Vec<Edge>)>> = Mutex::new(Vec::new());
    #[cfg(target_arch = "x86_64")]
    let use_simd = std::env::var_os("SS_NOSIMD").is_none() && is_x86_feature_detected!("avx2") && is_x86_feature_detected!("bmi2");
    #[cfg(not(target_arch = "x86_64"))]
    let use_simd = false;
    struct P1 {
        ev: PartBuf<EvA>,
        cl: PartBuf<ClA>,
    }
    let p1: Vec<P1> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let cx = &cx;
                let chunks = &chunks;
                let next = &next;
                let lchunks = &lchunks;
                let lnext = &lnext;
                let ldone = &ldone;
                let ledges = &ledges;
                let parent_cell = &parent_cell;
                let ut = &ut;
                s.spawn(move || {
                    pin_thread(t);
                    let mut st = PState::new(npart, per_part, cl_cap);
                    // links first: all threads parse and intern link chunks
                    loop {
                        let i = lnext.fetch_add(1, Ordering::Relaxed);
                        if i >= lchunks.len() {
                            break;
                        }
                        let c: Chunk = lchunks[i];
                        let data = c.data;
                        let mut ls = LinkSt::new((c.end - c.start) / 24 + 16);
                        if use_simd {
                            #[cfg(target_arch = "x86_64")]
                            unsafe {
                                let al = &mut st.alloc;
                                let mut cb = |f: &[&'static [u8]; 2]| ls.line(f, ut, al);
                                scan_fields_px::<2, _>(data, c.start, c.end, &mut cb);
                            }
                        } else {
                            for_lines(data, c.start, c.end, |l| {
                                let mut f: [&'static [u8]; 2] = [&[]; 2];
                                if split_fields(l, &mut f) {
                                    ls.line(&f, ut, &mut st.alloc);
                                }
                            });
                        }
                        let e = ls.finish(ut, &mut st.alloc);
                        ledges.lock().unwrap().push((i, e));
                        ldone.fetch_add(1, Ordering::Release);
                    }
                    if t == 0 {
                        while ldone.load(Ordering::Acquire) < lchunks.len() {
                            std::thread::yield_now();
                        }
                        let mut all = std::mem::take(&mut *ledges.lock().unwrap());
                        all.sort_by_key(|x| x.0);
                        let edges: Vec<Vec<Edge>> = all.into_iter().map(|x| x.1).collect();
                        // every link chunk is interned, so no linked user has a higher index
                        let n_linked = ut.n_users();
                        log("links interned");
                        let parent = build_dsu(&edges, ut, cap_users, n_linked);
                        log("links done");
                        let _ = parent_cell.set(parent);
                    }
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        let c = chunks[i];
                        let data = c.data;
                        if use_simd {
                            #[cfg(target_arch = "x86_64")]
                            unsafe {
                                match c.kind {
                                    Kind::Event => {
                                        let mut cb = |f: &[&'static [u8]; 7]| event_fields(f, cx, &mut st, ut);
                                        scan_fields_px::<7, _>(data, c.start, c.end, &mut cb);
                                    }
                                    Kind::Click => {
                                        let mut cb = |f: &[&'static [u8]; 4]| click_fields(f, cx, &mut st, ut);
                                        scan_fields_px::<4, _>(data, c.start, c.end, &mut cb);
                                    }
                                    Kind::Link => {}
                                }
                            }
                        } else {
                            match c.kind {
                                Kind::Event => for_lines(data, c.start, c.end, |l| parse_event_line(l, cx, &mut st, ut)),
                                Kind::Click => for_lines(data, c.start, c.end, |l| parse_click_line(l, cx, &mut st, ut)),
                                Kind::Link => {}
                            }
                        }
                    }
                    st.flush(ut);
                    st.ev.finish();
                    st.cl.finish();
                    P1 { ev: st.ev, cl: st.cl }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let parent = parent_cell.into_inner().expect("dsu");
    let n_users = ut.n_users().max(1);
    // person partitions: ranges of person index
    let mut shift2 = 0u32;
    while (n_users >> shift2) > (1usize << pbits2) {
        shift2 += 1;
    }
    let npart2 = (n_users >> shift2) + 1;
    log(&format!("phase 1 parse done ({} user slots, {} person partitions)", n_users, npart2));
    if *LOG_ON.get().unwrap_or(&false) {
        // arena fill per thread: rows in the fixed regions vs rows that spilled to overflow vectors
        let fill: Vec<String> = p1
            .iter()
            .map(|t| {
                let (mut m, mut o) = (0usize, 0usize);
                for p in 0..npart {
                    m += t.ev.main(p).len();
                    o += t.ev.over(p).len();
                }
                format!("{m}+{o}")
            })
            .collect();
        log(&format!("event arena fill (main+overflow) per thread: {} (cap {} each)", fill.join(" "), npart * per_part));
    }

    // ---------------- phase 2: dedup events -> person partitions; clicks -> person partitions
    const CL_PIECE: usize = 1 << 16;
    let mut cl_items: Vec<(usize, usize, usize)> = Vec::new();
    for (t, r) in p1.iter().enumerate() {
        let n = r.cl.len(0);
        let mut a = 0;
        while a < n {
            let b = (a + CL_PIECE).min(n);
            cl_items.push((t, a, b));
            a = b;
        }
    }
    let next = AtomicUsize::new(0);
    let total_items = npart + cl_items.len();
    struct P2 {
        ev: ChunkBuf<EvB, 16>,
        cl: ChunkBuf<ClB, 8>,
        // fresh chunk memory; the other chunks live inside the phase-1 buffers
        _slabs: Vec<Vec<u8>>,
    }
    let p2: Vec<P2> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let p1 = &p1;
                let parent = &parent;
                let cl_items = &cl_items;
                let next = &next;
                s.spawn(move || {
                    pin_thread(t);
                    let mut rev: ChunkBuf<EvB, 16> = ChunkBuf::new(npart2);
                    let mut rcl: ChunkBuf<ClB, 8> = ChunkBuf::new(npart2);
                    let mut pool = Pool::new();
                    // Dedup, per event-ID partition:
                    //  1. a 2-bit "seen / seen again" counter filter over the keys; a key that has
                    //     two or more copies always lands every copy on a "seen again" counter
                    //  2. rows on a "seen once" counter are unique keys: emitted directly if valid;
                    //     the rest (true repeats + filter collisions) are set aside
                    //  3. the set-aside rows go through an exact min-ingest table (first copy wins
                    //     ties; invalid copies take part, so a winning tombstone drops the event)
                    let fmul: usize = std::env::var("SS_FMUL").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
                    let mut filt: Vec<u64> = Vec::new();
                    let mut cand: Vec<EvA> = Vec::new();
                    // exact table slots: (key, ingest, (candidate index + 1) << 1 | valid); 0 = empty
                    let mut table: Vec<(u64, u32, u32)> = Vec::new();
                    let mut win: Vec<u32> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= total_items {
                            break;
                        }
                        if i < npart {
                            let p = i;
                            let n: usize = p1.iter().map(|t| t.ev.len(p)).sum();
                            if n == 0 {
                                continue;
                            }
                            let t_a = Instant::now();
                            // counters: power of two, >= fmul per row (32 per u64 word)
                            let nctr = (n * fmul).next_power_of_two().max(64);
                            let words = nctr / 32;
                            let cmask = (nctr - 1) as u64;
                            if filt.len() < words {
                                filt = vec![0u64; words];
                            } else {
                                filt[..words].fill(0);
                            }
                            let fp = filt.as_mut_ptr();
                            for sl in p1.iter().flat_map(|t| [t.ev.main(p), t.ev.over(p)]) {
                                for e in sl.iter() {
                                    let c = e.key & cmask;
                                    unsafe {
                                        let w = fp.add((c >> 5) as usize);
                                        let x = *w;
                                        let m = 1u64 << ((c & 31) * 2);
                                        *w = x | m | ((x & m) << 1);
                                    }
                                }
                            }
                            tadd(0, t_a);
                            let t_b = Instant::now();
                            cand.clear();
                            cand.reserve(n + 1);
                            let cp = cand.as_mut_ptr();
                            let mut nc = 0usize;
                            let fp = filt.as_ptr();
                            for v in p1.iter().flat_map(|t| [t.ev.main(p), t.ev.over(p)]) {
                                let m = v.len();
                                for k in 0..m {
                                    if k + 16 < m {
                                        let f = unsafe { v.get_unchecked(k + 16) };
                                        prefetch(unsafe { parent.as_ptr().add(f.user as usize) });
                                    }
                                    let e = unsafe { *v.get_unchecked(k) };
                                    let c = e.key & cmask;
                                    let again = unsafe { *fp.add((c >> 5) as usize) } >> ((c & 31) * 2 + 1) & 1;
                                    // branch-free: stage the row both ways, keep one (or neither)
                                    unsafe { cp.add(nc).write(e) };
                                    nc += again as usize;
                                    let person = person_of(parent, e.user);
                                    rev.push_if(
                                        &mut pool,
                                        (person >> shift2) as usize,
                                        EvB { person, ts: e.ts, meta: e.meta },
                                        (again == 0) & (e.ts != u32::MAX),
                                    );
                                }
                            }
                            unsafe { cand.set_len(nc) };
                            tadd(1, t_b);
                            if nc != 0 {
                                let t_x = Instant::now();
                                assert!(nc < (1usize << 31) - 1, "dedup partition too large");
                                let cap = (nc * 2).next_power_of_two().max(16);
                                let mask = cap - 1;
                                if table.len() < cap {
                                    table = vec![(0, 0, 0); cap];
                                } else {
                                    table[..cap].fill((0, 0, 0));
                                }
                                for (j, e) in cand.iter().enumerate() {
                                    let key = e.key;
                                    let ing = e.ingest;
                                    // the low key bits chose the filter counter; use higher ones here
                                    let mut slot = ((key >> 20) as usize) & mask;
                                    loop {
                                        let tt = unsafe { table.get_unchecked_mut(slot) };
                                        let tag = ((j as u32 + 1) << 1) | (e.ts != u32::MAX) as u32;
                                        if tt.2 == 0 {
                                            *tt = (key, ing, tag);
                                            break;
                                        }
                                        if tt.0 == key {
                                            if ing < tt.1 {
                                                tt.1 = ing;
                                                tt.2 = tag;
                                            }
                                            break;
                                        }
                                        slot = (slot + 1) & mask;
                                    }
                                }
                                // winners that are valid rows, compacted branch-free
                                win.clear();
                                win.reserve(cap + 1);
                                let wp = win.as_mut_ptr();
                                let mut nw = 0usize;
                                for tt in table[..cap].iter() {
                                    unsafe { wp.add(nw).write((tt.2 >> 1).wrapping_sub(1)) };
                                    nw += (tt.2 & 1) as usize;
                                }
                                unsafe { win.set_len(nw) };
                                for k in 0..nw {
                                    if k + 8 < nw {
                                        let f = unsafe { cand.get_unchecked(*win.get_unchecked(k + 8) as usize) };
                                        prefetch(unsafe { parent.as_ptr().add(f.user as usize) });
                                    }
                                    let e = unsafe { cand.get_unchecked(*win.get_unchecked(k) as usize) };
                                    let person = person_of(parent, e.user);
                                    rev.push_if(&mut pool, (person >> shift2) as usize, EvB { person, ts: e.ts, meta: e.meta }, true);
                                }
                                tadd(7, t_x);
                            }
                            // every row of this partition has been read (repeats were copied out):
                            // its phase-1 memory becomes output chunks
                            for tt in p1.iter() {
                                let reg = unsafe { tt.ev.mem.as_ptr().add(p * tt.ev.cap) } as *mut u8;
                                pool.give(reg, tt.ev.cap * std::mem::size_of::<EvA>());
                            }
                        } else {
                            let t_c = Instant::now();
                            let (t, a, b) = cl_items[i - npart];
                            let cm = p1[t].cl.main(0);
                            let co = p1[t].cl.over(0);
                            // items index the concatenation main ++ over
                            let v: &[ClA] = if b <= cm.len() {
                                &cm[a..b]
                            } else if a >= cm.len() {
                                &co[a - cm.len()..b - cm.len()]
                            } else {
                                &cm[a..]
                            };
                            let v2: &[ClA] = if a < cm.len() && b > cm.len() { &co[..b - cm.len()] } else { &[] };
                            for v in [v, v2] {
                                for (k, c) in v.iter().enumerate() {
                                    if k + 16 < v.len() {
                                        prefetch(unsafe { parent.as_ptr().add(v[k + 16].user as usize) });
                                    }
                                    let person = person_of(parent, c.user);
                                    rcl.push_if(
                                        &mut pool,
                                        (person >> shift2) as usize,
                                        ClB { cid: c.cid, person, ts: c.ts, camp: c.camp, _pad: 0 },
                                        true,
                                    );
                                }
                            }
                            // this piece has been read: release the part inside the main buffer
                            if a < cm.len() {
                                let e = b.min(cm.len());
                                pool.give(cm[a..e].as_ptr() as *mut u8, (e - a) * std::mem::size_of::<ClA>());
                            }
                            tadd(2, t_c);
                        }
                    }
                    rev.finish(&mut pool);
                    rcl.finish(&mut pool);
                    P2 { ev: rev, cl: rcl, _slabs: pool.slabs }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    log_mem("phase 2 end");
    log("phase 2 dedup done");

    // ---------------- phase 3: per person partition
    let ncat = cats.len();
    let ncells = (cx.ev_hi / GAP_MS) as usize + 2;
    // sessions start inside the event window: its days, relative to base_day_off
    let win_dshift = (ws.div_euclid(DAY_MS) - base_day_off) as u32;
    let win_nd = ((we - 1).div_euclid(DAY_MS) - ws.div_euclid(DAY_MS) + 1) as usize;
    // largest partitions first (a whale person makes one partition much bigger)
    let mut order: Vec<usize> = (0..npart2).collect();
    let psize: Vec<usize> = (0..npart2).map(|q| p2.iter().map(|t| t.ev.len(q)).sum()).collect();
    order.sort_unstable_by(|a, b| psize[*b].cmp(&psize[*a]));
    let next = AtomicUsize::new(0);
    let cl_hi = cx.cl_hi;
    // campaign stats of all workers; each adds its own when its partitions are done
    let stats_all: Mutex<Vec<[u64; 4]>> = Mutex::new(vec![[0u64; 4]; (ncamp + 1) * ndays]);
    // each worker writes its own user_daily file as soon as its partitions are done, so the
    // writing overlaps the other workers' tail of phase 3
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    let _ = fs::create_dir_all(&ud);
    let _ = fs::create_dir_all(&cd);
    let workers: Vec<Worker> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let p2 = &p2;
                let next = &next;
                let ut = &ut;
                let order = &order;
                let ud = &ud;
                let stats_all = &stats_all;
                let cats = &cats;
                s.spawn(move || {
                    pin_thread(t);
                    let mut w = Worker::new(ncamp, ndays, ncat);
                    w.ensure_cells(ncells);
                    w.set_window(ncamp, win_dshift, win_nd);
                    w.cl_hi = cl_hi;
                    let mut evp: Vec<&[EvB]> = Vec::with_capacity(p2.len() * 2);
                    let mut clp: Vec<&[ClB]> = Vec::with_capacity(p2.len() * 2);
                    loop {
                        let qi = next.fetch_add(1, Ordering::Relaxed);
                        if qi >= npart2 {
                            break;
                        }
                        let q = order[qi];
                        evp.clear();
                        clp.clear();
                        for t in p2.iter() {
                            evp.extend(t.ev.slices(q));
                            clp.extend(t.cl.slices(q));
                        }
                        let qbase = (q << shift2) as u32;
                        let qsize = 1usize << shift2;
                        w.partition(&evp, &clp, qbase, qsize, base, base_day_off, organic, ut);
                    }
                    let rows = &w.rows;
                    let nrows = rows.day.len();
                    if nrows != 0 || t == 0 {
                        // the plain columns are already page bodies: only headers, the
                        // dictionary column and the footer are encoded here
                        let mut f = pq::Writer::new(nrows, nrows / 2 + 4096);
                        f.raw_col("user_id", 0, &rows.name);
                        f.raw_col("day", 1, pq::le_bytes(&rows.day));
                        f.raw_col("sessions", 2, pq::le_bytes(&rows.sessions));
                        f.raw_col("events", 2, pq::le_bytes(&rows.events));
                        f.raw_col("revenue_cents", 3, pq::le_bytes(&rows.revenue));
                        f.dict_col("top_category", cats, rows.top.iter().copied());
                        let (own, segs) = f.finish_parts();
                        let mut file = fs::File::create(ud.join(format!("part-{t:03}.parquet"))).expect("write user_daily");
                        let mut at = 0usize;
                        for (pos, seg) in segs {
                            file.write_all(&own[at..pos]).expect("write user_daily");
                            file.write_all(seg).expect("write user_daily");
                            at = pos;
                        }
                        file.write_all(&own[at..]).expect("write user_daily");
                    }
                    w.finalize_stats(&mut stats_all.lock().unwrap());
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    log("phase 3 sessions done");
    log(&format!(
        "cpu ms: dedup-filter {:.0}, dedup-emit {:.0}, dedup-exact {:.0}, clicks-map {:.0}, group {:.0}, persons {:.0} (cells {:.0}, attrib {:.0})",
        TACC[0].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[1].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[7].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[2].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[3].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[4].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[5].load(Ordering::Relaxed) as f64 / 1e6,
        TACC[6].load(Ordering::Relaxed) as f64 / 1e6
    ));

    // ---------------- phase 4: write campaign_daily (user_daily was written by the phase 3 workers)
    // The big buffers are released on helper threads meanwhile.
    let stats = stats_all.into_inner().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || {
            par_drop(p2);
            // only now: phase-2 output chunks live inside the phase-1 buffers
            par_drop(p1);
            log("phase 1/2 buffers released");
        });
        s.spawn(move || {
            par_drop(workers);
            log("workers released");
        });
    {
        // (campaign_id, channel, day, [sessions, purchasing, revenue, assisted])
        let mut crows: Vec<(&[u8], &[u8], i32, [u64; 4])> = Vec::new();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let mut st = stats[c * ndays + d];
                if st[0] == 0 && st[3] == 0 {
                    continue;
                }
                let day = (base_day_off + d as i64) as i32;
                if c == organic {
                    st[3] = 0;
                    crows.push((b"organic", b"organic", day, st));
                } else {
                    crows.push((camp_ids[c], camp_ch[c], day, st));
                }
            }
        }
        let mut f = pq::Writer::new(crows.len(), crows.len() * 64 + 1024);
        f.str_col("campaign_id", crows.iter().map(|r| r.0));
        f.str_col("channel", crows.iter().map(|r| r.1));
        f.date_col("day", crows.iter().map(|r| r.2));
        f.i32_col("sessions", crows.iter().map(|r| r.3[0] as i32));
        f.i32_col("purchasing_sessions", crows.iter().map(|r| r.3[1] as i32));
        f.i64_col("revenue_cents", crows.iter().map(|r| r.3[2] as i64));
        f.i32_col("assisted_sessions", crows.iter().map(|r| r.3[3] as i32));
        fs::write(cd.join("part-000.parquet"), f.finish()).expect("write campaign_daily");
    }
    log("phase 4 write done");
    });
    log("released");
    fast_exit(nthreads);
}
