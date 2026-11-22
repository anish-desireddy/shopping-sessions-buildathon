//! Integrated Rust event + attribution pipeline for Shopping Sessions.
//!
//! Reads the same 32 raw event partitions the Python path creates, plus links,
//! products, clicks, campaigns and the manifest window. Emits sessions and
//! campaign_daily aggregates in little-endian binary; Python keeps the
//! user_daily aggregation and the DuckDB Parquet writes.
//!
//! `SHOPPING_INMEMORY_PARTS=1` is an experimental opt-in that keeps the raw
//! partition blocks in memory instead of writing and rereading scratch CSVs.
//! The default remains the disk-backed handoff.
//!
//! `SHOPPING_PARALLEL_CLICKS=1` is a separate experimental opt-in that scans
//! click files in ordered ranges and parallelizes click validation/indexing
//! and bucket sorting. `SHOPPING_CLICK_WORKERS` optionally sets that stage's
//! worker count independently of the event workers. `SHOPPING_FUSED_CLICKS=1`
//! additionally fuses scan, validation and person-bucket construction into one
//! pass, retaining invalid first copies as dedup markers. Neither switch changes
//! the default click path.
//!
//! `SHOPPING_PARALLEL_SESSIONIZE=1` sorts and sessionizes disjoint person
//! groups on `SHOPPING_SESSION_WORKERS` threads and merges the ordered chunks.
//! The default remains the original sequential sessionizer.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

const LOOKBACK_MS: i64 = 604_800_000;
const GAP_MS: i64 = 1_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: i64 = 10_000_000;

#[derive(Default)]
struct EventStats {
    rows: u64,
    malformed: u64,
    empty_id: u64,
    invalid_ingest: u64,
    unique: u64,
    duplicates: u64,
    invalid_user: u64,
    invalid_ts: u64,
    out_of_window: u64,
    invalid_type: u64,
    invalid_product: u64,
    invalid_price: u64,
    late: u64,
    kept: u64,
    input_bytes: u64,
    files: u64,
}

impl EventStats {
    fn merge(&mut self, other: EventStats) {
        self.rows += other.rows;
        self.malformed += other.malformed;
        self.empty_id += other.empty_id;
        self.invalid_ingest += other.invalid_ingest;
        self.unique += other.unique;
        self.duplicates += other.duplicates;
        self.invalid_user += other.invalid_user;
        self.invalid_ts += other.invalid_ts;
        self.out_of_window += other.out_of_window;
        self.invalid_type += other.invalid_type;
        self.invalid_product += other.invalid_product;
        self.invalid_price += other.invalid_price;
        self.late += other.late;
        self.kept += other.kept;
        self.input_bytes += other.input_bytes;
        self.files += other.files;
    }
}

#[derive(Default)]
struct ClickStats {
    rows: u64,
    malformed: u64,
    empty_id: u64,
    duplicates: u64,
    unique: u64,
    invalid_user: u64,
    invalid_ts: u64,
    out_of_window: u64,
    unknown_campaign: u64,
    kept: u64,
    input_bytes: u64,
    files: u64,
}

impl ClickStats {
    fn merge_scan(&mut self, other: &ClickStats) {
        self.rows += other.rows;
        self.malformed += other.malformed;
        self.empty_id += other.empty_id;
        self.duplicates += other.duplicates;
        self.input_bytes += other.input_bytes;
        self.files += other.files;
    }

    fn merge_index(&mut self, other: &ClickStats) {
        self.invalid_user += other.invalid_user;
        self.invalid_ts += other.invalid_ts;
        self.out_of_window += other.out_of_window;
        self.unknown_campaign += other.unknown_campaign;
        self.kept += other.kept;
    }
}

#[derive(Default)]
struct LinkStats {
    files: u64,
    bytes: u64,
    rows: u64,
    malformed: u64,
    empty_side: u64,
    pairs: u64,
    ids: u64,
    components: u64,
}

struct PartitionStats {
    input_files: u64,
    input_bytes: u64,
    raw_rows: u64,
    output_files: u64,
    output_bytes: u64,
}

#[derive(Default)]
struct PartitionShard {
    input_files: u64,
    input_bytes: u64,
    raw_rows: u64,
    output_files: u64,
    output_bytes: u64,
    part_bytes: Vec<u64>,
    outputs: Vec<EventPart>,
}

enum EventPart {
    Disk(PathBuf),
    Memory(Vec<u8>),
}

enum PartitionSink {
    Disk {
        path: PathBuf,
        writer: BufWriter<File>,
    },
    Memory(Vec<u8>),
}

impl PartitionSink {
    fn disk(path: PathBuf) -> Self {
        let writer =
            BufWriter::with_capacity(1 << 18, File::create(&path).expect("create partition"));
        Self::Disk { path, writer }
    }

    fn memory(capacity: usize) -> Self {
        Self::Memory(Vec::with_capacity(capacity))
    }

    fn into_event_part(self) -> EventPart {
        match self {
            Self::Disk { path, mut writer } => {
                let _ = writer.flush();
                EventPart::Disk(path)
            }
            Self::Memory(bytes) => EventPart::Memory(bytes),
        }
    }
}

impl Write for PartitionSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Disk { writer, .. } => writer.write(buf),
            Self::Memory(bytes) => {
                bytes.extend_from_slice(buf);
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Disk { writer, .. } => writer.flush(),
            Self::Memory(_) => Ok(()),
        }
    }
}

#[derive(Default)]
struct AttrMetrics {
    credited: u64,
    organic: u64,
    assist_links: u64,
    click_window_advances: u64,
    click_window_evictions: u64,
    credit_candidates: u64,
    max_active: u64,
}

struct Winner {
    ingest: i64,
    ts: Option<i128>,
    user: Vec<u8>,
    etype: Vec<u8>,
    product: Vec<u8>,
    price: Vec<u8>,
}

/// A dedup winner with only the information validation and sessionization use.
///
/// This is a representation change, not a reordering: the winning copy is still
/// chosen by `(event_id, ingests)`, an invalid winner still cannot be rescued,
/// and every validation check remains in the same order. `kind` uses 0/1/2 for
/// view/cart/purchase and 3 for every invalid event type.
struct CompactWinner {
    ingest: i64,
    ts: Option<i128>,
    user: Vec<u8>,
    kind: u8,
    product_present: bool,
    category: u16,
    price: Option<i64>,
}

struct Event {
    ts: i64,
    cents: i64,
    category: Option<u16>,
}

struct SessionRec {
    start: i64,
    day: i32,
    events: u32,
    revenue: i64,
    purchases: u32,
    categories: Vec<(u16, i64)>,
}

struct ClickCopy {
    user: Vec<u8>,
    campaign: Vec<u8>,
    ts: Option<i128>,
}

struct Click {
    id: Vec<u8>,
    ts: i64,
    campaign: usize,
}

/// One range-local click winner. The entry is kept even when the winner is
/// invalid, so a later copy in another range cannot replace it.
struct FusedEntry {
    outcome: FusedOutcome,
}

enum FusedOutcome {
    Valid {
        person: Vec<u8>,
        ts: i64,
        campaign: usize,
    },
    InvalidUser,
    InvalidTs,
    OutOfWindow,
    UnknownCampaign,
}

struct PreparedClicks {
    campaign_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    clicks_by_person: HashMap<Vec<u8>, Vec<Click>>,
    stats: ClickStats,
    prepare_wall: f64,
}

/// Read campaigns and build the complete validated click index.
///
/// This function is deliberately independent of sessions: it needs only the
/// manifest window, the identity map and the click/campaign inputs.
fn prepare_clicks(
    click_dir: &Path,
    campaigns_path: &Path,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    start_ms: i64,
    end_ms: i64,
    parallel_clicks: bool,
    click_workers: usize,
) -> PreparedClicks {
    let prepare_started = Instant::now();
    // Opt-in one-pass click path. The legacy scan/index path below remains the
    // default and still supports SHOPPING_PARALLEL_CLICKS without this switch.
    let fused_clicks = env::var("SHOPPING_FUSED_CLICKS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let (campaign_ids, campaign_names, channels) = read_campaigns(campaigns_path);

    let click_paths = csv_files(click_dir);
    let stats;
    let mut clicks_by_person;
    if parallel_clicks && fused_clicks {
        // The fused path performs validation and person/campaign resolution in
        // the scan workers, then merges range-local winners in source order.
        let (buckets, fused_stats) = scan_clicks_fused_parallel(
            &click_paths,
            click_workers,
            person_names,
            &campaign_ids,
            start_ms - LOOKBACK_MS,
            end_ms,
        );
        clicks_by_person = buckets;
        stats = fused_stats;
    } else {
        let (click_shards, mut scan_stats) = if parallel_clicks {
            scan_clicks_parallel(&click_paths, click_workers)
        } else {
            let (click_copies, click_stats) = scan_clicks_sequential(&click_paths);
            (vec![click_copies], click_stats)
        };

        let (buckets, index_stats) = if parallel_clicks {
            index_clicks_parallel(
                click_shards,
                person_names,
                &campaign_ids,
                start_ms - LOOKBACK_MS,
                end_ms,
                click_workers,
            )
        } else {
            index_clicks_sequential(
                &click_shards[0],
                person_names,
                &campaign_ids,
                start_ms - LOOKBACK_MS,
                end_ms,
            )
        };
        scan_stats.merge_index(&index_stats);
        clicks_by_person = buckets;
        stats = scan_stats;
    }

    if parallel_clicks {
        sort_click_buckets_parallel(&mut clicks_by_person, click_workers);
    } else {
        for bucket in clicks_by_person.values_mut() {
            bucket.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.id.cmp(&b.id)));
        }
    }

    PreparedClicks {
        campaign_names,
        channels,
        clicks_by_person,
        stats,
        prepare_wall: prepare_started.elapsed().as_secs_f64(),
    }
}

fn main() {
    let started = Instant::now();
    // Experimental switch; no default/competition behavior changes without it.
    let in_memory_parts = env::var("SHOPPING_INMEMORY_PARTS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let parallel_clicks = env::var("SHOPPING_PARALLEL_CLICKS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let compact_events = env::var("SHOPPING_COMPACT_EVENTS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let parallel_sessionize = env::var("SHOPPING_PARALLEL_SESSIONIZE")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut parts = None;
    let mut events = None;
    let mut parts_out = None;
    let mut links = None;
    let mut products = None;
    let mut clicks = None;
    let mut campaigns = None;
    let mut sessions_out = None;
    let mut campaigns_out = None;
    let mut user_daily_csv = None;
    let mut campaigns_csv = None;
    let mut start_ms = 0i64;
    let mut end_ms = 0i64;
    let mut partitions = 32usize;
    let mut workers = 8usize;
    let args: Vec<String> = env::args().collect();
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--parts" => parts = Some(args[index + 1].clone()),
            "--events" => events = Some(args[index + 1].clone()),
            "--parts-out" => parts_out = Some(args[index + 1].clone()),
            "--links" => links = Some(args[index + 1].clone()),
            "--products" => products = Some(args[index + 1].clone()),
            "--clicks" => clicks = Some(args[index + 1].clone()),
            "--campaigns" => campaigns = Some(args[index + 1].clone()),
            "--sessions-out" => sessions_out = Some(args[index + 1].clone()),
            "--campaigns-out" => campaigns_out = Some(args[index + 1].clone()),
            "--user-daily-csv" => user_daily_csv = Some(args[index + 1].clone()),
            "--campaigns-csv" => campaigns_csv = Some(args[index + 1].clone()),
            "--start-ms" => start_ms = args[index + 1].parse().expect("start"),
            "--end-ms" => end_ms = args[index + 1].parse().expect("end"),
            "--partitions" => partitions = args[index + 1].parse().expect("partitions"),
            "--workers" => workers = args[index + 1].parse().expect("workers"),
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
        index += 2;
    }
    let links = PathBuf::from(links.expect("--links"));
    let products_path = PathBuf::from(products.expect("--products"));
    let workers = workers.max(1);
    let click_workers = env::var("SHOPPING_CLICK_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(workers)
        .max(1);
    let session_workers = env::var("SHOPPING_SESSION_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(workers)
        .max(1);
    let partitions = partitions.max(1);
    let clicks_input = clicks.as_ref().map(PathBuf::from);
    let campaigns_input = campaigns.as_ref().map(PathBuf::from);

    let (person_names, _link_stats) = read_links(&links);
    let person_names = Arc::new(person_names);
    let persons = person_names.len();

    let (categories, products) = read_products(&products_path);
    let unknown_category = *products
        .get(b"unknown".as_slice())
        .unwrap_or(&(categories.len() as u16 - 1));

    let wants_attribution = campaigns_out.is_some() || campaigns_csv.is_some();

    let partition_started = Instant::now();
    let (groups, partition_stats): (Vec<Vec<EventPart>>, PartitionStats) =
        if let Some(events_dir) = events {
            let parts_out = PathBuf::from(parts_out.expect("--parts-out"));
            partition_events(
                &PathBuf::from(events_dir),
                &parts_out,
                partitions,
                workers,
                in_memory_parts,
            )
            .expect("partition events")
        } else {
            (
                csv_files(&PathBuf::from(parts.expect("--parts")))
                    .into_iter()
                    .map(|path| vec![EventPart::Disk(path)])
                    .collect(),
                PartitionStats {
                    input_files: 0,
                    input_bytes: 0,
                    raw_rows: 0,
                    output_files: 0,
                    output_bytes: 0,
                },
            )
        };
    let partition_elapsed = partition_started.elapsed().as_secs_f64();
    eprintln!(
        "[rust-parts] mode={} files={} bytes={}",
        if in_memory_parts { "memory" } else { "disk" },
        partition_stats.output_files,
        partition_stats.output_bytes
    );

    let parse_started = Instant::now();
    let (mut events_by_person, stats, winners_files) = collect_events(
        groups,
        workers,
        &person_names,
        &products,
        unknown_category,
        start_ms,
        end_ms,
        compact_events,
    );
    let parse_elapsed = parse_started.elapsed().as_secs_f64();

    let session_started = Instant::now();
    let (sessions_by_person, sessions_total) = if parallel_sessionize {
        sessionize_all_parallel(events_by_person.drain(), session_workers)
    } else {
        sessionize_all(events_by_person.drain())
    };
    let session_elapsed = session_started.elapsed().as_secs_f64();

    let mut output_elapsed = 0.0f64;
    let mut user_rows = 0u64;
    if let Some(path) = &user_daily_csv {
        let write_started = Instant::now();
        user_rows = write_user_daily_csv(&PathBuf::from(path), &categories, &sessions_by_person)
            .expect("write user_daily csv");
        output_elapsed = write_started.elapsed().as_secs_f64();
    }
    if let Some(path) = &sessions_out {
        let write_started = Instant::now();
        write_sessions(&PathBuf::from(path), &categories, &sessions_by_person)
            .expect("write sessions");
        output_elapsed += write_started.elapsed().as_secs_f64();
    }
    eprintln!(
        "[rust-phases] partition={:.3} parse_dedup_validate={:.3} \
         sessionize={:.3} output={:.3} user_rows={}",
        partition_elapsed, parse_elapsed, session_elapsed, output_elapsed, user_rows
    );
    eprintln!(
        "[rust-events] files={} rows={} malformed={} empty_id={} invalid_ingest={} \
         unique={} duplicates={} kept={} persons={} sessions={} dropped(user={},ts={}, \
         window={},type={},product={},price={},late={}) person_ids={}",
        winners_files,
        stats.rows,
        stats.malformed,
        stats.empty_id,
        stats.invalid_ingest,
        stats.unique,
        stats.duplicates,
        stats.kept,
        sessions_by_person.len(),
        sessions_total,
        stats.invalid_user,
        stats.invalid_ts,
        stats.out_of_window,
        stats.invalid_type,
        stats.invalid_product,
        stats.invalid_price,
        stats.late,
        persons
    );

    if wants_attribution {
        let attribution_started = Instant::now();
        let prepared = prepare_clicks(
            clicks_input.as_deref().expect("--clicks"),
            campaigns_input.as_deref().expect("--campaigns"),
            &person_names,
            start_ms,
            end_ms,
            parallel_clicks,
            click_workers,
        );
        let prepare_wall = prepared.prepare_wall;
        eprintln!(
            "[rust-clicks] mode={} scan_workers={}",
            if parallel_clicks {
                "parallel"
            } else {
                "sequential"
            },
            click_workers
        );
        let PreparedClicks {
            campaign_names,
            channels,
            clicks_by_person,
            stats: click_stats,
            ..
        } = prepared;

        let organic = campaign_names.len();
        let mut names = campaign_names;
        names.push(b"organic".to_vec());
        let mut channels = channels;
        channels.push(b"organic".to_vec());
        let (aggregates, attr_metrics) =
            attribute_parallel(&sessions_by_person, &clicks_by_person, organic, workers);

        let mut rows: Vec<(Vec<u8>, Vec<u8>, i32, [i64; 4])> = aggregates
            .into_iter()
            .map(|((campaign, day), values)| {
                (
                    names[campaign].clone(),
                    channels[campaign].clone(),
                    day,
                    values,
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.2.cmp(&b.2)));

        if let Some(out) = &campaigns_out {
            write_campaigns(&PathBuf::from(out), &rows).expect("write campaigns");
        }
        if let Some(path) = &campaigns_csv {
            write_campaigns_csv(&PathBuf::from(path), &rows).expect("write campaigns csv");
        }
        eprintln!(
            "[rust-phases] click_prepare={:.3} attribution={:.3}",
            prepare_wall,
            attribution_started.elapsed().as_secs_f64()
        );
        eprintln!(
            "[rust-attribution] rows={} malformed={} empty_id={} unique={} duplicates={} \
             kept={} dropped(user={},ts={},window={},campaign={}) credited={} organic={} \
             assists={}",
            click_stats.rows,
            click_stats.malformed,
            click_stats.empty_id,
            click_stats.unique,
            click_stats.duplicates,
            click_stats.kept,
            click_stats.invalid_user,
            click_stats.invalid_ts,
            click_stats.out_of_window,
            click_stats.unknown_campaign,
            attr_metrics.credited,
            attr_metrics.organic,
            attr_metrics.assist_links
        );
    }
    eprintln!("[rust] total {:.3}s", started.elapsed().as_secs_f64());
}

fn scan_clicks_sequential(paths: &[PathBuf]) -> (HashMap<Vec<u8>, ClickCopy>, ClickStats) {
    let mut click_stats = ClickStats::default();
    let mut click_copies: HashMap<Vec<u8>, ClickCopy> = HashMap::new();
    for path in paths {
        click_stats.files += 1;
        click_stats.input_bytes += fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let (rows, malformed) = for_each_row::<4, _>(path, true, |f| {
            let click_id = unquote(f[0]);
            if click_id.is_empty() {
                click_stats.empty_id += 1;
                return;
            }
            if click_copies.contains_key(click_id) {
                click_stats.duplicates += 1;
                return;
            }
            let parsed_ts = parse_ts(unquote(f[3]));
            click_copies.insert(
                click_id.to_vec(),
                ClickCopy {
                    user: unquote(f[1]).to_vec(),
                    campaign: unquote(f[2]).to_vec(),
                    ts: parsed_ts,
                },
            );
        });
        click_stats.rows += rows;
        click_stats.malformed += malformed;
    }
    click_stats.unique = click_copies.len() as u64;
    (click_copies, click_stats)
}

fn click_file_ranges(paths: &[PathBuf], workers: usize) -> Vec<(usize, usize)> {
    if paths.is_empty() {
        return Vec::new();
    }
    let count = workers.max(1).min(paths.len());
    if count == 1 {
        return vec![(0, paths.len())];
    }
    let sizes: Vec<u64> = paths
        .iter()
        .map(|path| fs::metadata(path).map(|m| m.len()).unwrap_or(0))
        .collect();
    let mut prefix = Vec::with_capacity(paths.len() + 1);
    prefix.push(0u64);
    for size in &sizes {
        prefix.push(prefix.last().copied().unwrap() + *size);
    }
    let total = *prefix.last().unwrap();
    let mut ranges = Vec::with_capacity(count);
    let mut start = 0usize;
    for boundary in 1..=count {
        if boundary == count {
            ranges.push((start, paths.len()));
            break;
        }
        let ideal = total * boundary as u64 / count as u64;
        let first_end = start + 1;
        let last_end = paths.len() - (count - boundary);
        let mut best_end = first_end;
        let mut best_distance = prefix[first_end].abs_diff(ideal);
        for end in first_end + 1..=last_end {
            let distance = prefix[end].abs_diff(ideal);
            if distance < best_distance {
                best_distance = distance;
                best_end = end;
            }
        }
        ranges.push((start, best_end));
        start = best_end;
    }
    ranges
}

fn scan_clicks_parallel(
    paths: &[PathBuf],
    workers: usize,
) -> (Vec<HashMap<Vec<u8>, ClickCopy>>, ClickStats) {
    let ranges = click_file_ranges(paths, workers);
    if ranges.is_empty() {
        return (Vec::new(), ClickStats::default());
    }
    let range_count = ranges.len();
    // Shards decouple the parallel scan from the ordered global merge. Every
    // copy of a click id lands in the same shard, and each shard merges the
    // range-local maps in range order, preserving first-copy precedence.
    let shard_count = workers.saturating_mul(4).clamp(1, 32);
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for (range_index, (start, end)) in ranges.into_iter().enumerate() {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut local: Vec<HashMap<Vec<u8>, ClickCopy>> =
                    (0..shard_count).map(|_| HashMap::new()).collect();
                let mut stats = ClickStats::default();
                for path in &paths[start..end] {
                    stats.files += 1;
                    stats.input_bytes += fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                    let (rows, malformed) = for_each_row::<4, _>(path, true, |f| {
                        let click_id = unquote(f[0]);
                        if click_id.is_empty() {
                            stats.empty_id += 1;
                            return;
                        }
                        let shard = (partition_hash(click_id) % shard_count as u64) as usize;
                        let bucket = &mut local[shard];
                        if bucket.contains_key(click_id) {
                            stats.duplicates += 1;
                            return;
                        }
                        let parsed_ts = parse_ts(unquote(f[3]));
                        bucket.insert(
                            click_id.to_vec(),
                            ClickCopy {
                                user: unquote(f[1]).to_vec(),
                                campaign: unquote(f[2]).to_vec(),
                                ts: parsed_ts,
                            },
                        );
                    });
                    stats.rows += rows;
                    stats.malformed += malformed;
                }
                tx.send((range_index, local, stats)).unwrap();
            });
        }
    });
    drop(tx);
    let mut results: Vec<Option<(Vec<HashMap<Vec<u8>, ClickCopy>>, ClickStats)>> =
        (0..range_count).map(|_| None).collect();
    for (range_index, local, stats) in rx {
        results[range_index] = Some((local, stats));
    }
    let mut stats = ClickStats::default();
    // Index by [shard][range] so the merge below can consume each shard in
    // source range order while never touching another shard's maps.
    let mut by_shard: Vec<Vec<HashMap<Vec<u8>, ClickCopy>>> = (0..shard_count)
        .map(|_| Vec::with_capacity(range_count))
        .collect();
    for (local, local_stats) in results.into_iter().flatten() {
        stats.merge_scan(&local_stats);
        for (shard, map) in local.into_iter().enumerate() {
            by_shard[shard].push(map);
        }
    }
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for (shard, maps) in by_shard.into_iter().enumerate() {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut winners: HashMap<Vec<u8>, ClickCopy> = HashMap::new();
                let mut cross_range_duplicates = 0u64;
                for mut local in maps {
                    for (click_id, copy) in local.drain() {
                        if winners.contains_key(click_id.as_slice()) {
                            cross_range_duplicates += 1;
                        } else {
                            winners.insert(click_id, copy);
                        }
                    }
                }
                tx.send((shard, winners, cross_range_duplicates)).unwrap();
            });
        }
    });
    drop(tx);
    let mut merged: Vec<Option<HashMap<Vec<u8>, ClickCopy>>> =
        (0..shard_count).map(|_| None).collect();
    for (shard, winners, cross_range_duplicates) in rx {
        stats.duplicates += cross_range_duplicates;
        merged[shard] = Some(winners);
    }
    let shards: Vec<HashMap<Vec<u8>, ClickCopy>> = merged.into_iter().flatten().collect();
    stats.unique = shards.iter().map(|map| map.len() as u64).sum();
    (shards, stats)
}

/// One-pass click preparation: parse, dedup, validate and build person buckets
/// in the range workers, then merge each shard's range-local winners in source
/// order. An invalid first copy is retained as an invalid marker so a later
/// valid duplicate cannot replace it. The legacy two-pass path remains intact.
fn scan_clicks_fused_parallel(
    paths: &[PathBuf],
    workers: usize,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    campaign_ids: &HashMap<Vec<u8>, usize>,
    click_window: i64,
    end_ms: i64,
) -> (HashMap<Vec<u8>, Vec<Click>>, ClickStats) {
    let ranges = click_file_ranges(paths, workers);
    if ranges.is_empty() {
        return (HashMap::new(), ClickStats::default());
    }
    let range_count = ranges.len();
    let shard_count = workers.saturating_mul(4).clamp(1, 32);
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for (range_index, (start, end)) in ranges.into_iter().enumerate() {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut local: Vec<HashMap<Vec<u8>, FusedEntry>> =
                    (0..shard_count).map(|_| HashMap::new()).collect();
                let mut stats = ClickStats::default();
                for path in &paths[start..end] {
                    stats.files += 1;
                    stats.input_bytes += fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                    let (rows, malformed) = for_each_row::<4, _>(path, true, |f| {
                        let click_id = unquote(f[0]);
                        if click_id.is_empty() {
                            stats.empty_id += 1;
                            return;
                        }
                        let shard = (partition_hash(click_id) % shard_count as u64) as usize;
                        let bucket = &mut local[shard];
                        if bucket.contains_key(click_id) {
                            stats.duplicates += 1;
                            return;
                        }
                        let parsed_ts = parse_ts(unquote(f[3]));
                        let user = unquote(f[1]);
                        let outcome = if user.is_empty() {
                            FusedOutcome::InvalidUser
                        } else if let Some(ts) = parsed_ts {
                            if ts < i128::from(click_window) || ts >= i128::from(end_ms) {
                                FusedOutcome::OutOfWindow
                            } else if let Some(&campaign) = campaign_ids.get(unquote(f[2])) {
                                let person =
                                    person_names.get(user).map(Vec::as_slice).unwrap_or(user);
                                FusedOutcome::Valid {
                                    person: person.to_vec(),
                                    ts: ts as i64,
                                    campaign,
                                }
                            } else {
                                FusedOutcome::UnknownCampaign
                            }
                        } else {
                            FusedOutcome::InvalidTs
                        };
                        bucket.insert(click_id.to_vec(), FusedEntry { outcome });
                    });
                    stats.rows += rows;
                    stats.malformed += malformed;
                }
                tx.send((range_index, local, stats)).unwrap();
            });
        }
    });
    drop(tx);
    let mut results: Vec<Option<(Vec<HashMap<Vec<u8>, FusedEntry>>, ClickStats)>> =
        (0..range_count).map(|_| None).collect();
    for (range_index, local, stats) in rx {
        results[range_index] = Some((local, stats));
    }
    let mut stats = ClickStats::default();
    // The same [shard][range] layout as the legacy scan. Range order is the
    // source-file order, which is the first-copy precedence rule.
    let mut by_shard: Vec<Vec<HashMap<Vec<u8>, FusedEntry>>> = (0..shard_count)
        .map(|_| Vec::with_capacity(range_count))
        .collect();
    for (local, local_stats) in results.into_iter().flatten() {
        stats.merge_scan(&local_stats);
        for (shard, map) in local.into_iter().enumerate() {
            by_shard[shard].push(map);
        }
    }
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for maps in by_shard {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut winners: HashMap<Vec<u8>, FusedEntry> = HashMap::new();
                let mut cross_range_duplicates = 0u64;
                for mut local in maps {
                    for (click_id, entry) in local.drain() {
                        if winners.contains_key(click_id.as_slice()) {
                            cross_range_duplicates += 1;
                        } else {
                            winners.insert(click_id, entry);
                        }
                    }
                }
                let mut buckets: HashMap<Vec<u8>, Vec<Click>> = HashMap::new();
                let mut local_stats = ClickStats::default();
                for (click_id, entry) in winners.drain() {
                    local_stats.unique += 1;
                    match entry.outcome {
                        FusedOutcome::Valid {
                            person,
                            ts,
                            campaign,
                        } => {
                            local_stats.kept += 1;
                            buckets.entry(person).or_default().push(Click {
                                id: click_id,
                                ts,
                                campaign,
                            });
                        }
                        FusedOutcome::InvalidUser => local_stats.invalid_user += 1,
                        FusedOutcome::InvalidTs => local_stats.invalid_ts += 1,
                        FusedOutcome::OutOfWindow => local_stats.out_of_window += 1,
                        FusedOutcome::UnknownCampaign => local_stats.unknown_campaign += 1,
                    }
                }
                tx.send((buckets, local_stats, cross_range_duplicates))
                    .unwrap();
            });
        }
    });
    drop(tx);
    let mut clicks_by_person: HashMap<Vec<u8>, Vec<Click>> = HashMap::new();
    for (mut local, local_stats, cross_range_duplicates) in rx {
        stats.duplicates += cross_range_duplicates;
        stats.unique += local_stats.unique;
        stats.merge_index(&local_stats);
        for (person, mut list) in local.drain() {
            clicks_by_person
                .entry(person)
                .or_insert_with(Vec::new)
                .append(&mut list);
        }
    }
    (clicks_by_person, stats)
}

fn index_clicks_sequential(
    click_copies: &HashMap<Vec<u8>, ClickCopy>,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    campaign_ids: &HashMap<Vec<u8>, usize>,
    click_window: i64,
    end_ms: i64,
) -> (HashMap<Vec<u8>, Vec<Click>>, ClickStats) {
    let mut clicks_by_person: HashMap<Vec<u8>, Vec<Click>> = HashMap::new();
    let mut stats = ClickStats::default();
    for (click_id, copy) in click_copies {
        if copy.user.is_empty() || copy.ts.is_none() {
            continue;
        }
        let ts = copy.ts.unwrap();
        if ts < i128::from(click_window) || ts >= i128::from(end_ms) {
            continue;
        }
        let campaign = match campaign_ids.get(&copy.campaign) {
            Some(index) => *index,
            None => continue,
        };
        stats.kept += 1;
        let person = person_names
            .get(&copy.user)
            .cloned()
            .unwrap_or_else(|| copy.user.clone());
        clicks_by_person.entry(person).or_default().push(Click {
            id: click_id.clone(),
            ts: ts as i64,
            campaign,
        });
    }
    (clicks_by_person, stats)
}
fn index_clicks_parallel(
    click_copies: Vec<HashMap<Vec<u8>, ClickCopy>>,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    campaign_ids: &HashMap<Vec<u8>, usize>,
    click_window: i64,
    end_ms: i64,
    workers: usize,
) -> (HashMap<Vec<u8>, Vec<Click>>, ClickStats) {
    let mut slots: Vec<Option<(Vec<u8>, ClickCopy)>> = click_copies
        .into_iter()
        .flat_map(|map| map.into_iter())
        .map(Some)
        .collect();
    if slots.is_empty() {
        return (HashMap::new(), ClickStats::default());
    }
    let workers = workers.max(1).min(slots.len());
    let chunk = slots.len().div_ceil(workers);
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for slots in slots.chunks_mut(chunk) {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut local: HashMap<Vec<u8>, Vec<Click>> = HashMap::new();
                let mut stats = ClickStats::default();
                for slot in slots {
                    let Some((click_id, copy)) = slot.take() else {
                        continue;
                    };
                    if copy.user.is_empty() {
                        stats.invalid_user += 1;
                        continue;
                    }
                    let Some(ts) = copy.ts else {
                        stats.invalid_ts += 1;
                        continue;
                    };
                    if ts < i128::from(click_window) || ts >= i128::from(end_ms) {
                        stats.out_of_window += 1;
                        continue;
                    }
                    let campaign = match campaign_ids.get(&copy.campaign) {
                        Some(index) => *index,
                        None => {
                            stats.unknown_campaign += 1;
                            continue;
                        }
                    };
                    stats.kept += 1;
                    let person = person_names
                        .get(&copy.user)
                        .map(Vec::as_slice)
                        .unwrap_or_else(|| copy.user.as_slice());
                    let click = Click {
                        id: click_id,
                        ts: ts as i64,
                        campaign,
                    };
                    if let Some(bucket) = local.get_mut(person) {
                        bucket.push(click);
                    } else {
                        local.insert(person.to_vec(), vec![click]);
                    }
                }
                tx.send((local, stats)).unwrap();
            });
        }
    });
    drop(tx);
    let mut clicks_by_person: HashMap<Vec<u8>, Vec<Click>> = HashMap::new();
    let mut stats = ClickStats::default();
    for (mut local, local_stats) in rx {
        stats.merge_index(&local_stats);
        for (person, mut list) in local.drain() {
            clicks_by_person
                .entry(person)
                .or_insert_with(Vec::new)
                .append(&mut list);
        }
    }
    (clicks_by_person, stats)
}

fn sort_click_buckets_parallel(
    clicks_by_person: &mut HashMap<Vec<u8>, Vec<Click>>,
    workers: usize,
) {
    let mut buckets: Vec<&mut Vec<Click>> = clicks_by_person.values_mut().collect();
    if buckets.is_empty() {
        return;
    }
    let workers = workers.max(1).min(buckets.len());
    let chunk = buckets.len().div_ceil(workers);
    std::thread::scope(|scope| {
        for slice in buckets.chunks_mut(chunk) {
            scope.spawn(move || {
                for bucket in slice {
                    bucket.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.id.cmp(&b.id)));
                }
            });
        }
    });
}

fn attribute_parallel(
    sessions_by_person: &BTreeMap<Vec<u8>, Vec<SessionRec>>,
    clicks_by_person: &HashMap<Vec<u8>, Vec<Click>>,
    organic: usize,
    workers: usize,
) -> (HashMap<(usize, i32), [i64; 4]>, AttrMetrics) {
    let people: Vec<(&Vec<u8>, &Vec<SessionRec>)> = sessions_by_person.iter().collect();
    let workers = workers.max(1).min(people.len().max(1));
    let chunk = people.len().div_ceil(workers);
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for worker in 0..workers {
            let tx = tx.clone();
            let people = &people;
            let start = worker * chunk;
            let end = (start + chunk).min(people.len());
            scope.spawn(move || {
                let slice = if start < end {
                    &people[start..end]
                } else {
                    &[]
                };
                let mut aggregates: HashMap<(usize, i32), [i64; 4]> = HashMap::new();
                let mut metrics = AttrMetrics::default();
                let empty: Vec<Click> = Vec::new();
                for (person, sessions) in slice {
                    let clicks = clicks_by_person.get(*person).unwrap_or(&empty);
                    let mut left = 0usize;
                    let mut right = 0usize;
                    let mut active: HashMap<usize, u32> = HashMap::new();
                    for session in *sessions {
                        let start = session.start;
                        while right < clicks.len() && clicks[right].ts <= start {
                            *active.entry(clicks[right].campaign).or_insert(0) += 1;
                            right += 1;
                            metrics.click_window_advances += 1;
                        }
                        while left < right && clicks[left].ts < start - LOOKBACK_MS {
                            let campaign = clicks[left].campaign;
                            let remaining = active.get(&campaign).copied().unwrap_or(0) - 1;
                            if remaining == 0 {
                                active.remove(&campaign);
                            } else {
                                active.insert(campaign, remaining);
                            }
                            left += 1;
                            metrics.click_window_evictions += 1;
                        }
                        let mut assists: Vec<usize> = active.keys().copied().collect();
                        assists.sort_unstable();
                        metrics.assist_links += assists.len() as u64;
                        if active.len() as u64 > metrics.max_active {
                            metrics.max_active = active.len() as u64;
                        }
                        for campaign in &assists {
                            aggregates
                                .entry((*campaign, session.day))
                                .or_insert([0, 0, 0, 0])[3] += 1;
                        }
                        let mut credit = organic;
                        if right > 0 {
                            metrics.credit_candidates += 1;
                            let candidate = &clicks[right - 1];
                            if start - candidate.ts <= LOOKBACK_MS {
                                let mut first = right - 1;
                                while first > 0 && clicks[first - 1].ts == candidate.ts {
                                    first -= 1;
                                }
                                credit = clicks[first].campaign;
                            }
                        }
                        let entry = aggregates
                            .entry((credit, session.day))
                            .or_insert([0, 0, 0, 0]);
                        entry[0] += 1;
                        if session.purchases > 0 {
                            entry[1] += 1;
                        }
                        entry[2] += session.revenue;
                        if credit == organic {
                            metrics.organic += 1;
                        } else {
                            metrics.credited += 1;
                        }
                    }
                }
                tx.send((aggregates, metrics)).unwrap();
            });
        }
    });
    drop(tx);
    let mut aggregates: HashMap<(usize, i32), [i64; 4]> = HashMap::new();
    let mut metrics = AttrMetrics::default();
    for (local, local_metrics) in rx {
        metrics.credited += local_metrics.credited;
        metrics.organic += local_metrics.organic;
        metrics.assist_links += local_metrics.assist_links;
        metrics.click_window_advances += local_metrics.click_window_advances;
        metrics.click_window_evictions += local_metrics.click_window_evictions;
        metrics.credit_candidates += local_metrics.credit_candidates;
        metrics.max_active = metrics.max_active.max(local_metrics.max_active);
        for (key, values) in local {
            let entry = aggregates.entry(key).or_insert([0, 0, 0, 0]);
            for index in 0..4 {
                entry[index] += values[index];
            }
        }
    }
    (aggregates, metrics)
}

fn first_field_quoted(line: &[u8]) -> &[u8] {
    if line.first() == Some(&b'"') {
        match line[1..].iter().position(|byte| *byte == b'"') {
            Some(end) => &line[1..1 + end],
            None => b"",
        }
    } else {
        match line.iter().position(|byte| *byte == b',') {
            Some(end) => &line[..end],
            None => line,
        }
    }
}

fn partition_hash(value: &[u8]) -> u64 {
    // FNV-1a: deterministic across threads and runs. Only equality within this
    // process matters, because every copy of an id must take the same shard.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn partition_events(
    events_dir: &Path,
    parts_out: &Path,
    partitions: usize,
    workers: usize,
    in_memory: bool,
) -> std::io::Result<(Vec<Vec<EventPart>>, PartitionStats)> {
    if !in_memory {
        fs::create_dir_all(parts_out)?;
    }
    let paths = csv_files(events_dir);
    let next = AtomicUsize::new(0);
    let failure: Mutex<Option<String>> = Mutex::new(None);
    let worker_count = workers.max(1);
    let memory_capacity = if in_memory {
        let total_input_bytes: u64 = paths
            .iter()
            .map(|path| fs::metadata(path).map(|m| m.len()).unwrap_or(0))
            .sum();
        let shard_count = (partitions as u64)
            .saturating_mul(worker_count as u64)
            .max(1);
        (total_input_bytes / shard_count) as usize + (1 << 20)
    } else {
        0
    };
    let shards: Vec<PartitionShard> = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_count);
        for thread_index in 0..worker_count {
            let next = &next;
            let paths = &paths;
            let failure = &failure;
            workers.push(scope.spawn(move || {
                let mut shard = PartitionShard {
                    part_bytes: vec![0; partitions],
                    ..PartitionShard::default()
                };
                let mut sinks: Vec<PartitionSink> = (0..partitions)
                    .map(|part| {
                        if in_memory {
                            PartitionSink::memory(memory_capacity)
                        } else {
                            let path =
                                parts_out.join(format!("part-{part:02}-{thread_index:02}.csv"));
                            PartitionSink::disk(path)
                        }
                    })
                    .collect();
                loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    if at >= paths.len() {
                        break;
                    }
                    let path = &paths[at];
                    shard.input_files += 1;
                    shard.input_bytes += fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                    let result = (|| -> std::io::Result<()> {
                        let file = File::open(path)?;
                        let mut reader = BufReader::with_capacity(1 << 20, file);
                        let mut line = Vec::with_capacity(1024);
                        let read = reader.read_until(b'\n', &mut line)?;
                        if read == 0 {
                            return Ok(());
                        }
                        if line.last() == Some(&b'\n') {
                            line.pop();
                        }
                        if split_n::<7>(&line).is_none() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("{}: bad event header", path.display()),
                            ));
                        }
                        loop {
                            line.clear();
                            if reader.read_until(b'\n', &mut line)? == 0 {
                                break;
                            }
                            if line.last() == Some(&b'\n') {
                                line.pop();
                            }
                            shard.raw_rows += 1;
                            let part = (partition_hash(first_field_quoted(&line))
                                % partitions as u64)
                                as usize;
                            shard.part_bytes[part] += line.len() as u64 + 1;
                            sinks[part].write_all(&line)?;
                            sinks[part].write_all(b"\n")?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        let mut slot = failure.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(error.to_string());
                        }
                    }
                }
                shard.output_files = sinks.len() as u64;
                shard.output_bytes = shard.part_bytes.iter().sum();
                shard.outputs = sinks
                    .into_iter()
                    .map(PartitionSink::into_event_part)
                    .collect();
                shard
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().expect("partition worker"))
            .collect()
    });
    if let Some(error) = failure.into_inner().unwrap() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, error));
    }
    let mut stats = PartitionStats {
        input_files: 0,
        input_bytes: 0,
        raw_rows: 0,
        output_files: 0,
        output_bytes: 0,
    };
    for shard in &shards {
        stats.input_files += shard.input_files;
        stats.input_bytes += shard.input_bytes;
        stats.raw_rows += shard.raw_rows;
        stats.output_files += shard.output_files;
        stats.output_bytes += shard.output_bytes;
    }
    let mut groups: Vec<Vec<EventPart>> = (0..partitions)
        .map(|_| Vec::with_capacity(worker_count))
        .collect();
    for shard in shards {
        for (part, output) in shard.outputs.into_iter().enumerate() {
            groups[part].push(output);
        }
    }
    Ok((groups, stats))
}

fn process_group(
    parts: Vec<EventPart>,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    products: &HashMap<Vec<u8>, u16>,
    unknown_category: u16,
    start_ms: i64,
    end_ms: i64,
    by_person: &mut HashMap<Vec<u8>, Vec<Event>>,
    stats: &mut EventStats,
) {
    let mut winners: HashMap<Vec<u8>, Winner> = HashMap::new();
    for part in parts {
        let consume = |f: [&[u8]; 7]| {
            let event_id = unquote(f[0]);
            if event_id.is_empty() {
                stats.empty_id += 1;
                return;
            }
            let ingest = match parse_f1(unquote(f[3])) {
                Some(value) => value as i64,
                None => {
                    stats.invalid_ingest += 1;
                    return;
                }
            };
            if let Some(previous) = winners.get(event_id) {
                stats.duplicates += 1;
                if previous.ingest <= ingest {
                    return;
                }
            }
            let parsed_ts = parse_ts(unquote(f[2]));
            winners.insert(
                event_id.to_vec(),
                Winner {
                    ingest,
                    ts: parsed_ts,
                    user: unquote(f[1]).to_vec(),
                    etype: unquote(f[4]).to_vec(),
                    product: unquote(f[5]).to_vec(),
                    price: unquote(f[6]).to_vec(),
                },
            );
        };
        let (rows, malformed, input_bytes) = match part {
            EventPart::Disk(path) => {
                let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                let result = for_each_row::<7, _>(&path, false, consume);
                (result.0, result.1, bytes)
            }
            EventPart::Memory(data) => {
                let bytes = data.len() as u64;
                let result = for_each_row_bytes::<7, _>(&data, consume);
                (result.0, result.1, bytes)
            }
        };
        stats.files += 1;
        stats.input_bytes += input_bytes;
        stats.rows += rows;
        stats.malformed += malformed;
    }
    stats.unique += winners.len() as u64;
    for winner in winners.values() {
        if winner.user.is_empty() {
            stats.invalid_user += 1;
            continue;
        }
        let ts = match winner.ts {
            Some(value) => value,
            None => {
                stats.invalid_ts += 1;
                continue;
            }
        };
        if ts < i128::from(start_ms) || ts >= i128::from(end_ms) {
            stats.out_of_window += 1;
            continue;
        }
        if winner.etype != b"view" && winner.etype != b"cart" && winner.etype != b"purchase" {
            stats.invalid_type += 1;
            continue;
        }
        if winner.product.is_empty() {
            stats.invalid_product += 1;
            continue;
        }
        let cents = if winner.etype == b"purchase" {
            match parse_price(&winner.price) {
                Some(value) => value,
                None => {
                    stats.invalid_price += 1;
                    continue;
                }
            }
        } else {
            0
        };
        if i128::from(winner.ingest) - ts > i128::from(LATE_MS) {
            stats.late += 1;
            continue;
        }
        let category = if winner.etype == b"purchase" {
            Some(*products.get(&winner.product).unwrap_or(&unknown_category))
        } else {
            None
        };
        stats.kept += 1;
        let person = person_names
            .get(&winner.user)
            .cloned()
            .unwrap_or_else(|| winner.user.clone());
        by_person.entry(person).or_default().push(Event {
            ts: ts as i64,
            cents,
            category,
        });
    }
}

fn collect_events(
    groups: Vec<Vec<EventPart>>,
    workers: usize,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    products: &HashMap<Vec<u8>, u16>,
    unknown_category: u16,
    start_ms: i64,
    end_ms: i64,
    compact: bool,
) -> (HashMap<Vec<u8>, Vec<Event>>, EventStats, u64) {
    let (tx, rx) = mpsc::channel();
    let group_count = groups.len();
    let queue = Mutex::new(groups);
    let workers = workers.max(1).min(group_count.max(1));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let queue = &queue;
            scope.spawn(move || {
                let mut local: HashMap<Vec<u8>, Vec<Event>> = HashMap::new();
                let mut stats = EventStats::default();
                let mut files = 0u64;
                loop {
                    let group = queue.lock().unwrap().pop();
                    let Some(group) = group else {
                        break;
                    };
                    files += group.len() as u64;
                    if compact {
                        process_group_compact(
                            group,
                            person_names,
                            products,
                            unknown_category,
                            start_ms,
                            end_ms,
                            &mut local,
                            &mut stats,
                        );
                    } else {
                        process_group(
                            group,
                            person_names,
                            products,
                            unknown_category,
                            start_ms,
                            end_ms,
                            &mut local,
                            &mut stats,
                        );
                    }
                }
                tx.send((local, stats, files)).unwrap();
            });
        }
    });
    drop(tx);
    let mut events_by_person = HashMap::new();
    let mut stats = EventStats::default();
    let mut files = 0u64;
    for (mut local, local_stats, local_files) in rx {
        stats.merge(local_stats);
        files += local_files;
        for (person, mut list) in local.drain() {
            events_by_person
                .entry(person)
                .or_insert_with(Vec::new)
                .append(&mut list);
        }
    }
    (events_by_person, stats, files)
}

/// Compact-record counterpart of [`process_group`].
///
/// The dedup key and winner precedence are unchanged. For each winner the
/// parser reduces event type, product presence/category and purchase price to
/// fixed-width fields, so repeated `view`/`cart`/`purchase` strings and product
/// IDs are not allocated 60M times. Validation below applies the exact same
/// checks in the exact same order as the legacy path.
fn process_group_compact(
    parts: Vec<EventPart>,
    person_names: &HashMap<Vec<u8>, Vec<u8>>,
    products: &HashMap<Vec<u8>, u16>,
    unknown_category: u16,
    start_ms: i64,
    end_ms: i64,
    by_person: &mut HashMap<Vec<u8>, Vec<Event>>,
    stats: &mut EventStats,
) {
    let mut winners: HashMap<Vec<u8>, CompactWinner> = HashMap::new();
    for part in parts {
        let consume = |f: [&[u8]; 7]| {
            let event_id = unquote(f[0]);
            if event_id.is_empty() {
                stats.empty_id += 1;
                return;
            }
            let ingest = match parse_f1(unquote(f[3])) {
                Some(value) => value as i64,
                None => {
                    stats.invalid_ingest += 1;
                    return;
                }
            };
            if let Some(previous) = winners.get_mut(event_id) {
                stats.duplicates += 1;
                if previous.ingest <= ingest {
                    return;
                }
            }
            let parsed_ts = parse_ts(unquote(f[2]));
            let etype = unquote(f[4]);
            let kind = if etype == b"view" {
                0
            } else if etype == b"cart" {
                1
            } else if etype == b"purchase" {
                2
            } else {
                3
            };
            let product = unquote(f[5]);
            let product_present = !product.is_empty();
            let category = if kind == 2 && product_present {
                *products.get(product).unwrap_or(&unknown_category)
            } else {
                unknown_category
            };
            let price = if kind == 2 {
                parse_price(unquote(f[6]))
            } else {
                Some(0)
            };
            let winner = CompactWinner {
                ingest,
                ts: parsed_ts,
                user: unquote(f[1]).to_vec(),
                kind,
                product_present,
                category,
                price,
            };
            if let Some(previous) = winners.get_mut(event_id) {
                *previous = winner;
            } else {
                winners.insert(event_id.to_vec(), winner);
            }
        };
        let (rows, malformed, input_bytes) = match part {
            EventPart::Disk(path) => {
                let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                let result = for_each_row::<7, _>(&path, false, consume);
                (result.0, result.1, bytes)
            }
            EventPart::Memory(data) => {
                let bytes = data.len() as u64;
                let result = for_each_row_bytes::<7, _>(&data, consume);
                (result.0, result.1, bytes)
            }
        };
        stats.files += 1;
        stats.input_bytes += input_bytes;
        stats.rows += rows;
        stats.malformed += malformed;
    }
    stats.unique += winners.len() as u64;
    for winner in winners.values() {
        if winner.user.is_empty() {
            stats.invalid_user += 1;
            continue;
        }
        let ts = match winner.ts {
            Some(value) => value,
            None => {
                stats.invalid_ts += 1;
                continue;
            }
        };
        if ts < i128::from(start_ms) || ts >= i128::from(end_ms) {
            stats.out_of_window += 1;
            continue;
        }
        if winner.kind > 2 {
            stats.invalid_type += 1;
            continue;
        }
        if !winner.product_present {
            stats.invalid_product += 1;
            continue;
        }
        let cents = if winner.kind == 2 {
            match winner.price {
                Some(value) => value,
                None => {
                    stats.invalid_price += 1;
                    continue;
                }
            }
        } else {
            0
        };
        if i128::from(winner.ingest) - ts > i128::from(LATE_MS) {
            stats.late += 1;
            continue;
        }
        let category = if winner.kind == 2 {
            Some(winner.category)
        } else {
            None
        };
        stats.kept += 1;
        // Borrow the canonical name while the bucket exists. The legacy path
        // cloned it for every surviving event; at 60M that is 58.5M short-lived
        // allocations that the compact path can avoid without changing keys.
        let person = person_names
            .get(winner.user.as_slice())
            .map(Vec::as_slice)
            .unwrap_or(winner.user.as_slice());
        let event = Event {
            ts: ts as i64,
            cents,
            category,
        };
        if let Some(list) = by_person.get_mut(person) {
            list.push(event);
        } else {
            by_person.insert(person.to_vec(), vec![event]);
        }
    }
}

fn sessionize_person(person: Vec<u8>, mut list: Vec<Event>) -> (Vec<u8>, Vec<SessionRec>) {
    list.sort_by_key(|event| event.ts);
    let mut sessions: Vec<SessionRec> = Vec::new();
    let mut current: Option<SessionRec> = None;
    let mut previous_ts = 0i64;
    for event in list {
        let new_session = match &current {
            None => true,
            Some(_) => event.ts - previous_ts > GAP_MS,
        };
        if new_session {
            if let Some(session) = current.take() {
                sessions.push(session);
            }
            current = Some(SessionRec {
                start: event.ts,
                day: event.ts.div_euclid(86_400_000) as i32,
                events: 1,
                revenue: event.cents,
                purchases: i32::from(event.category.is_some()) as u32,
                categories: Vec::new(),
            });
        } else if let Some(session) = current.as_mut() {
            session.events += 1;
            session.revenue += event.cents;
            if event.category.is_some() {
                session.purchases += 1;
            }
        }
        if let (Some(session), Some(category)) = (current.as_mut(), event.category) {
            let mut found = false;
            for pair in session.categories.iter_mut() {
                if pair.0 == category {
                    pair.1 += event.cents;
                    found = true;
                    break;
                }
            }
            if !found {
                session.categories.push((category, event.cents));
            }
        }
        previous_ts = event.ts;
    }
    if let Some(session) = current.take() {
        sessions.push(session);
    }
    for session in sessions.iter_mut() {
        session.categories.sort_unstable_by_key(|pair| pair.0);
    }
    (person, sessions)
}

fn sessionize_all<I>(events: I) -> (BTreeMap<Vec<u8>, Vec<SessionRec>>, u64)
where
    I: Iterator<Item = (Vec<u8>, Vec<Event>)>,
{
    let mut sessions_by_person = BTreeMap::new();
    let mut total = 0u64;
    for (person, list) in events {
        let (person, sessions) = sessionize_person(person, list);
        total += sessions.len() as u64;
        sessions_by_person.insert(person, sessions);
    }
    (sessions_by_person, total)
}

/// Sort and sessionize disjoint person groups in parallel, then merge the
/// independently ordered `BTreeMap` chunks. `BTreeMap::append` moves nodes, so
/// the final key order and every per-person session record are unchanged.
fn sessionize_all_parallel<I>(
    events: I,
    workers: usize,
) -> (BTreeMap<Vec<u8>, Vec<SessionRec>>, u64)
where
    I: Iterator<Item = (Vec<u8>, Vec<Event>)>,
{
    let mut people: Vec<Option<(Vec<u8>, Vec<Event>)>> = events.map(Some).collect();
    if people.is_empty() {
        return (BTreeMap::new(), 0);
    }
    let workers = workers.max(1).min(people.len());
    let chunk = people.len().div_ceil(workers);
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for slice in people.chunks_mut(chunk) {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut local = BTreeMap::new();
                let mut total = 0u64;
                for slot in slice.iter_mut() {
                    let Some((person, list)) = slot.take() else {
                        continue;
                    };
                    let (person, sessions) = sessionize_person(person, list);
                    total += sessions.len() as u64;
                    local.insert(person, sessions);
                }
                tx.send((local, total)).unwrap();
            });
        }
    });
    drop(tx);
    let mut sessions_by_person = BTreeMap::new();
    let mut total = 0u64;
    for (mut local, local_total) in rx {
        total += local_total;
        sessions_by_person.append(&mut local);
    }
    (sessions_by_person, total)
}

fn top_category(categories: &[Vec<u8>], totals: &HashMap<u16, i64>) -> Option<u16> {
    let mut best: Option<(u16, i64)> = None;
    for (&category, &cents) in totals {
        best = match best {
            None => Some((category, cents)),
            Some((best_category, best_cents)) => {
                if cents > best_cents
                    || (cents == best_cents
                        && categories[category as usize] < categories[best_category as usize])
                {
                    Some((category, cents))
                } else {
                    Some((best_category, best_cents))
                }
            }
        };
    }
    best.map(|(category, _)| category)
}

fn write_user_daily_csv(
    path: &Path,
    categories: &[Vec<u8>],
    sessions_by_person: &BTreeMap<Vec<u8>, Vec<SessionRec>>,
) -> std::io::Result<u64> {
    let file = File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut rows = 0u64;
    for (person, sessions) in sessions_by_person {
        let mut day = i32::MIN;
        let mut count = 0u64;
        let mut events = 0u64;
        let mut revenue = 0i64;
        let mut totals: HashMap<u16, i64> = HashMap::new();
        for session in sessions {
            if count > 0 && session.day != day {
                out.write_all(person)?;
                write!(out, ",{},{},{},{},", day, count, events, revenue)?;
                if let Some(category) = top_category(categories, &totals) {
                    out.write_all(&categories[category as usize])?;
                }
                out.write_all(b"\n")?;
                rows += 1;
                day = i32::MIN;
                count = 0;
                events = 0;
                revenue = 0;
                totals.clear();
            }
            if count == 0 {
                day = session.day;
            }
            count += 1;
            events += u64::from(session.events);
            revenue += session.revenue;
            for &(category, cents) in &session.categories {
                *totals.entry(category).or_insert(0) += cents;
            }
        }
        if count > 0 {
            out.write_all(person)?;
            write!(out, ",{},{},{},{},", day, count, events, revenue)?;
            if let Some(category) = top_category(categories, &totals) {
                out.write_all(&categories[category as usize])?;
            }
            out.write_all(b"\n")?;
            rows += 1;
        }
    }
    out.flush()?;
    Ok(rows)
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read directory")
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().map(|ext| ext == "csv").unwrap_or(false))
        .collect();
    paths.sort();
    paths
}

fn for_each_row<const N: usize, F: FnMut([&[u8]; N])>(
    path: &Path,
    skip_header: bool,
    consume: F,
) -> (u64, u64) {
    let file = File::open(path).expect("open csv");
    let mut reader = BufReader::with_capacity(1 << 20, file);
    for_each_row_reader(&mut reader, skip_header, consume)
}

fn for_each_row_bytes<const N: usize, F: FnMut([&[u8]; N])>(data: &[u8], consume: F) -> (u64, u64) {
    let mut reader = BufReader::with_capacity(1 << 20, data);
    for_each_row_reader(&mut reader, false, consume)
}

fn for_each_row_reader<const N: usize, F: FnMut([&[u8]; N]), R: BufRead>(
    reader: &mut R,
    skip_header: bool,
    mut consume: F,
) -> (u64, u64) {
    let mut line: Vec<u8> = Vec::with_capacity(1024);
    let mut first = true;
    let mut rows = 0u64;
    let mut malformed = 0u64;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line).expect("read line");
        if read == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if first {
            first = false;
            if skip_header {
                continue;
            }
        }
        match split_n::<N>(&line) {
            Some(fields) => {
                rows += 1;
                consume(fields);
            }
            None => malformed += 1,
        }
    }
    (rows, malformed)
}

fn split_n<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() || std::str::from_utf8(line).is_err() {
        return None;
    }
    let mut fields: [&[u8]; N] = [b""; N];
    let mut count = 0usize;
    let mut start = 0usize;
    for index in 0..=line.len() {
        if index == line.len() || line[index] == b',' {
            if count == N {
                return None;
            }
            let field = &line[start..index];
            if !valid_field(field) {
                return None;
            }
            fields[count] = field;
            count += 1;
            start = index + 1;
        }
    }
    if count == N {
        Some(fields)
    } else {
        None
    }
}

fn valid_field(field: &[u8]) -> bool {
    if field.first() == Some(&b'"') {
        field.len() >= 2
            && field.last() == Some(&b'"')
            && !field[1..field.len() - 1].contains(&b'"')
    } else {
        !field.contains(&b'"')
    }
}

fn unquote(field: &[u8]) -> &[u8] {
    if field.first() == Some(&b'"') && field.len() >= 2 {
        &field[1..field.len() - 1]
    } else {
        field
    }
}

fn read_links(links: &Path) -> (HashMap<Vec<u8>, Vec<u8>>, LinkStats) {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut stats = LinkStats::default();
    for path in csv_files(links) {
        stats.files += 1;
        stats.bytes += fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let (well_formed, malformed) = for_each_row::<2, _>(&path, true, |f| {
            let left = unquote(f[0]);
            let right = unquote(f[1]);
            if left.is_empty() || right.is_empty() {
                stats.empty_side += 1;
                return;
            }
            stats.pairs += 1;
            rows.push((left.to_vec(), right.to_vec()));
        });
        stats.rows += well_formed;
        stats.malformed += malformed;
    }
    let mut ids: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut parent: Vec<usize> = Vec::new();
    let mut rank: Vec<u32> = Vec::new();
    for (left, right) in rows {
        let left_id = intern(&mut ids, &left);
        let right_id = intern(&mut ids, &right);
        while parent.len() < ids.len() {
            parent.push(parent.len());
            rank.push(0);
        }
        let left_root = find(&mut parent, left_id);
        let right_root = find(&mut parent, right_id);
        if left_root != right_root {
            if rank[left_root] < rank[right_root] {
                parent[left_root] = right_root;
            } else {
                parent[right_root] = left_root;
                if rank[left_root] == rank[right_root] {
                    rank[left_root] += 1;
                }
            }
        }
    }
    let mut smallest: HashMap<usize, Vec<u8>> = HashMap::new();
    for (item, index) in &ids {
        let root = find(&mut parent, *index);
        match smallest.get_mut(&root) {
            Some(current) => {
                if item < current {
                    *current = item.clone();
                }
            }
            None => {
                smallest.insert(root, item.clone());
            }
        }
    }
    stats.ids = ids.len() as u64;
    stats.components = smallest.len() as u64;
    let mut names: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(ids.len());
    for (item, index) in ids {
        let root = find(&mut parent, index);
        names.insert(item, smallest[&root].clone());
    }
    (names, stats)
}

fn intern(ids: &mut HashMap<Vec<u8>, usize>, value: &[u8]) -> usize {
    if let Some(index) = ids.get(value) {
        return *index;
    }
    let index = ids.len();
    ids.insert(value.to_vec(), index);
    index
}

fn find(parent: &mut [usize], mut item: usize) -> usize {
    let mut root = item;
    while parent[root] != root {
        root = parent[root];
    }
    while parent[item] != root {
        let next = parent[item];
        parent[item] = root;
        item = next;
    }
    root
}

fn read_products(path: &Path) -> (Vec<Vec<u8>>, HashMap<Vec<u8>, u16>) {
    let mut categories: Vec<Vec<u8>> = Vec::new();
    let mut category_ids: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut products: HashMap<Vec<u8>, u16> = HashMap::new();
    for_each_row::<2, _>(path, true, |f| {
        let product = unquote(f[0]);
        if product.is_empty() {
            return;
        }
        let category = unquote(f[1]);
        let id = match category_ids.get(category) {
            Some(value) => *value,
            None => {
                let value = categories.len() as u16;
                category_ids.insert(category.to_vec(), value);
                categories.push(category.to_vec());
                value
            }
        };
        products.insert(product.to_vec(), id);
    });
    if !category_ids.contains_key(b"unknown".as_slice()) {
        categories.push(b"unknown".to_vec());
    }
    (categories, products)
}

fn read_campaigns(path: &Path) -> (HashMap<Vec<u8>, usize>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut ids: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut names: Vec<Vec<u8>> = Vec::new();
    let mut channels: Vec<Vec<u8>> = Vec::new();
    for_each_row::<2, _>(path, true, |f| {
        let campaign = unquote(f[0]);
        if campaign.is_empty() {
            return;
        }
        let channel = unquote(f[1]);
        if let Some(index) = ids.get(campaign) {
            channels[*index] = channel.to_vec();
        } else {
            let index = names.len();
            ids.insert(campaign.to_vec(), index);
            names.push(campaign.to_vec());
            channels.push(channel.to_vec());
        }
    });
    (ids, names, channels)
}

fn write_sessions(
    path: &Path,
    categories: &[Vec<u8>],
    sessions: &BTreeMap<Vec<u8>, Vec<SessionRec>>,
) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    out.write_all(&(categories.len() as u32).to_le_bytes())?;
    for category in categories {
        out.write_all(&(category.len() as u32).to_le_bytes())?;
        out.write_all(category)?;
    }
    let count: u32 = sessions.values().map(|value| value.len() as u32).sum();
    out.write_all(&count.to_le_bytes())?;
    for (person, records) in sessions {
        for record in records {
            out.write_all(&(person.len() as u32).to_le_bytes())?;
            out.write_all(person)?;
            out.write_all(&record.start.to_le_bytes())?;
            out.write_all(&record.day.to_le_bytes())?;
            out.write_all(&record.events.to_le_bytes())?;
            out.write_all(&record.revenue.to_le_bytes())?;
            out.write_all(&record.purchases.to_le_bytes())?;
            out.write_all(&(record.categories.len() as u16).to_le_bytes())?;
            for (category, cents) in &record.categories {
                out.write_all(&category.to_le_bytes())?;
                out.write_all(&cents.to_le_bytes())?;
            }
        }
    }
    out.flush()
}

fn write_campaigns_csv(
    path: &Path,
    rows: &[(Vec<u8>, Vec<u8>, i32, [i64; 4])],
) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for (campaign, channel, day, values) in rows {
        out.write_all(campaign)?;
        out.write_all(b",")?;
        out.write_all(channel)?;
        write!(
            out,
            ",{},{},{},{},{}\n",
            day, values[0], values[1], values[2], values[3]
        )?;
    }
    out.flush()
}

fn write_campaigns(path: &Path, rows: &[(Vec<u8>, Vec<u8>, i32, [i64; 4])]) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 16, file);
    out.write_all(&(rows.len() as u32).to_le_bytes())?;
    for (campaign, channel, day, values) in rows {
        out.write_all(&(campaign.len() as u32).to_le_bytes())?;
        out.write_all(campaign)?;
        out.write_all(&(channel.len() as u32).to_le_bytes())?;
        out.write_all(channel)?;
        out.write_all(&day.to_le_bytes())?;
        for value in values {
            out.write_all(&value.to_le_bytes())?;
        }
    }
    out.flush()
}

fn digits(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().all(|byte| byte.is_ascii_digit())
}

fn value2(value: &[u8]) -> Option<u32> {
    if value.len() != 2 || !digits(value) {
        return None;
    }
    Some(u32::from(value[0] - b'0') * 10 + u32::from(value[1] - b'0'))
}

fn value4(value: &[u8]) -> Option<u32> {
    if value.len() != 4 || !digits(value) {
        return None;
    }
    let mut result = 0u32;
    for byte in value {
        result = result * 10 + u32::from(*byte - b'0');
    }
    Some(result)
}

fn is_leap(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_part = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_part + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146097 + day_of_era - 719468
}

fn date_days(value: &[u8]) -> Option<i64> {
    if value.len() != 10 || value[4] != b'-' || value[7] != b'-' {
        return None;
    }
    let year = value4(&value[0..4])?;
    let month = value2(&value[5..7])?;
    let day = value2(&value[8..10])?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(
        i64::from(year),
        i64::from(month),
        i64::from(day),
    ))
}

fn clock(value: &[u8]) -> Option<(u32, u32, u32)> {
    let hour = value2(&value[11..13])?;
    let minute = value2(&value[14..16])?;
    let second = value2(&value[17..19])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour, minute, second))
}

fn parse_f1(value: &[u8]) -> Option<i128> {
    if value.len() != 20
        || value[4] != b'-'
        || value[7] != b'-'
        || value[10] != b'T'
        || value[13] != b':'
        || value[16] != b':'
        || value[19] != b'Z'
    {
        return None;
    }
    let days = date_days(&value[0..10])?;
    let (hour, minute, second) = clock(value)?;
    Some((days * 86_400 + i64::from(hour * 3600 + minute * 60 + second)) as i128 * 1_000)
}

fn parse_ts(value: &[u8]) -> Option<i128> {
    if value.is_empty() {
        return None;
    }
    if digits(value) {
        let mut number: i128 = 0;
        for byte in value {
            number = number
                .saturating_mul(10)
                .saturating_add(i128::from(*byte - b'0'));
        }
        return Some(number);
    }
    if value.len() == 20 {
        return parse_f1(value);
    }
    if value.len() == 25
        && value[4] == b'-'
        && value[7] == b'-'
        && value[10] == b'T'
        && value[13] == b':'
        && value[16] == b':'
        && (value[19] == b'+' || value[19] == b'-')
        && value[22] == b':'
    {
        let days = date_days(&value[0..10])?;
        let (hour, minute, second) = clock(value)?;
        let offset_hours = value2(&value[20..22])?;
        let offset_minutes = value2(&value[23..25])?;
        if offset_hours > 23 || offset_minutes > 59 {
            return None;
        }
        let sign: i64 = if value[19] == b'+' { 1 } else { -1 };
        let offset = sign * i64::from(offset_hours * 3600 + offset_minutes * 60);
        let seconds = days * 86_400 + i64::from(hour * 3600 + minute * 60 + second) - offset;
        return Some(seconds as i128 * 1_000);
    }
    None
}

fn parse_price(value: &[u8]) -> Option<i64> {
    if value.is_empty() || !digits(value) {
        return None;
    }
    let mut start = 0;
    while start < value.len() && value[start] == b'0' {
        start += 1;
    }
    let significant = &value[start..];
    if significant.is_empty() || significant.len() > 8 {
        return None;
    }
    let mut cents = 0i64;
    for byte in significant {
        cents = cents * 10 + i64::from(*byte - b'0');
    }
    if cents > MAX_PRICE {
        return None;
    }
    Some(cents)
}
