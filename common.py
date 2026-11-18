"""Shared helpers for forward (GCS->Azure) and rollback (Azure->GCS) pipelines.

Events carry an operation: CREATE (copy the object) or DELETE (remove it at the destination).
"""
import os, socket, subprocess, tempfile, time, logging
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync")

WORKER_NAME = os.getenv("WORKER_NAME", socket.gethostname())
MAX_RETRY = int(os.getenv("MAX_RETRY", "5"))
BATCH_MAX_FILES = int(os.getenv("BATCH_MAX_FILES", "5000"))
BATCH_MAX_BYTES = int(os.getenv("BATCH_MAX_BYTES", str(5 * 1024 ** 3)))
BATCH_MAX_AGE_SEC = int(os.getenv("BATCH_MAX_AGE_SEC", "120"))
POLL_SEC = int(os.getenv("POLL_SEC", "10"))
# Delete safety: deletes are only batched when DELETE_ENABLED=true, and only after they
# have waited DELETE_DELAY_SEC (time to notice and stop an accidental mass delete).
DELETE_ENABLED = os.getenv("DELETE_ENABLED", "false").lower() == "true"
DELETE_DELAY_SEC = int(os.getenv("DELETE_DELAY_SEC", "360"))
DELETE_CONCURRENCY = int(os.getenv("DELETE_CONCURRENCY", "8"))
RCLONE_FLAGS = os.getenv(
    "RCLONE_FLAGS", "--transfers=8 --checkers=8 --multi-thread-streams=4 --buffer-size=32M --retries=3"
).split()

# An event is ready to batch if it is a CREATE, or a DELETE that is enabled and old enough.
READY_SQL = ("(COALESCE(operation,'CREATE')='CREATE' OR "
             "(%s AND created_at < NOW() - make_interval(secs => %s)))")


def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), dbname=os.getenv("PGDATABASE", "gcssync"),
        user=os.getenv("PGUSER", "gcssync"), password=os.environ["PGPASSWORD"],
    )


# Forward and rollback differ only by table/column names and rclone direction.
FORWARD = dict(
    events="file_events", batches="sync_batches", map="batch_files", map_col="file_event_id",
    group_col="bucket_name", name_col="object_name", size_col="size_bytes",
    src_remote=os.getenv("GCS_REMOTE", "gcs"), dst_remote=os.getenv("AZURE_REMOTE", "azure"),
)
ROLLBACK = dict(
    events="blob_events", batches="rollback_batches", map="rollback_batch_files", map_col="blob_event_id",
    group_col="container_name", name_col="blob_name", size_col=None,
    src_remote=os.getenv("AZURE_REMOTE", "azure"), dst_remote=os.getenv("GCS_REMOTE", "gcs"),
)


def create_batches(cfg):
    """Group ready NEW events per bucket/container into PENDING batches (count OR size OR age)."""
    ev, bt, mp, gc = cfg["events"], cfg["batches"], cfg["map"], cfg["group_col"]
    size_expr = f"COALESCE(SUM({cfg['size_col']}),0)" if cfg["size_col"] else "0"
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    {gc},
                    COUNT(*),
                    {size_expr},
                    EXTRACT(
                        EPOCH FROM NOW() - MIN(
                            CASE
                                WHEN COALESCE(operation, 'CREATE') = 'DELETE'
                                THEN created_at + make_interval(secs => %s)
                                ELSE created_at
                            END
                        )
                    )
                FROM {ev}
                WHERE status = 'NEW'
                  AND {READY_SQL}
                GROUP BY {gc}
                """,
                (
                    DELETE_DELAY_SEC,
                    DELETE_ENABLED,
                    DELETE_DELAY_SEC,
                ),
            )
            groups = cur.fetchall()
        conn.commit()
        for grp, cnt, size, age in groups:
            if not (cnt >= BATCH_MAX_FILES or size >= BATCH_MAX_BYTES or age >= BATCH_MAX_AGE_SEC):
                continue
            while True:  # drain this group in capped batches
                with conn, conn.cursor() as cur:
                    cols = f"event_id, {cfg['size_col']}" if cfg["size_col"] else "event_id, 0"
                    cur.execute(
                        f"SELECT {cols} FROM {ev} WHERE status='NEW' AND {gc}=%s AND {READY_SQL} "
                        f"ORDER BY event_id LIMIT %s FOR UPDATE SKIP LOCKED",
                        (grp, DELETE_ENABLED, DELETE_DELAY_SEC, BATCH_MAX_FILES))
                    rows, ids, total = cur.fetchall(), [], 0
                    for i, s in rows:
                        ids.append(i); total += s or 0
                        if total >= BATCH_MAX_BYTES:
                            break
                    if not ids:
                        break
                    cur.execute(
                        f"INSERT INTO {bt} ({gc},file_count,status) VALUES (%s,%s,'PENDING') RETURNING batch_id",
                        (grp, len(ids)))
                    bid = cur.fetchone()[0]
                    cur.executemany(f"INSERT INTO {mp} (batch_id,{cfg['map_col']}) VALUES (%s,%s)",
                                    [(bid, i) for i in ids])
                    cur.execute(f"UPDATE {ev} SET status='BATCHED' WHERE event_id = ANY(%s)", (ids,))
                    log.info("created batch %s for %s with %s files", bid, grp, len(ids))
                    if len(ids) < BATCH_MAX_FILES and total < BATCH_MAX_BYTES:
                        break
    finally:
        conn.close()


def claim_batch(conn, cfg):
    with conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {cfg['batches']} SET status='PROCESSING', started_at=NOW() WHERE batch_id = ("
            f"SELECT batch_id FROM {cfg['batches']} WHERE status='PENDING' "
            f"ORDER BY batch_id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING batch_id, {cfg['group_col']}")
        return cur.fetchone()


def source_has(cfg, group, name):
    """True if the live object exists at the source, False if it does not, None if the check failed."""
    res = subprocess.run(["rclone", "lsf", "--files-only", f"{cfg['src_remote']}:{group}/{name}"],
                         capture_output=True, text=True)
    if res.returncode == 0:
        return bool(res.stdout.strip())
    if res.returncode in (3, 4):  # directory or object not found
        return False
    log.error("source check failed for %s rc=%s err=%s", name, res.returncode, res.stderr)
    return None


def delete_one_object(cfg, group, name):
    """Safely process one DELETE event."""
    if name.endswith("/"):
        return None

    present = source_has(cfg, group, name)

    if present is None:
        return f"source check failed for {name}"

    if present:
        log.warning(
            "SKIP DELETE %s: object exists again at the source",
            name,
        )
        return None

    target = f"{cfg['dst_remote']}:{group}/{name}"
    cmd = ["rclone", "deletefile", target]

    log.info("DELETE TARGET=%s", target)
    log.info("DELETE CMD=%s", cmd)

    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    log.info(
        "DELETE name=%s RC=%s",
        name,
        res.returncode,
    )

    if res.returncode == 0:
        return None

    if (
        res.returncode in (3, 4)
        or "not found" in (res.stderr or "").lower()
    ):
        log.info(
            "DELETE %s: already absent at the destination",
            name,
        )
        return None

    error = (
        res.stderr
        or res.stdout
        or f"delete failed for {name}"
    )[-2000:]

    log.error(
        "DELETE FAILED name=%s error=%s",
        name,
        error,
    )
    return error


def delete_objects(cfg, group, names):
    """Delete destination objects using bounded concurrency."""
    delete_names = [
        name
        for name in names
        if not name.endswith("/")
    ]

    if not delete_names:
        return None

    log.info(
        "PARALLEL DELETE START group=%s files=%s concurrency=%s",
        group,
        len(delete_names),
        DELETE_CONCURRENCY,
    )

    errors = []

    with ThreadPoolExecutor(
        max_workers=DELETE_CONCURRENCY
    ) as executor:
        futures = {
            executor.submit(
                delete_one_object,
                cfg,
                group,
                name,
            ): name
            for name in delete_names
        }

        for future in as_completed(futures):
            name = futures[future]

            try:
                error = future.result()
            except Exception as exc:
                error = f"{name}: {type(exc).__name__}: {exc}"
                log.exception(
                    "DELETE UNHANDLED ERROR name=%s",
                    name,
                )

            if error:
                errors.append(error)

    log.info(
        "PARALLEL DELETE END group=%s files=%s errors=%s",
        group,
        len(delete_names),
        len(errors),
    )

    if errors:
        return "; ".join(errors[:10])[-2000:]

    return None

def process_batch(conn, cfg, batch_id, group):
    ev, bt, mp, nc = cfg["events"], cfg["batches"], cfg["map"], cfg["name_col"]
    with conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT e.event_id, e.{nc}, COALESCE(e.operation,'CREATE') "
            f"FROM {ev} e JOIN {mp} m ON m.{cfg['map_col']} = e.event_id WHERE m.batch_id=%s",
            (batch_id,))
        files = cur.fetchall()
    ids = [f[0] for f in files]
    create_files = [f[1] for f in files if f[2] == "CREATE"]
    delete_names = [f[1] for f in files if f[2] == "DELETE"]
    err = None

    # Same object named by both a CREATE and a DELETE in this batch: the source decides.
    for name in set(create_files) & set(delete_names):
        present = source_has(cfg, group, name)
        if present is None:
            err = f"source check failed for {name}"
            break
        if present:
            delete_names = [n for n in delete_names if n != name]
        else:
            create_files = [n for n in create_files if n != name]

    if err is None and create_files:
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as tf:
            tf.write("\n".join(create_files))
            list_path = tf.name
        try:
            cmd = ["rclone", "copy", f"{cfg['src_remote']}:{group}", f"{cfg['dst_remote']}:{group}",
                   "--files-from", list_path, "--log-level", "INFO", *RCLONE_FLAGS]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                err = (res.stderr or "rclone copy failed")[-2000:]
        finally:
            os.unlink(list_path)

    if err is None and delete_names:
        err = delete_objects(cfg, group, delete_names)

    with conn, conn.cursor() as cur:
        if err is None:
            cur.execute(
                f"UPDATE {ev} SET status='COMPLETED', processed_at=NOW(), worker_name=%s, error_message=NULL "
                f"WHERE event_id = ANY(%s)", (WORKER_NAME, ids))
            cur.execute(f"UPDATE {bt} SET status='COMPLETED', completed_at=NOW() WHERE batch_id=%s", (batch_id,))
            return
        # Failure: count the attempt, fail files at the retry limit, return the rest to NEW.
        cur.execute(
            f"UPDATE {ev} SET retry_count=retry_count+1, worker_name=%s, error_message=%s "
            f"WHERE event_id = ANY(%s)", (WORKER_NAME, err, ids))
        cur.execute(f"UPDATE {ev} SET status='FAILED' WHERE event_id = ANY(%s) AND retry_count >= %s",
                    (ids, MAX_RETRY))
        cur.execute(f"UPDATE {ev} SET status='NEW' WHERE event_id = ANY(%s) AND status='BATCHED'", (ids,))
        cur.execute(f"SELECT COUNT(*) FROM {ev} WHERE event_id = ANY(%s) AND status='FAILED'", (ids,))
        failed = cur.fetchone()[0]
        cur.execute(f"UPDATE {bt} SET status=%s, completed_at=NOW() WHERE batch_id=%s",
                    ("FAILED" if failed else "RETRIED", batch_id))
        log.error("batch %s failed: %s", batch_id, err[:300])


def run_worker(cfg):
    conn = db()
    while True:
        try:
            claimed = claim_batch(conn, cfg)
            log.info("claimed=%s", claimed)
            if not claimed:
                time.sleep(POLL_SEC)
                continue
            log.info("starting batch=%s", claimed[0])
            process_batch(conn, cfg, *claimed)
            log.info("completed batch=%s", claimed[0])
        except psycopg2.Error:
            log.exception("db error, reconnecting")
            time.sleep(POLL_SEC)
            try:
                conn.close()
            except Exception:
                pass
            conn = db()
        except Exception:
            log.exception("worker error")
            time.sleep(POLL_SEC)