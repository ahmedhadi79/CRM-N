import os
import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone
import logging
import zipfile
import io
import re
import unicodedata
import gzip

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# ===== Config =====
BUCKET_NAME = os.environ["S3_BUCKET"]
PREFIX = "data-lake/"
FILENAME_PREFIX = "Tracking_Winback_ScreenVisits_IAS_ABTest"
ATHENA_TABLE = "tracking_winback_screenvisits_ias_abtest"
DATABASE = "datalake_raw"
DAYS_BACKLOG = 5

# Chunking & CSV read behavior
CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", "250000"))
CSV_READ_OPTS = {
    "sep": ",",
    "quotechar": "\"",
    "encoding": "utf-8",
    "on_bad_lines": "skip",
    "low_memory": True
}
# If you KNOW your schema, define it here to lock types (recommended for production)
# Example: {"id": "string", "created_at": "timestamp[ns]", "value": "float64"}
DTYPE_HINTS: dict[str, str] | None = None

# Writing behavior
S3_RAW = os.environ["S3_RAW"]
OVERWRITE_PARTITIONS = os.getenv("OVERWRITE_PARTITIONS", "0") in {"1", "true", "TRUE", "yes", "Yes"}

# ==================

def _normalize_colname(name: str) -> str:
    # ascii, lowercase, underscores
    n = unicodedata.normalize("NFKD", name)
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"[^0-9a-zA-Z]+", "_", n).strip("_").lower()
    return n or "col"

def _standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names
    df = df.rename(columns={c: _normalize_colname(c) for c in df.columns})
    # stabilize dtypes (best-effort if DTYPE_HINTS not provided)
    if DTYPE_HINTS:
        # map pandas/pyarrow-ish strings to pandas dtype
        # pandas can accept "string", "Int64", "Float64", "boolean", "datetime64[ns]"
        # quick mapper:
        mapper = {
            "string": "string",
            "int64": "Int64",
            "float64": "Float64",
            "bool": "boolean",
            "boolean": "boolean",
            "datetime64[ns]": "datetime64[ns]",
            "timestamp[ns]": "datetime64[ns]",
        }
        apply_map = {k: mapper.get(v, v) for k, v in DTYPE_HINTS.items() if k in df.columns}
        for col, dt in apply_map.items():
            try:
                if dt == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
                else:
                    df[col] = df[col].astype(dt)
            except Exception:
                logger.warning("Fell back to string for column %s", col, exc_info=True)
                df[col] = df[col].astype("string")
    else:
        # no hints: coerce with convert_dtypes for consistency across chunks
        df = df.convert_dtypes()
    return df

def get_files_by_date_range(bucket_name: str, prefix: str, days_backlog: int):
    """Return [(key, file_date)] for files matching prefix within last N days (by filename date)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    regex = re.compile(rf"{re.escape(FILENAME_PREFIX)}.*_(\d{{2}}_\d{{2}}_\d{{4}})\.zip$")
    today_utc = datetime.now(timezone.utc).date()

    files: list[tuple[str, datetime.date]] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            base = os.path.basename(key)
            m = regex.search(base)
            if not m:
                continue
            try:
                file_date = datetime.strptime(m.group(1), "%d_%m_%Y").date()
            except ValueError:
                logger.warning("Skipping file due to invalid date format: %s", key)
                continue
            if (today_utc - file_date).days <= days_backlog:
                files.append((key, file_date))

    logger.info("Filtered %d file(s) within last %s day(s).", len(files), days_backlog)
    return sorted(files, key=lambda x: x[1])  # older first

def _yield_csv_chunks_from_bytes(data: bytes, chunksize: int):
    """Yield CSV chunks trying common encodings."""
    encodings = [CSV_READ_OPTS.get("encoding", "utf-8"),
                 "utf-8-sig", "utf-16", "cp1252", "latin1"]
    base = {k: v for k, v in CSV_READ_OPTS.items() if k != "encoding"}
    for enc in encodings:
        bio = io.BytesIO(data)
        try:
            reader = pd.read_csv(bio, chunksize=chunksize, encoding=enc, **base)
            # iterate to prove the encoding works
            for chunk in reader:
                yield chunk
            return
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, "unable to decode with tried encodings")

def iter_csv_chunks_from_s3_auto(bucket_name: str, key: str, chunksize: int):
    """
    Auto-detect ZIP / GZIP / plain CSV / Excel (inside ZIP) and yield (name, chunk_df).
    """
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = obj["Body"].read()
    ct = obj.get("ContentType", "unknown")
    logger.info("Reading S3 object key=%s content_type=%s size=%d bytes", key, ct, len(body))

    bio = io.BytesIO(body)

    # ZIP (robust detection)
    if zipfile.is_zipfile(bio):
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            names = [n for n in z.namelist() if not n.endswith("/")]
            csvs = [n for n in names if n.lower().endswith(".csv")]
            excels = [n for n in names if n.lower().endswith((".xlsx", ".xls"))]

            if csvs:
                for csv_name in csvs:
                    with z.open(csv_name) as f:
                        data = f.read()
                        for chunk in _yield_csv_chunks_from_bytes(data, chunksize):
                            yield csv_name, chunk
                return

            if excels:
                # Try Excel (requires openpyxl for .xlsx)
                for xl_name in excels:
                    with z.open(xl_name) as f:
                        xldata = f.read()
                    try:
                        # Pandas will read from BytesIO; engine picked automatically if available
                        df = pd.read_excel(io.BytesIO(xldata))
                        # chunk the dataframe manually to keep the contract consistent
                        if len(df) == 0:
                            continue
                        for start in range(0, len(df), chunksize):
                            yield xl_name, df.iloc[start:start+chunksize]
                        return
                    except Exception as e:
                        logger.warning("Excel inside ZIP not readable (%s): %s", xl_name, e, exc_info=True)

            raise ValueError(f"No readable CSV/Excel files found inside zip: {key}")

    # GZIP (magic 1f 8b)
    if body[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
            data = gz.read()
            for chunk in _yield_csv_chunks_from_bytes(data, chunksize):
                yield os.path.basename(key).replace(".gz", ""), chunk
        return

    # Plain CSV (or mislabeled)
    for chunk in _yield_csv_chunks_from_bytes(body, chunksize):
        yield os.path.basename(key), chunk

def write_chunk_to_s3(
    df: pd.DataFrame,
    athena_table: str,
    filename_prefix: str,
    partition_date,
    mode: str,
    s3_bucket: str | None = None
):
    """Write one chunk to parquet; partition by year/month/day + date."""
    if s3_bucket is None:
        s3_bucket = S3_RAW

    if not isinstance(partition_date, str):
        date_str = partition_date.strftime("%Y-%m-%d")
        y, m, d = partition_date.year, partition_date.month, partition_date.day
    else:
        date_str = partition_date
        y, m, d = map(int, date_str.split("-"))

    out = df.copy()
    out["date"] = date_str
    out["year"] = y
    out["month"] = m
    out["day"] = d

    path = f"s3://{s3_bucket}/{athena_table}/"
    return wr.s3.to_parquet(
        df=out,
        path=path,
        dataset=True,
        index=False,
        partition_cols=["year", "month", "day"],
        database=DATABASE,
        table=athena_table,
        mode=mode,
        filename_prefix=filename_prefix,
        schema_evolution=True,
        use_threads=True
    )

def process_file(key: str, file_date, overwritten_partitions: set[str]):
    """Stream a ZIP's CSV(s) in chunks and write to S3 with stable schema."""
    logger.info("Processing: %s", key)
    base_prefix = os.path.splitext(os.path.basename(key))[0]

    # decide mode: first write per partition can overwrite (if enabled), then append
    part_key = (isinstance(file_date, str) and file_date) or file_date.strftime("%Y-%m-%d")
    first_chunk_mode = "append"
    if OVERWRITE_PARTITIONS and part_key not in overwritten_partitions:
        first_chunk_mode = "overwrite_partitions"

    chunks = 0
    for csv_name, chunk in iter_csv_chunks_from_s3_auto(BUCKET_NAME, key, CHUNK_ROWS):
        chunk = _standardize_df(chunk)
        mode = first_chunk_mode if chunks == 0 else "append"
        res = write_chunk_to_s3(
            df=chunk,
            athena_table=ATHENA_TABLE,
            filename_prefix=f"{base_prefix}/{os.path.splitext(os.path.basename(csv_name))[0]}_",
            partition_date=file_date,
            mode=mode,
        )
        logger.debug("write result: %r", res)
        chunks += 1

    if OVERWRITE_PARTITIONS and chunks > 0:
        overwritten_partitions.add(part_key)

    logger.info("Wrote %d chunk(s) for %s", chunks, key)
    return chunks

def lambda_handler(event, context):
    logger.info("Starting daily ingestion process...")

    files = get_files_by_date_range(BUCKET_NAME, PREFIX, DAYS_BACKLOG)
    if not files:
        logger.warning("No files found for the specified date range.")
        return {"status": "no_files"}

    processed_files = 0
    processed_chunks = 0
    failed = []
    overwritten_partitions: set[str] = set()

    for key, file_date in files:
        try:
            chunks = process_file(key, file_date, overwritten_partitions)
            processed_files += 1
            processed_chunks += chunks
        except Exception:
            logger.exception("Error processing file %s", key)
            failed.append(key)

    logger.info("Done. files=%d chunks=%d failed=%d",
                processed_files, processed_chunks, len(failed))

    return {
        "status": "success" if processed_files and not failed else "partial",
        "files": processed_files,
        "chunks": processed_chunks,
        "failed": failed,
        "overwrite_partitions": OVERWRITE_PARTITIONS
    }
