from __future__ import annotations

import csv
import hashlib
import math
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import pymysql


@dataclass(frozen=True)
class ImportPlan:
    table_name: str
    mode: str  # "wide" | "long"
    columns: list[str]  # for wide: SQL column names; for long: fixed schema columns
    source_relpath: str


_IDENT_RE = re.compile(r"[^0-9a-zA-Z_]")


def _sanitize_identifier(s: str) -> str:
    s = s.strip()
    s = s.replace(" ", "_")
    s = _IDENT_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def _db_quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _stable_short_hash(s: str, n: int = 10) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def _make_table_name(csv_path: Path, data_root: Path, prefix: str | None) -> str:
    rel = csv_path.relative_to(data_root).as_posix()
    stem = csv_path.stem
    parts = list(csv_path.relative_to(data_root).parts)
    # keep a bit of folder context to avoid collisions
    base = "__".join([*parts[:-1], stem])
    base = _sanitize_identifier(base)
    if prefix:
        base = _sanitize_identifier(prefix) + "__" + base

    # MariaDB identifier limit: 64 chars
    if len(base) > 58:
        base = base[:58].rstrip("_") + "_" + _stable_short_hash(rel, 5)
    return base


def _read_csv_header(csv_path: Path, encoding: str) -> list[str]:
    # Many of these files have very long first line; csv.reader still works streaming.
    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return header


def _has_duplicates(items: Sequence[str]) -> bool:
    seen: set[str] = set()
    for x in items:
        if x in seen:
            return True
        seen.add(x)
    return False


def _uniqueify_columns(raw_headers: Sequence[str]) -> list[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for h in raw_headers:
        base = _sanitize_identifier(h) or "col"
        n = counts.get(base, 0) + 1
        counts[base] = n
        out.append(base if n == 1 else f"{base}_{n}")
    return out


def _infer_plan(
    csv_path: Path,
    data_root: Path,
    *,
    encoding: str,
    mode: str,
    wide_max_columns: int,
    table_prefix: str | None,
) -> ImportPlan:
    rel = csv_path.relative_to(data_root).as_posix()
    raw_header = _read_csv_header(csv_path, encoding=encoding)

    if not raw_header:
        raise ValueError(f"CSV 空檔或無表頭: {rel}")

    forced = mode.lower()
    if forced not in {"auto", "wide", "long"}:
        raise ValueError("--mode 只能是 auto|wide|long")

    auto_long = _has_duplicates(raw_header) or (len(raw_header) > wide_max_columns)
    chosen_mode = (
        "long"
        if (forced == "long" or (forced == "auto" and auto_long))
        else "wide"
    )

    table_name = _make_table_name(csv_path, data_root, table_prefix)

    if chosen_mode == "wide":
        cols = _uniqueify_columns(raw_header)
        return ImportPlan(
            table_name=table_name,
            mode="wide",
            columns=cols,
            source_relpath=rel,
        )

    # long mode schema
    return ImportPlan(
        table_name=table_name,
        mode="long",
        columns=[
            "source_file",
            "row_idx",
            "col_idx",
            "col_name",
            "value_float",
            "value_str",
        ],
        source_relpath=rel,
    )


def _connect(db_cfg: dict[str, str], timeout_s: int) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=db_cfg["host"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        port=int(db_cfg["port"]),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
    )


def _exec(cursor, sql: str) -> None:
    cursor.execute(sql)


def _table_exists(cursor, table: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """,
        (table,),
    )
    return cursor.fetchone() is not None


def _drop_table(cursor, table: str) -> None:
    _exec(cursor, f"DROP TABLE {_db_quote_ident(table)};")


def _create_table_wide(cursor, table: str, columns: Sequence[str]) -> None:
    col_defs = ",\n  ".join(
        f"{_db_quote_ident(c)} TEXT NULL" for c in columns
    )
    sql = f"""
    CREATE TABLE {_db_quote_ident(table)} (
      `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      `source_file` VARCHAR(400) NOT NULL,
      {col_defs}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    _exec(cursor, sql)


def _create_table_long(cursor, table: str) -> None:
    sql = f"""
    CREATE TABLE {_db_quote_ident(table)} (
      `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      `source_file` VARCHAR(400) NOT NULL,
      `row_idx` INT NOT NULL,
      `col_idx` INT NOT NULL,
      `col_name` VARCHAR(128) NULL,
      `value_float` DOUBLE NULL,
      `value_str` TEXT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    _exec(cursor, sql)


def _iter_csv_rows(csv_path: Path, encoding: str) -> Iterator[list[str]]:
    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            yield row


def _try_float(s: str) -> float | None:
    s = s.strip()
    if s == "":
        return None
    try:
        v = float(s)
        # Reject NaN and Infinity — not valid MariaDB DOUBLE values
        return None if (math.isnan(v) or math.isinf(v)) else v
    except ValueError:
        return None


def _chunked(it: Iterable, size: int) -> Iterator[list]:
    buf: list = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _insert_wide(
    cursor,
    table: str,
    plan: ImportPlan,
    csv_path: Path,
    *,
    encoding: str,
    batch_size: int,
) -> int:
    columns = plan.columns
    quoted_cols = ", ".join(_db_quote_ident(c) for c in ["source_file", *columns])
    placeholders = ", ".join(["%s"] * (1 + len(columns)))
    sql = (
        f"INSERT INTO {_db_quote_ident(table)} "
        f"({quoted_cols}) VALUES ({placeholders});"
    )

    rows_iter = _iter_csv_rows(csv_path, encoding=encoding)
    header = next(rows_iter, None)
    if header is None:
        return 0

    inserted = 0
    rel = plan.source_relpath

    def gen_params() -> Iterator[tuple]:
        for row in rows_iter:
            # pad or truncate to header width
            if len(row) < len(columns):
                row = [*row, *([""] * (len(columns) - len(row)))]
            elif len(row) > len(columns):
                row = row[: len(columns)]
            yield (rel, *row)

    for chunk in _chunked(gen_params(), batch_size):
        cursor.executemany(sql, chunk)
        inserted += len(chunk)
    return inserted


def _insert_long(
    cursor,
    table: str,
    plan: ImportPlan,
    csv_path: Path,
    *,
    encoding: str,
    batch_size: int,
) -> int:
    quoted_cols = ", ".join(
        _db_quote_ident(c) for c in plan.columns
    )
    placeholders = ", ".join(["%s"] * len(plan.columns))
    sql = (
        f"INSERT INTO {_db_quote_ident(table)} "
        f"({quoted_cols}) VALUES ({placeholders});"
    )

    rows_iter = _iter_csv_rows(csv_path, encoding=encoding)
    header = next(rows_iter, None)
    if header is None:
        return 0

    rel = plan.source_relpath
    inserted = 0

    # For these datasets, header names are often all identical; keep original text anyway.
    def gen_params() -> Iterator[tuple]:
        for r_idx, row in enumerate(rows_iter, start=1):
            for c_idx, cell in enumerate(row, start=1):
                stripped = cell.strip() if cell else ""
                vf = _try_float(stripped)
                # Store NULL for empty cells and numeric values; text only for non-numeric, non-empty cells
                vs = None if (vf is not None or not stripped) else stripped[:4000]
                col_name = header[c_idx - 1] if (c_idx - 1) < len(header) else None
                # source_file, row_idx, col_idx, col_name, value_float, value_str
                yield (rel, r_idx, c_idx, col_name, vf, vs)

    for chunk in _chunked(gen_params(), batch_size):
        cursor.executemany(sql, chunk)
        inserted += len(chunk)
    return inserted


def _iter_csv_files(data_root: Path) -> list[Path]:
    return sorted([p for p in data_root.rglob("*.csv") if p.is_file()])


def _parse_env_file(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and (
            (value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")
        ):
            value = value[1:-1]
        values[key] = value
    return values


def _pick_setting(env_file_values: dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if os.environ.get(k):
            return os.environ[k]
        if env_file_values.get(k):
            return env_file_values[k]
    return default


def _pick_setting_with_source(
    env_file_values: dict[str, str], *keys: str, default: str = ""
) -> tuple[str, str]:
    """Return (value, source_label) where source is '環境變數', '.env 檔案', or '預設值'."""
    for k in keys:
        if os.environ.get(k):
            return os.environ[k], "環境變數"
        if env_file_values.get(k):
            return env_file_values[k], ".env 檔案"
    return default, "預設值"


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_db_config(env_file_values: dict[str, str]) -> tuple[dict[str, str], str]:
    host, cfg_src = _pick_setting_with_source(env_file_values, "MARIADB_HOST", "DB_HOST")
    cfg = {
        "host": host,
        "port": _pick_setting(env_file_values, "MARIADB_PORT", "DB_PORT", default="3306"),
        "user": _pick_setting(env_file_values, "MARIADB_USER", "DB_USER"),
        "password": _pick_setting(env_file_values, "MARIADB_PASSWORD", "DB_PASSWORD"),
        "database": _pick_setting(env_file_values, "MARIADB_DATABASE", "DB_NAME", "DB_DATABASE"),
    }
    missing = [k for k in ("host", "user", "password", "database") if not cfg[k]]
    if missing:
        return {}, f"缺少欄位: {', '.join(missing)}"
    return cfg, cfg_src


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    env_file = project_root / "env" / ".env"
    env_file_values = _parse_env_file(env_file)

    raw_data_root = _pick_setting(env_file_values, "IMPORT_DATA_ROOT", default="data")
    data_root_path = Path(raw_data_root)
    data_root = (
        data_root_path
        if data_root_path.is_absolute()
        else (project_root / data_root_path)
    ).resolve()
    if not data_root.exists():
        print(f"找不到資料夾: {data_root}", file=sys.stderr)
        return 2

    db_cfg, cfg_src = _resolve_db_config(env_file_values)
    if not db_cfg:
        print(
            f"缺少 MariaDB 連線資訊（{cfg_src}）。請在 env/.env 設定 MARIADB_HOST/MARIADB_USER/MARIADB_PASSWORD/MARIADB_DATABASE",
            file=sys.stderr,
        )
        return 2
    print(f"使用連線設定來源: {cfg_src}")

    mode = _pick_setting(env_file_values, "IMPORT_MODE", default="auto").lower()
    if mode not in {"auto", "wide", "long"}:
        print("IMPORT_MODE 必須是 auto、wide 或 long", file=sys.stderr)
        return 2
    wide_max_columns = _parse_int(_pick_setting(env_file_values, "IMPORT_WIDE_MAX_COLUMNS", default="900"), 900)
    if_exists = _pick_setting(env_file_values, "IMPORT_IF_EXISTS", default="skip").lower()
    if if_exists not in {"skip", "drop"}:
        print("IMPORT_IF_EXISTS 必須是 skip 或 drop", file=sys.stderr)
        return 2
    table_prefix = _pick_setting(env_file_values, "IMPORT_TABLE_PREFIX", default="")
    encoding = _pick_setting(env_file_values, "IMPORT_ENCODING", default="utf-8")
    timeout = _parse_int(_pick_setting(env_file_values, "IMPORT_TIMEOUT", default="30"), 30)
    batch_size = _parse_int(_pick_setting(env_file_values, "IMPORT_BATCH_SIZE", default="2000"), 2000)
    limit_files = _parse_int(_pick_setting(env_file_values, "IMPORT_LIMIT_FILES", default="0"), 0)

    csv_files = _iter_csv_files(data_root)
    if limit_files > 0:
        csv_files = csv_files[:limit_files]

    if not csv_files:
        print("沒有找到任何 .csv", file=sys.stderr)
        return 2

    cn = _connect(db_cfg, timeout_s=timeout)
    try:
        cur = cn.cursor()
        processed = 0
        failed = 0
        total_inserted = 0

        for csv_path in csv_files:
            plan = _infer_plan(
                csv_path,
                data_root,
                encoding=encoding,
                mode=mode,
                wide_max_columns=wide_max_columns,
                table_prefix=table_prefix or None,
            )

            table = plan.table_name
            table_created_this_run = False

            try:
                exists = _table_exists(cur, table)
                if exists and if_exists == "skip":
                    processed += 1
                    continue
                if exists and if_exists == "drop":
                    _drop_table(cur, table)
                    # DDL triggers implicit commit in MariaDB — cannot be rolled back

                if not _table_exists(cur, table):
                    if plan.mode == "wide":
                        _create_table_wide(cur, table, plan.columns)
                    else:
                        _create_table_long(cur, table)
                    table_created_this_run = True
                    # DDL triggers implicit commit — table now exists permanently

                if plan.mode == "wide":
                    inserted = _insert_wide(
                        cur,
                        table,
                        plan,
                        csv_path,
                        encoding=encoding,
                        batch_size=batch_size,
                    )
                else:
                    inserted = _insert_long(
                        cur,
                        table,
                        plan,
                        csv_path,
                        encoding=encoding,
                        batch_size=batch_size,
                    )

                cn.commit()
                processed += 1
                total_inserted += inserted
                print(
                    f"[OK] {plan.source_relpath} -> {db_cfg['database']}.{table} ({plan.mode}) inserted={inserted}"
                )

            except Exception as file_err:
                # Roll back uncommitted INSERTs for this file only
                try:
                    cn.rollback()
                except Exception:
                    pass
                # If this run created the table but INSERT failed, drop the orphaned empty table
                if table_created_this_run and _table_exists(cur, table):
                    try:
                        _drop_table(cur, table)
                        print(f"[WARN] 已清除本次建立的空表 {table}", file=sys.stderr)
                    except Exception:
                        pass
                failed += 1
                print(
                    f"[ERROR] {plan.source_relpath} 匯入失敗（已 rollback 本檔 INSERT，前面已成功的檔案不受影響）：{file_err}",
                    file=sys.stderr,
                )

        print(f"完成：files={processed}, failed={failed}, inserted_rows={total_inserted}")
        return 0 if failed == 0 else 1
    except Exception as e:
        try:
            cn.rollback()
        except Exception:
            pass
        print(f"連線或初始化失敗：{e}", file=sys.stderr)
        return 1
    finally:
        try:
            cn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

