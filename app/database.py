"""SQLite 持久化层。

四张表：
- chains：链条头信息与链尾缓存（tail_hash / current_holder 是事件表的可信投影）
- events：链上的全部事件，序号 0 为创世记录（文件摘要 + 首位持有人），
  序号 >=1 为移交记录。
- offers：待接收移交邀约。只保存一次性确认码的 SHA-256 摘要（code_digest），
  不保存明文；tail_hash 记录邀约发起时锁定的链尾，确认时必须仍然匹配。
  witness_threshold 为 NULL 表示无见证邀约（兼容存量数据），否则为通过阈值。
  cancel_digest 为可选一次性撤销码的 SHA-256 摘要（与 offer_id 做域分隔），
  NULL 表示该邀约不可撤销（含存量旧邀约）；cancelled_at 非 NULL 表示邀约
  已被撤销（与 accepted 互斥的存储终态，由写事务串行保证）。
- offer_witnesses：见证邀约的见证人。只保存一次性令牌的 SHA-256 摘要
  （token_digest，与 offer_id 做域分隔），不存明文；decision 为 NULL 表示
  尚未表态，同一见证人至多表态一次（令牌本身即一次性）。拒绝/过期/链尾推进
  等终止状态为派生状态，不额外改写邀约行。

防分叉保障：
1. 所有写操作在进程内串行锁 + SQLite `BEGIN IMMEDIATE` 中完成，
   追加前校验链尾哈希，校验与插入在同一事务内原子完成；
2. (chain_id, seq) 唯一、(chain_id, nonce) 唯一，由数据库兜底，
   任何竞争写入都不可能产生第二条分叉链或重放随机数。

建表语句全部为 CREATE ... IF NOT EXISTS，init_db() 可重复执行，
对已有数据库文件只做增量补表/补列，不触碰存量数据。
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from . import config

# 进程内写锁：保证“读链尾 -> 校验 -> 追加提交”整体串行
_write_lock = threading.RLock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chains (
    chain_id        TEXT PRIMARY KEY,
    file_hash       TEXT NOT NULL,
    first_holder    TEXT NOT NULL,
    tail_hash       TEXT NOT NULL,
    current_holder  TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    prev_hash     TEXT NOT NULL DEFAULT '',
    giver         TEXT NOT NULL DEFAULT '',
    receiver      TEXT NOT NULL DEFAULT '',
    nonce         TEXT NOT NULL DEFAULT '',
    event_hash    TEXT NOT NULL,
    canonical     TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    UNIQUE(chain_id, seq),
    UNIQUE(chain_id, nonce),
    FOREIGN KEY(chain_id) REFERENCES chains(chain_id)
);

CREATE INDEX IF NOT EXISTS idx_events_chain_seq
    ON events(chain_id, seq);

CREATE TABLE IF NOT EXISTS offers (
    offer_id           TEXT PRIMARY KEY,
    chain_id           TEXT NOT NULL,
    tail_hash          TEXT NOT NULL,
    receiver           TEXT NOT NULL,
    code_digest        TEXT NOT NULL,
    cancel_digest      TEXT,
    witness_threshold  INTEGER,
    status             TEXT NOT NULL DEFAULT 'pending',
    expires_at         TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    accepted_at        TEXT,
    accepted_seq       INTEGER,
    cancelled_at       TEXT,
    CHECK(status IN ('pending', 'accepted')),
    CHECK(witness_threshold IS NULL OR witness_threshold >= 1),
    FOREIGN KEY(chain_id) REFERENCES chains(chain_id)
);

CREATE INDEX IF NOT EXISTS idx_offers_chain
    ON offers(chain_id);

CREATE TABLE IF NOT EXISTS offer_witnesses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    offer_id        TEXT NOT NULL,
    witness_index   INTEGER NOT NULL,
    name            TEXT NOT NULL,
    token_digest    TEXT NOT NULL,
    decision        TEXT,
    decided_at      TEXT,
    CHECK(decision IS NULL OR decision IN ('approved', 'rejected')),
    UNIQUE(offer_id, witness_index),
    UNIQUE(offer_id, token_digest),
    FOREIGN KEY(offer_id) REFERENCES offers(offer_id)
);

CREATE INDEX IF NOT EXISTS idx_offer_witnesses_offer
    ON offer_witnesses(offer_id, witness_index);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=10.0,
        isolation_level=None,  # 手动事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def _get_conn() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """初始化（或校验）数据库结构。可重复执行。

    对更早版本创建的 offers 表增量补列 witness_threshold / cancel_digest /
    cancelled_at；存量邀约这些列均为 NULL：无见证邀约行为与升级前一致，
    无撤销码邀约不可撤销。
    """
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _get_conn() as conn:
        conn.executescript(_SCHEMA)
        offer_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(offers)").fetchall()
        }
        if "witness_threshold" not in offer_cols:
            conn.execute(
                "ALTER TABLE offers ADD COLUMN witness_threshold INTEGER"
            )
        if "cancel_digest" not in offer_cols:
            conn.execute("ALTER TABLE offers ADD COLUMN cancel_digest TEXT")
        if "cancelled_at" not in offer_cols:
            conn.execute("ALTER TABLE offers ADD COLUMN cancelled_at TEXT")


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """串行化的写事务：BEGIN IMMEDIATE 立即取得写锁，提交前不会被插队。"""
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    with _get_conn() as conn:
        conn.execute("BEGIN")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
