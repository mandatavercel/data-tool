"""
modules/mapping/translation_db.py

브랜드·제품 영문화 파이프라인 SQLite 저장소.

테이블:
  brand              — 정규화된 브랜드 (name_kr 유니크, name_en 확정 영문명)
  product            — 정규화된 제품 (name_kr 유니크, 분해 속성, name_en_assembled)
  name_candidate     — 영문명 후보 + 출처/신뢰도/검수 이력
  product_attribute  — 제품 속성 분해 보조 (한 행에 안 담는 경우용)

저장 위치: mapping_app/translation.sqlite3
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


# DB 파일 위치 — mapping_app 폴더 아래
DB_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "mapping_app" / "translation.sqlite3"
)


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS brand (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name_kr             TEXT NOT NULL UNIQUE,
    name_en             TEXT,                           -- 확정 영문명 (검수 후)
    company_corp_code   TEXT,                           -- DART corp_code (선택)
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name_kr             TEXT NOT NULL UNIQUE,
    name_en_assembled   TEXT,                           -- 속성에서 조립된 영문명
    gtin                TEXT,                           -- GTIN/바코드
    brand_id            INTEGER REFERENCES brand(id),
    base_product        TEXT,                           -- 분해 속성
    flavor              TEXT,
    format              TEXT,
    package_size        TEXT,
    variant             TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 카테고리(섹터/업종/분류) — LLM 으로 자연스러운 영문 매핑
CREATE TABLE IF NOT EXISTS category (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name_kr             TEXT NOT NULL UNIQUE,
    name_en             TEXT,                           -- 확정 영문명
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS name_candidate (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type         TEXT NOT NULL CHECK(entity_type IN ('brand','product','category')),
    entity_id           INTEGER NOT NULL,
    candidate_en        TEXT NOT NULL,
    source              TEXT NOT NULL,
        -- 'internal_db' | 'kipris' | 'gs1' | 'official_site' | 'romanizer' | 'llm' | 'manual'
    confidence          REAL NOT NULL DEFAULT 0.5,      -- 0.0 ~ 1.0
    raw_payload         TEXT,                            -- JSON 원본 응답
    is_selected         INTEGER NOT NULL DEFAULT 0,     -- BOOLEAN
    reviewer            TEXT,
    reviewed_at         TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_brand_name_kr        ON brand(name_kr);
CREATE INDEX IF NOT EXISTS idx_product_name_kr      ON product(name_kr);
CREATE INDEX IF NOT EXISTS idx_category_name_kr     ON category(name_kr);
CREATE INDEX IF NOT EXISTS idx_candidate_entity     ON name_candidate(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_candidate_selected   ON name_candidate(is_selected);
"""


# ── Connection helper ────────────────────────────────────────────────────────
@contextmanager
def connect(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Context manager — SQLite 커넥션 + 트랜잭션 자동 처리."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# 프로세스 수명 동안 한 DB 파일은 한 번만 스키마 실행 — 매 호출마다
# executescript 를 돌리면 SQLite I/O 비용이 누적되어 배치 처리에서 무시 못함.
_INIT_DONE: set[Path] = set()


def init_db(db_path: Path | None = None) -> None:
    """스키마 초기화 (멱등 + 프로세스 캐시). DB 파일·테이블이 없으면 생성.

    또한 옛 버전의 `name_candidate.CHECK(entity_type IN ('brand','product'))` 를
    감지하면 category 를 허용하도록 자동 마이그레이션한다.
    공백 포함된 옛 영문명도 언더바로 일괄 정규화.
    """
    path = (db_path or DB_PATH).resolve()
    if path in _INIT_DONE:
        return
    with connect(path) as conn:
        conn.executescript(SCHEMA_SQL)
        _migrate_candidate_check(conn)
        _migrate_normalize_en_spaces(conn)
    _INIT_DONE.add(path)


def _migrate_normalize_en_spaces(conn: sqlite3.Connection) -> None:
    """옛 데이터의 영문명 공백을 언더바로 일괄 정규화 (1회성, 멱등).

    회사 표준은 'Samsung_Electronics' 같이 공백 없는 형태인데, 초기 버전이
    LLM 응답을 그대로 저장한 흔적이 남아있을 수 있어 보정.
    """
    from modules.mapping.translation import normalize_en

    # candidate_en — 공백 포함 행 모두 정규화
    rows = conn.execute(
        "SELECT id, candidate_en FROM name_candidate "
        "WHERE candidate_en LIKE '% %'"
    ).fetchall()
    for r in rows:
        new_en = normalize_en(r["candidate_en"])
        if new_en and new_en != r["candidate_en"]:
            # 같은 (entity, source, normalized) 가 이미 있을 수 있으니 충돌 회피
            try:
                conn.execute(
                    "UPDATE name_candidate SET candidate_en = ? WHERE id = ?",
                    (new_en, r["id"]),
                )
            except sqlite3.IntegrityError:
                # 중복 제약 — 옛 row 삭제
                conn.execute("DELETE FROM name_candidate WHERE id = ?", (r["id"],))

    # 각 엔티티 테이블의 확정 영문명도 정규화
    for table, en_col in (("brand", "name_en"),
                          ("product", "name_en_assembled"),
                          ("category", "name_en")):
        rows = conn.execute(
            f"SELECT id, {en_col} AS en FROM {table} "
            f"WHERE {en_col} IS NOT NULL AND {en_col} LIKE '% %'"
        ).fetchall()
        for r in rows:
            new_en = normalize_en(r["en"])
            if new_en and new_en != r["en"]:
                conn.execute(
                    f"UPDATE {table} SET {en_col} = ?, "
                    f"updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (new_en, r["id"]),
                )


def _migrate_candidate_check(conn: sqlite3.Connection) -> None:
    """name_candidate 의 entity_type CHECK 제약을 검사 — category 를 허용하지 않으면 테이블 재생성."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='name_candidate'"
    ).fetchone()
    if not row or not row["sql"]:
        return
    schema = row["sql"]
    if "'category'" in schema or "\"category\"" in schema:
        return   # 이미 category 허용
    # 옛 스키마 — rename + 재생성 + 데이터 복사
    conn.execute("ALTER TABLE name_candidate RENAME TO name_candidate_old_v1")
    conn.executescript("""
    CREATE TABLE name_candidate (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type     TEXT NOT NULL CHECK(entity_type IN ('brand','product','category')),
        entity_id       INTEGER NOT NULL,
        candidate_en    TEXT NOT NULL,
        source          TEXT NOT NULL,
        confidence      REAL NOT NULL DEFAULT 0.5,
        raw_payload     TEXT,
        is_selected     INTEGER NOT NULL DEFAULT 0,
        reviewer        TEXT,
        reviewed_at     TIMESTAMP,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO name_candidate (
        id, entity_type, entity_id, candidate_en, source,
        confidence, raw_payload, is_selected, reviewer, reviewed_at, created_at
    )
    SELECT id, entity_type, entity_id, candidate_en, source,
           confidence, raw_payload, is_selected, reviewer, reviewed_at, created_at
    FROM name_candidate_old_v1;
    DROP TABLE name_candidate_old_v1;
    CREATE INDEX IF NOT EXISTS idx_candidate_entity   ON name_candidate(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_candidate_selected ON name_candidate(is_selected);
    """)


# ══════════════════════════════════════════════════════════════════════════════
# Brand CRUD
# ══════════════════════════════════════════════════════════════════════════════

def get_brand_by_kr(name_kr: str) -> dict | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM brand WHERE name_kr = ?", (name_kr,)
        ).fetchone()
        return dict(row) if row else None


def upsert_brand(
    name_kr: str,
    name_en: str | None = None,
    company_corp_code: str | None = None,
) -> int:
    """브랜드 upsert. id 반환."""
    init_db()
    with connect() as conn:
        existing = conn.execute(
            "SELECT id FROM brand WHERE name_kr = ?", (name_kr,)
        ).fetchone()
        if existing:
            sets, vals = [], []
            if name_en is not None:
                sets.append("name_en = ?"); vals.append(name_en)
            if company_corp_code is not None:
                sets.append("company_corp_code = ?"); vals.append(company_corp_code)
            if sets:
                sets.append("updated_at = CURRENT_TIMESTAMP")
                vals.append(existing["id"])
                conn.execute(
                    f"UPDATE brand SET {', '.join(sets)} WHERE id = ?", vals
                )
            return existing["id"]
        cur = conn.execute(
            "INSERT INTO brand (name_kr, name_en, company_corp_code) VALUES (?, ?, ?)",
            (name_kr, name_en, company_corp_code),
        )
        return cur.lastrowid


def upsert_category(name_kr: str, name_en: str | None = None) -> int:
    """카테고리 upsert. id 반환."""
    init_db()
    with connect() as conn:
        existing = conn.execute(
            "SELECT id FROM category WHERE name_kr = ?", (name_kr,)
        ).fetchone()
        if existing:
            if name_en is not None:
                conn.execute(
                    "UPDATE category SET name_en = ?, "
                    "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (name_en, existing["id"]),
                )
            return existing["id"]
        cur = conn.execute(
            "INSERT INTO category (name_kr, name_en) VALUES (?, ?)",
            (name_kr, name_en),
        )
        return cur.lastrowid


# ══════════════════════════════════════════════════════════════════════════════
# Product CRUD
# ══════════════════════════════════════════════════════════════════════════════

PRODUCT_ATTR_KEYS = (
    "base_product", "flavor", "format", "package_size", "variant",
    "gtin", "brand_id", "name_en_assembled",
)


def get_product_by_kr(name_kr: str) -> dict | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM product WHERE name_kr = ?", (name_kr,)
        ).fetchone()
        return dict(row) if row else None


def upsert_product(name_kr: str, attributes: dict | None = None) -> int:
    """제품 upsert. attributes = {base_product, flavor, format, package_size, variant, gtin, brand_id, name_en_assembled}"""
    init_db()
    attrs = attributes or {}
    with connect() as conn:
        existing = conn.execute(
            "SELECT id FROM product WHERE name_kr = ?", (name_kr,)
        ).fetchone()
        if existing:
            sets, vals = [], []
            for k in PRODUCT_ATTR_KEYS:
                if k in attrs and attrs[k] is not None:
                    sets.append(f"{k} = ?"); vals.append(attrs[k])
            if sets:
                sets.append("updated_at = CURRENT_TIMESTAMP")
                vals.append(existing["id"])
                conn.execute(
                    f"UPDATE product SET {', '.join(sets)} WHERE id = ?", vals
                )
            return existing["id"]
        cur = conn.execute("""
            INSERT INTO product (
                name_kr, name_en_assembled, gtin, brand_id,
                base_product, flavor, format, package_size, variant
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name_kr,
            attrs.get("name_en_assembled"),
            attrs.get("gtin"),
            attrs.get("brand_id"),
            attrs.get("base_product"),
            attrs.get("flavor"),
            attrs.get("format"),
            attrs.get("package_size"),
            attrs.get("variant"),
        ))
        return cur.lastrowid


# ══════════════════════════════════════════════════════════════════════════════
# Name candidate CRUD
# ══════════════════════════════════════════════════════════════════════════════

VALID_SOURCES = {
    "internal_db", "kipris", "gs1", "official_site",
    "romanizer", "llm", "manual",
}

# entity_type → (table_name, en_column_name) 매핑
_ENTITY_TO_TABLE = {
    "brand":    ("brand",    "name_en"),
    "product":  ("product",  "name_en_assembled"),
    "category": ("category", "name_en"),
}


def _entity_meta(entity_type: str) -> tuple[str, str] | None:
    return _ENTITY_TO_TABLE.get(entity_type)


def add_candidate(
    entity_type: str,
    entity_id: int,
    candidate_en: str,
    source: str,
    confidence: float = 0.5,
    raw_payload: dict | None = None,
) -> int:
    """영문명 후보 추가. 동일 (entity, source, candidate_en) 이면 신뢰도만 갱신.
    저장 직전에 공백→언더바 정규화 자동 적용 (회사 표준 underscore 양식).
    """
    if entity_type not in _ENTITY_TO_TABLE:
        raise ValueError(f"invalid entity_type: {entity_type}")
    if source not in VALID_SOURCES:
        raise ValueError(f"invalid source: {source}")
    # 회사 표준: 영문명에 공백 없음 — 언더바 통일
    from modules.mapping.translation import normalize_en
    candidate_en = normalize_en(candidate_en)
    if not candidate_en:
        return -1
    init_db()
    with connect() as conn:
        existing = conn.execute("""
            SELECT id FROM name_candidate
            WHERE entity_type = ? AND entity_id = ?
              AND source = ? AND candidate_en = ?
        """, (entity_type, entity_id, source, candidate_en)).fetchone()
        if existing:
            conn.execute(
                "UPDATE name_candidate SET confidence = ?, raw_payload = ? WHERE id = ?",
                (confidence, json.dumps(raw_payload) if raw_payload else None,
                 existing["id"]),
            )
            return existing["id"]
        cur = conn.execute("""
            INSERT INTO name_candidate (
                entity_type, entity_id, candidate_en,
                source, confidence, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            entity_type, entity_id, candidate_en, source, confidence,
            json.dumps(raw_payload) if raw_payload else None,
        ))
        return cur.lastrowid


def list_candidates(entity_type: str, entity_id: int) -> list[dict]:
    init_db()
    with connect() as conn:
        rows = conn.execute("""
            SELECT * FROM name_candidate
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY is_selected DESC, confidence DESC, created_at DESC
        """, (entity_type, entity_id)).fetchall()
        return [dict(r) for r in rows]


def select_candidate(candidate_id: int, reviewer: str = "user") -> bool:
    """후보를 확정 — 같은 엔티티의 다른 후보는 is_selected=0 처리,
    선택된 후보의 영문명을 brand/product/category 테이블에도 반영.
    """
    init_db()
    with connect() as conn:
        row = conn.execute(
            "SELECT entity_type, entity_id, candidate_en "
            "FROM name_candidate WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if not row:
            return False

        # 다른 후보 unselect
        conn.execute("""
            UPDATE name_candidate SET is_selected = 0
            WHERE entity_type = ? AND entity_id = ? AND id != ?
        """, (row["entity_type"], row["entity_id"], candidate_id))

        # 이 후보 selected
        conn.execute("""
            UPDATE name_candidate
            SET is_selected = 1,
                reviewer = ?,
                reviewed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (reviewer, candidate_id))

        # entity 테이블에 확정 영문명 반영
        meta = _entity_meta(row["entity_type"])
        if not meta:
            return False
        table, en_col = meta
        conn.execute(
            f"UPDATE {table} SET {en_col} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (row["candidate_en"], row["entity_id"]),
        )
        return True


def get_confirmed_en(entity_type: str, name_kr: str) -> str | None:
    """확정된 영문명 lookup — 매핑 앱이 변환 시 사용."""
    meta = _entity_meta(entity_type)
    if not meta:
        return None
    init_db()
    table, en_col = meta
    with connect() as conn:
        row = conn.execute(
            f"SELECT {en_col} AS en FROM {table} WHERE name_kr = ?",
            (name_kr,),
        ).fetchone()
        return row["en"] if row and row["en"] else None


def get_confirmed_en_many(
    entity_type: str,
    names_kr: list[str],
    batch: int = 500,
) -> dict[str, str]:
    """배치 lookup — 확정 영문명만 dict 로 반환. 미확정 항목은 dict 에 없음.

    skip_confirmed 체크를 1 connection · N 개 IN 쿼리로 처리해
    N 번 connect/init 비용을 제거한다.
    """
    meta = _entity_meta(entity_type)
    if not names_kr or not meta:
        return {}
    init_db()
    table, en_col = meta
    out: dict[str, str] = {}
    uniq = list({n for n in names_kr if n})
    with connect() as conn:
        for start in range(0, len(uniq), batch):
            chunk = uniq[start:start + batch]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT name_kr, {en_col} AS en FROM {table} "
                f"WHERE name_kr IN ({placeholders}) "
                f"AND {en_col} IS NOT NULL AND {en_col} != ''",
                chunk,
            ).fetchall()
            for r in rows:
                out[r["name_kr"]] = r["en"]
    return out


def bulk_save_candidates(
    entity_type: str,
    rows: list[tuple[str, dict, list[dict]]],
) -> None:
    """배치 저장 — 1 connection · 1 transaction 으로 entity upsert + 후보 insert.

    Args:
        entity_type: 'brand' | 'product' | 'category'
        rows: [(name_kr, entity_attrs, [candidate_dict, ...]), ...]
            entity_attrs: brand/category 는 빈 dict, product 면 attributes
            candidate_dict: {candidate_en, source, confidence, raw_payload}

    개별 add_candidate(N×3 회) 대비 disk sync 횟수를 1/100 이하로 줄임.
    """
    meta = _entity_meta(entity_type)
    if not meta or not rows:
        return
    init_db()
    from modules.mapping.translation import normalize_en

    table, _en_col = meta
    is_brand    = entity_type == "brand"
    is_product  = entity_type == "product"
    is_category = entity_type == "category"

    with connect() as conn:
        # ── entity upsert (id 캐시) ─────────────────────────────────────────
        all_names = [n for n, _, _ in rows]
        placeholders = ",".join("?" * len(all_names))
        existing = {
            r["name_kr"]: r["id"]
            for r in conn.execute(
                f"SELECT id, name_kr FROM {table} WHERE name_kr IN ({placeholders})",
                all_names,
            ).fetchall()
        }
        name_to_id: dict[str, int] = dict(existing)

        for name_kr, attrs, _cands in rows:
            if name_kr in name_to_id:
                # update attributes (product only)
                if is_product and attrs:
                    sets, vals = [], []
                    for k in PRODUCT_ATTR_KEYS:
                        if k in attrs and attrs[k] is not None:
                            sets.append(f"{k} = ?"); vals.append(attrs[k])
                    if sets:
                        sets.append("updated_at = CURRENT_TIMESTAMP")
                        vals.append(name_to_id[name_kr])
                        conn.execute(
                            f"UPDATE {table} SET {', '.join(sets)} WHERE id = ?",
                            vals,
                        )
                continue
            if is_brand:
                cur = conn.execute(
                    "INSERT INTO brand (name_kr) VALUES (?)", (name_kr,),
                )
            elif is_category:
                cur = conn.execute(
                    "INSERT INTO category (name_kr) VALUES (?)", (name_kr,),
                )
            else:  # product
                cur = conn.execute("""
                    INSERT INTO product (
                        name_kr, name_en_assembled, gtin, brand_id,
                        base_product, flavor, format, package_size, variant
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    name_kr,
                    (attrs or {}).get("name_en_assembled"),
                    (attrs or {}).get("gtin"),
                    (attrs or {}).get("brand_id"),
                    (attrs or {}).get("base_product"),
                    (attrs or {}).get("flavor"),
                    (attrs or {}).get("format"),
                    (attrs or {}).get("package_size"),
                    (attrs or {}).get("variant"),
                ))
            name_to_id[name_kr] = cur.lastrowid

        # ── 후보 upsert ────────────────────────────────────────────────────
        for name_kr, _attrs, cands in rows:
            ent_id = name_to_id.get(name_kr)
            if not ent_id:
                continue
            for c in cands:
                cand_en = normalize_en(c.get("candidate_en") or "")
                if not cand_en:
                    continue
                src = c.get("source")
                if src not in VALID_SOURCES:
                    continue
                conf = float(c.get("confidence", 0.5))
                payload = c.get("raw_payload")
                payload_json = json.dumps(payload) if payload else None
                existing_row = conn.execute("""
                    SELECT id FROM name_candidate
                    WHERE entity_type = ? AND entity_id = ?
                      AND source = ? AND candidate_en = ?
                """, (entity_type, ent_id, src, cand_en)).fetchone()
                if existing_row:
                    conn.execute(
                        "UPDATE name_candidate SET confidence = ?, raw_payload = ? "
                        "WHERE id = ?",
                        (conf, payload_json, existing_row["id"]),
                    )
                else:
                    conn.execute("""
                        INSERT INTO name_candidate (
                            entity_type, entity_id, candidate_en,
                            source, confidence, raw_payload
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (entity_type, ent_id, cand_en, src, conf, payload_json))


def bulk_select_top(entity_type: str, reviewer: str = "auto") -> int:
    """미확정 엔티티들에 대해 confidence 1순위 후보를 자동 선택.

    Returns: 자동 확정된 엔티티 수.
    """
    meta = _entity_meta(entity_type)
    if not meta:
        return 0
    init_db()
    table, en_col = meta
    n_done = 0
    with connect() as conn:
        # 아직 영문 확정 안 된 엔티티 id 목록
        rows = conn.execute(
            f"SELECT id FROM {table} "
            f"WHERE {en_col} IS NULL OR {en_col} = ''"
        ).fetchall()
        for r in rows:
            ent_id = r["id"]
            top = conn.execute("""
                SELECT id, candidate_en FROM name_candidate
                WHERE entity_type = ? AND entity_id = ?
                ORDER BY confidence DESC, created_at DESC
                LIMIT 1
            """, (entity_type, ent_id)).fetchone()
            if not top:
                continue
            # select 처리
            conn.execute("""
                UPDATE name_candidate SET is_selected = 0
                WHERE entity_type = ? AND entity_id = ?
            """, (entity_type, ent_id))
            conn.execute("""
                UPDATE name_candidate
                SET is_selected = 1, reviewer = ?, reviewed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (reviewer, top["id"]))
            conn.execute(
                f"UPDATE {table} SET {en_col} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (top["candidate_en"], ent_id),
            )
            n_done += 1
    return n_done


def purge_partial_korean_candidates(entity_type: str = "product") -> dict:
    """후보 중 한글이 섞인 candidate_en 또는 옛 규칙기반(official_site) 후보를
    모두 삭제 + 해당 엔티티의 확정 영문도(한글 포함 시) null 처리.

    제품 영문화에서 규칙 기반이 'Nongshim 신라면 Big Bowl' 같은 부분-한글을
    저장해뒀을 가능성이 있어, DB 청소용으로 제공. official_site (규칙기반) 은
    부분 영문화 위험이 크므로 한글 여부와 무관하게 일괄 삭제.

    Returns: {"candidates_deleted": int, "entities_unconfirmed": int}
    """
    import re as _re
    _HG = _re.compile(r"[가-힣ᄀ-ᇿ㄰-㆏]")
    meta = _entity_meta(entity_type)
    if not meta:
        return {"candidates_deleted": 0, "entities_unconfirmed": 0}
    init_db()
    table, en_col = meta
    with connect() as conn:
        # 한글 섞인 후보 + 규칙기반(official_site) 후보 조회
        rows = conn.execute("""
            SELECT id, entity_id, candidate_en, source FROM name_candidate
            WHERE entity_type = ?
        """, (entity_type,)).fetchall()
        bad_ids: list[int] = []
        bad_entity_ids: set[int] = set()
        for r in rows:
            has_hg = bool(_HG.search(r["candidate_en"] or ""))
            is_rule = (r["source"] == "official_site")
            if has_hg or is_rule:
                bad_ids.append(r["id"])
                bad_entity_ids.add(r["entity_id"])
        if not bad_ids:
            return {"candidates_deleted": 0, "entities_unconfirmed": 0}
        # 삭제
        conn.executemany(
            "DELETE FROM name_candidate WHERE id = ?",
            [(i,) for i in bad_ids],
        )
        # 해당 엔티티 중 확정 영문에 한글이 섞여 있으면 null 처리
        ent_rows = conn.execute(
            f"SELECT id, {en_col} AS en FROM {table} "
            f"WHERE id IN ({','.join('?' * len(bad_entity_ids))})",
            list(bad_entity_ids),
        ).fetchall()
        unconfirmed = 0
        for er in ent_rows:
            if er["en"] and _HG.search(er["en"]):
                conn.execute(
                    f"UPDATE {table} SET {en_col} = NULL, "
                    f"updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (er["id"],),
                )
                unconfirmed += 1
        return {
            "candidates_deleted": len(bad_ids),
            "entities_unconfirmed": unconfirmed,
        }


def clear_all_selections(entity_type: str | None = None) -> int:
    """모든 확정 취소 (entity_type=None 이면 전체)."""
    init_db()
    n_cleared = 0
    targets = [entity_type] if entity_type else list(_ENTITY_TO_TABLE.keys())
    with connect() as conn:
        for et in targets:
            meta = _entity_meta(et)
            if not meta:
                continue
            table, en_col = meta
            cur = conn.execute(
                "UPDATE name_candidate SET is_selected = 0, "
                "reviewer = NULL, reviewed_at = NULL "
                "WHERE entity_type = ? AND is_selected = 1",
                (et,),
            )
            n_cleared += cur.rowcount
            conn.execute(
                f"UPDATE {table} SET {en_col} = NULL, "
                f"updated_at = CURRENT_TIMESTAMP WHERE {en_col} IS NOT NULL"
            )
    return n_cleared


def export_master_dict(entity_type: str = "brand"):
    """확정된 영문 마스터 사전 export — pandas DataFrame 반환.

    Returns columns: name_kr, name_en, source, confidence, reviewer, reviewed_at
    (확정된 항목만)
    """
    import pandas as pd
    meta = _entity_meta(entity_type)
    if not meta:
        return pd.DataFrame()
    init_db()
    table, en_col = meta
    with connect() as conn:
        rows = conn.execute(f"""
            SELECT
              b.name_kr           AS name_kr,
              b.{en_col}          AS name_en,
              c.source            AS source,
              c.confidence        AS confidence,
              c.reviewer          AS reviewer,
              c.reviewed_at       AS reviewed_at
            FROM {table} b
            LEFT JOIN name_candidate c
              ON c.entity_type = ? AND c.entity_id = b.id AND c.is_selected = 1
            WHERE b.{en_col} IS NOT NULL AND b.{en_col} != ''
            ORDER BY b.name_kr
        """, (entity_type,)).fetchall()
        return pd.DataFrame([dict(r) for r in rows])


def stats() -> dict:
    """간단한 통계 — UI 헤더에 표시용."""
    init_db()
    with connect() as conn:
        c = conn.execute("SELECT COUNT(*) FROM brand").fetchone()[0]
        c_en = conn.execute(
            "SELECT COUNT(*) FROM brand WHERE name_en IS NOT NULL AND name_en != ''"
        ).fetchone()[0]
        p = conn.execute("SELECT COUNT(*) FROM product").fetchone()[0]
        p_en = conn.execute(
            "SELECT COUNT(*) FROM product WHERE name_en_assembled IS NOT NULL AND name_en_assembled != ''"
        ).fetchone()[0]
        cat = conn.execute("SELECT COUNT(*) FROM category").fetchone()[0]
        cat_en = conn.execute(
            "SELECT COUNT(*) FROM category WHERE name_en IS NOT NULL AND name_en != ''"
        ).fetchone()[0]
        cand = conn.execute("SELECT COUNT(*) FROM name_candidate").fetchone()[0]
    return {
        "brand_total":    c,   "brand_confirmed":    c_en,
        "product_total":  p,   "product_confirmed":  p_en,
        "category_total": cat, "category_confirmed": cat_en,
        "candidate_total": cand,
    }
