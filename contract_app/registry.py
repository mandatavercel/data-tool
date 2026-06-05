"""계약서 양식 레지스트리 — templates/ 폴더 스캔."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


@dataclass(frozen=True)
class TemplateMeta:
    key: str
    name: str
    type: str
    scope: str
    language: str
    version: str
    status: str  # active / draft / archived
    doc_code: str
    party_a_role: str
    party_b_role: str
    description: str
    template_file: str
    schema_file: str
    tags: list[str]
    folder: Path

    @property
    def template_path(self) -> Path:
        return self.folder / self.template_file

    @property
    def schema_path(self) -> Path:
        return self.folder / self.schema_file


def _load_one(folder: Path) -> TemplateMeta | None:
    mf = folder / "manifest.json"
    if not mf.exists():
        return None
    try:
        data = json.loads(mf.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return TemplateMeta(
        key=data.get("key") or folder.name,
        name=data.get("name") or folder.name,
        type=data.get("type", ""),
        scope=data.get("scope", ""),
        language=data.get("language", "ko"),
        version=data.get("version", "1.0"),
        status=data.get("status", "active"),
        doc_code=data.get("doc_code", ""),
        party_a_role=data.get("party_a_role", ""),
        party_b_role=data.get("party_b_role", ""),
        description=data.get("description", ""),
        template_file=data.get("template_file", "template.docx"),
        schema_file=data.get("schema_file", "schema.json"),
        tags=list(data.get("tags", [])),
        folder=folder,
    )


def list_templates(include_drafts: bool = False) -> list[TemplateMeta]:
    """templates/<key>/manifest.json 을 모두 읽어 리스트로 반환."""
    if not TEMPLATES_DIR.exists():
        return []
    out: list[TemplateMeta] = []
    for sub in sorted(TEMPLATES_DIR.iterdir()):
        if not sub.is_dir():
            continue
        meta = _load_one(sub)
        if meta is None:
            continue
        if meta.status != "active" and not include_drafts:
            continue
        out.append(meta)
    # type 우선, 그 다음 scope, version 역순
    out.sort(key=lambda m: (m.type, m.scope, -float(m.version or 0)))
    return out


def get_template(key: str) -> TemplateMeta | None:
    for m in list_templates(include_drafts=True):
        if m.key == key:
            return m
    return None


def load_schema(meta: TemplateMeta) -> dict[str, Any]:
    return json.loads(meta.schema_path.read_text(encoding="utf-8"))
