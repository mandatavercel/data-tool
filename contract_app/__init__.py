"""Mandata Contract Builder — 표준 계약서 생성 도구.

여러 양식(템플릿)을 한 곳에서 관리하고, 질문지 답변으로 .docx 초안을 자동 생성한다.
신규 양식 추가는 contract_app/templates/<key>/ 폴더에 manifest.json + schema.json + template.docx
세 파일만 넣으면 된다.
"""
__all__ = ["registry", "generator"]
