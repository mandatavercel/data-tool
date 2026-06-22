"""Mandata 마켓 데이터 sub-app.

한국 주식의 가격·거래·외국인 보유 데이터를 효율적으로 탐색하고
임의 종목 묶음을 CSV·Excel·JSON으로 추출.

데이터 소스 (모두 무료):
- mandata_kr.Identifier   : KR 주식 마스터 (지수 멤버십, ISIN, BBG, RIC, 섹터)
- pykrx                   : KRX 공식 OHLCV·외국인 보유·거래대금·시가총액
- yfinance (fallback only): KRX가 막힐 때 EOD 대체용
"""
