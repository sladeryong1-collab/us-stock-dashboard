# -*- coding: utf-8 -*-
"""
나스닥100 + S&P500 종가/섹터/시가총액 수집 스크립트
(인터넷 제한이 없는 환경에서 도는 걸 가정합니다.)

동작 순서:
  1) tickers_cache.json이 있고 최신(TICKER_CACHE_MAX_AGE_DAYS 이내)이고
     "섹터 정보가 실제로 채워져 있으면" 그대로 사용.
     없거나 오래됐거나 섹터가 비어있으면 새로 받아옵니다:
       - 티커 + 종목명 + 섹터: S&P500은 SPY 홀딩스 파일(1순위)/위키피디아(대체),
         나스닥100은 위키피디아에서 가져옵니다.
       - 발행주식수: 종목마다 개별 조회가 필요해서(야후 파이낸스) 느리기 때문에,
         자주 안 바뀌는 값이라는 점을 이용해 이 캐시 갱신 주기(기본 7일)에만 받아옵니다.
  2) 종목을 배치(BATCH_SIZE)로 쪼개서 종가 + 전일대비 등락률을 수집
     (시가총액 = 캐시된 발행주식수 × 오늘 종가로 매일 새로 계산 — 종가는 어차피
     매일 받아오니까 시가총액 때문에 추가로 야후에 요청을 더 보내지는 않습니다)
  3) 결과를 data/prices.json 으로 저장 (index.html이 이 파일을 읽어서 표시)

실패한 배치는 재시도하고, 그래도 안 되면 해당 종목만 "실패" 표시하고 넘어갑니다.

──────────────────────────────────────────────────────────────
[2026-08-27 수정] 섹터가 전부 "-"로 나오는 문제 수정
  원인 1: SPY 홀딩스 파일(SPDR 공식 xlsx)의 "Sector" 컬럼 값이 이번 배포본에서는
          종목별로 실제 섹터명이 아니라 문자열 "-"만 들어있었습니다. 컬럼 자체는
          정상적으로 찾아졌기 때문에 예외가 안 나고 그대로 "-"가 저장돼버렸어요.
          → 이제 "-" 같은 값은 "섹터 없음(None)"으로 취급하고, 유효한 섹터 비율이
            너무 낮으면 SPY 소스를 실패로 간주해서 위키피디아로 자동 대체합니다.
  원인 2: 나스닥100 위키피디아 표 파싱이 실패해서(표 구조 인식 실패) 나스닥100
          전용 종목(ARM, MSTR, PDD 등)은 이름/섹터가 아예 None이었습니다.
          → 실패 시 각 표의 컬럼명을 로그로 남겨서 다음에 왜 실패했는지 바로
            보이게 했고, "90개 이상인 첫 표"가 아니라 "유효 티커가 가장 많이
            나온 표"를 선택하도록 완화해서 표 순서가 바뀌어도 더 잘 버티게
            했습니다.
  원인 3: tickers_cache.json 캐시는 "meta/shares 키가 있고 7일 이내면" 무조건
          재사용했기 때문에, 한 번 "-"로 잘못 저장되면 코드를 고쳐도 캐시
          때문에 최대 7일간 계속 "-"가 나왔습니다.
          → 캐시를 불러올 때 섹터가 실제로 채워져 있는 비율도 같이 확인해서,
            너무 낮으면 캐시가 최신이어도 다시 받아오도록 했습니다.
──────────────────────────────────────────────────────────────
"""

import io
import json
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

# ── 설정값 ──────────────────────────────────────────────
BATCH_SIZE = 50
BATCH_SLEEP = (3, 6)
MAX_RETRY = 3
RETRY_SLEEP = 20

SHARES_FETCH_WORKERS = 8  # 발행주식수 병렬 조회 동시 개수 (너무 크면 야후 쪽에서 막을 수 있어요)

ROOT = Path(__file__).resolve().parent.parent
TICKER_CACHE_PATH = ROOT / "tickers_cache.json"
TICKER_CACHE_MAX_AGE_DAYS = 7
OUTPUT_PATH = ROOT / "data" / "prices.json"

# 캐시(혹은 방금 받아온 메타데이터)의 섹터 값 중 이 비율 미만만 "유효"하면
# 못 믿을 데이터로 보고 다시 받아옵니다. (예: SPY 파일이 전부 "-"만 내려줄 때)
MIN_VALID_SECTOR_RATIO = 0.5

KST = timezone(timedelta(hours=9))

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
TICKER_PATTERN = re.compile(r"^[A-Z]{1,6}([.\-][A-Z]{1,2})?$")

# ICB(나스닥100 위키 표 기준) → GICS(S&P500 기준) 섹터 이름을 최대한 맞춰주기 위한 매핑.
# 두 지수에 동시에 속한 종목은 어차피 S&P500 쪽 GICS 섹터를 우선 쓰기 때문에, 이 매핑은
# "나스닥100에만 속한" 종목(ARM, MSTR, APP 등)의 섹터 표기를 통일하는 용도예요.
ICB_TO_GICS_SECTOR = {
    "Technology": "Information Technology",
    "Telecommunications": "Communication Services",
    "Basic Materials": "Materials",
    "Health Care": "Health Care",
    "Healthcare": "Health Care",
    "Consumer Discretionary": "Consumer Discretionary",
    "Consumer Staples": "Consumer Staples",
    "Industrials": "Industrials",
    "Financials": "Financials",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Real Estate": "Real Estate",
}

# 이런 값들은 "섹터가 있는 척하는 빈 값"으로 보고 None 취급합니다.
_INVALID_SECTOR_VALUES = {"-", "--", "", "n/a", "na", "none", "null", "nan"}


def _clean_sector(value) -> str | None:
    """SPY 파일이나 위키 표에서 막 뽑아온 섹터 원문 값을 정리합니다.
    "-" 처럼 실제로는 값이 없는데 빈 문자열이 아니라서 걸러지지 않던 값들을
    여기서 전부 None으로 통일해요."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in _INVALID_SECTOR_VALUES:
        return None
    return text


def _valid_sector_ratio(meta: dict[str, dict]) -> float:
    if not meta:
        return 0.0
    valid = sum(1 for info in meta.values() if _clean_sector(info.get("sector")))
    return valid / len(meta)


def _flatten_col(col) -> str:
    """DataFrame 컬럼명이 ('A','B') 같은 멀티인덱스 튜플이어도 문자열로 펴줍니다."""
    flat = " ".join(str(p) for p in col) if isinstance(col, tuple) else str(col)
    return re.sub(r"\[.*?\]", "", flat).strip().lower()


def _find_column(table: "pd.DataFrame", candidates: tuple[str, ...]):
    """컬럼명이 후보 목록(소문자 기준) 중 하나와 정확히 일치하면 그 컬럼을 반환합니다.
    표 구조가 조금 바뀌어도(대소문자, 각주, 멀티인덱스 등) 안 죽게 하려고 이렇게 넉넉하게 잡아요."""
    for col in table.columns:
        if _flatten_col(col) in candidates:
            return col
    return None


def get_sp500_records_from_spy() -> list[dict]:
    """SPY(State Street) 공식 보유종목 파일에서 S&P500 티커/종목명/섹터를 가져옵니다.
    위키피디아보다 신뢰도가 높은 1차 소스라 이걸 먼저 시도합니다.

    단, 이 파일의 "Sector" 컬럼이 (버전에 따라) 종목별 실제 섹터명 대신
    "-" 같은 자리표시자만 채워져 있을 때가 있어서, 그런 경우는 이 소스를
    실패로 간주하고 위키피디아 대체 경로로 넘어가도록 예외를 던집니다.
    """
    url = "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
    resp.raise_for_status()

    raw = pd.read_excel(io.BytesIO(resp.content), header=None)
    header_row = None
    for i, row in raw.iterrows():
        if row.astype(str).str.strip().eq("Ticker").any():
            header_row = i
            break
    if header_row is None:
        raise RuntimeError("SPY holdings 파일에서 'Ticker' 헤더 행을 찾지 못했습니다.")

    table = pd.read_excel(io.BytesIO(resp.content), header=header_row)
    name_col = _find_column(table, ("name",))
    sector_col = _find_column(table, ("sector", "gics sector"))

    records = []
    for _, row in table.iterrows():
        ticker = str(row.get("Ticker", "")).strip()
        if not TICKER_PATTERN.match(ticker):
            continue
        records.append(
            {
                "ticker": ticker.replace(".", "-"),
                "name": (str(row[name_col]).strip() if name_col is not None and pd.notna(row[name_col]) else None),
                "sector": _clean_sector(row[sector_col]) if sector_col is not None else None,
            }
        )

    if len(records) < 400:
        raise RuntimeError(f"SPY holdings에서 티커가 너무 적게({len(records)}개) 파싱됐습니다.")

    valid_sector = sum(1 for r in records if r["sector"])
    ratio = valid_sector / len(records)
    if sector_col is None or ratio < MIN_VALID_SECTOR_RATIO:
        raise RuntimeError(
            f"SPY holdings의 섹터 컬럼({sector_col})이 신뢰할 수 없습니다 "
            f"(유효 섹터 {valid_sector}/{len(records)} = {ratio:.0%}). "
            "위키피디아로 대체합니다."
        )

    return records


def get_sp500_records_from_wikipedia() -> list[dict]:
    """SPY 파일을 못 받아왔을 때(또는 섹터가 못 미더울 때) 쓰는 대체 소스."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    table = pd.read_html(io.StringIO(resp.text))[0]

    ticker_col = _find_column(table, ("symbol", "ticker")) or "Symbol"
    name_col = _find_column(table, ("security", "company", "name"))
    sector_col = _find_column(table, ("gics sector", "sector"))

    records = []
    for _, row in table.iterrows():
        ticker = str(row[ticker_col]).strip()
        if not TICKER_PATTERN.match(ticker.replace(".", "-")):
            continue
        records.append(
            {
                "ticker": ticker.replace(".", "-"),
                "name": (str(row[name_col]).strip() if name_col is not None and pd.notna(row[name_col]) else None),
                "sector": _clean_sector(row[sector_col]) if sector_col is not None else None,
            }
        )
    return records


def get_sp500_records() -> list[dict]:
    try:
        records = get_sp500_records_from_spy()
        print(f"S&P500 티커를 SPY 홀딩스 파일에서 받아왔습니다 ({len(records)}개).", file=sys.stderr)
        return records
    except Exception as e:
        print(f"[경고] SPY 홀딩스 실패({e}) → 위키피디아로 대체합니다.", file=sys.stderr)
        return get_sp500_records_from_wikipedia()


# 나스닥100 실시간 스크레이핑이 전부 막혔을 때 쓰는 최후의 비상용 명단입니다.
# 위키피디아 "Nasdaq-100" 문서에서 2026-08-26에 받아온 스냅샷이라 시간이 지나면
# 편입/편출 종목이 실제와 달라질 수 있어요. 다른 방법이 다 실패했을 때만 씁니다.
# (종목명/섹터는 이 비상용 경로에서는 못 채우고, 티커만 확보합니다.)
NASDAQ100_FALLBACK_TICKERS = [
    "ADBE", "AMD", "ABNB", "ALNY", "GOOGL", "GOOG", "AMZN", "AEP", "AMGN", "ADI",
    "AAPL", "AMAT", "APP", "ARM", "ASML", "ADSK", "ADP", "AXON", "BKR", "BKNG",
    "AVGO", "CDNS", "CHTR", "CTAS", "CSCO", "CCEP", "CTSH", "CMCSA", "CEG", "CPRT",
    "CSGP", "COST", "CRWD", "CSX", "DDOG", "DXCM", "FANG", "DASH", "EA", "EXC",
    "FAST", "FER", "FTNT", "GEHC", "GILD", "HON", "IDXX", "INSM", "INTC", "INTU",
    "ISRG", "KDP", "KLAC", "KHC", "LRCX", "LIN", "MAR", "MRVL", "MELI", "META",
    "MCHP", "MU", "MSFT", "MSTR", "MDLZ", "MPWR", "MNST", "NFLX", "NVDA", "NXPI",
    "ORLY", "ODFL", "PCAR", "PLTR", "PANW", "PAYX", "PYPL", "PDD", "PEP", "QCOM",
    "REGN", "ROP", "ROST", "SNDK", "STX", "SHOP", "SBUX", "SNPS", "TMUS", "TTWO",
    "TSLA", "TXN", "TRI", "VRSK", "VRTX", "WMT", "WBD", "WDC", "WDAY", "XEL", "ZS",
]


def get_nasdaq100_records_from_wikipedia(url: str) -> list[dict]:
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))

    best_records: list[dict] = []
    debug_info = []
    for idx, t in enumerate(tables):
        ticker_col = _find_column(t, ("ticker", "symbol", "ticker symbol"))
        debug_info.append(f"  표 {idx}: 컬럼={list(t.columns)[:8]} (행 {len(t)}개)")
        if ticker_col is None:
            continue
        name_col = _find_column(t, ("company", "name", "security"))
        sector_col = _find_column(t, ("icb industry", "gics sector", "sector"))

        records = []
        for _, row in t.iterrows():
            ticker = str(row[ticker_col]).replace(".", "-").strip()
            if not TICKER_PATTERN.match(ticker):
                continue
            sector = _clean_sector(row[sector_col]) if sector_col is not None else None
            if sector is not None:
                sector = ICB_TO_GICS_SECTOR.get(sector, sector)
            records.append(
                {
                    "ticker": ticker,
                    "name": (str(row[name_col]).strip() if name_col is not None and pd.notna(row[name_col]) else None),
                    "sector": sector,
                }
            )

        # "90개 이상인 첫 표"를 바로 채택하지 않고, 지금까지 본 것 중 가장 티커를
        # 많이 건진 표를 기억해둡니다 → 위키 표 순서가 바뀌어도 더 잘 버팁니다.
        if len(records) > len(best_records):
            best_records = records

    if len(best_records) >= 90:
        return best_records

    # 여기까지 왔다는 건 실패했다는 뜻 — 다음에 왜 실패했는지 바로 알 수 있게
    # 페이지에서 찾은 표들의 구조를 전부 로그로 남겨둡니다.
    print(f"[디버그] '{url}'에서 찾은 표 {len(tables)}개:", file=sys.stderr)
    for line in debug_info:
        print(line, file=sys.stderr)
    raise RuntimeError(
        f"'{url}' 에서 나스닥100 표를 찾지 못했습니다 (최선 결과 {len(best_records)}개, 90개 필요). "
        "페이지 구조가 바뀐 것 같아요."
    )


def get_nasdaq100_records() -> list[dict]:
    """여러 소스를 순서대로 시도합니다. 다 실패하면 마지막으로 고정된 비상용 명단을 씁니다.
    (이 함수 자체는 예외를 던지지 않도록 설계했어요 — 티커 목록 하나 때문에
     전체 파이프라인이 죽는 걸 막기 위해서입니다.)"""
    attempts = [
        ("위키피디아(일반 문서)", "https://en.wikipedia.org/wiki/Nasdaq-100"),
        ("위키피디아(REST API)", "https://en.wikipedia.org/api/rest_v1/page/html/Nasdaq-100"),
    ]
    for label, url in attempts:
        try:
            records = get_nasdaq100_records_from_wikipedia(url)
            print(f"나스닥100 티커를 {label}에서 받아왔습니다 ({len(records)}개).", file=sys.stderr)
            return records
        except Exception as e:
            print(f"[경고] 나스닥100 티커 수집 실패 - {label}: {e}", file=sys.stderr)

    print(
        f"[경고] 나스닥100 티커를 실시간으로 가져오지 못해 비상용 고정 명단을 사용합니다 "
        f"({len(NASDAQ100_FALLBACK_TICKERS)}개, 2026-08-26 기준 스냅샷 — 최신 편입/편출과 다를 수 있어요).",
        file=sys.stderr,
    )
    return [{"ticker": t, "name": None, "sector": None} for t in NASDAQ100_FALLBACK_TICKERS]


def get_universe() -> tuple[list[str], dict[str, dict]]:
    """S&P500 + 나스닥100 티커를 합치고, 종목명/섹터 메타데이터도 함께 만듭니다.
    두 지수에 겹치는 종목은 S&P500 쪽 정보(GICS 섹터)를 우선으로 씁니다."""
    sp500 = get_sp500_records()
    nasdaq100 = get_nasdaq100_records()

    meta: dict[str, dict] = {}
    for rec in nasdaq100:  # 나스닥100을 먼저 넣고
        meta[rec["ticker"]] = {"name": rec["name"], "sector": _clean_sector(rec["sector"])}
    for rec in sp500:  # S&P500으로 덮어써서(우선순위) GICS 섹터가 이기게 함
        meta[rec["ticker"]] = {"name": rec["name"], "sector": _clean_sector(rec["sector"])}

    universe = sorted(meta.keys())
    print(f"S&P500 {len(sp500)}개 + 나스닥100 {len(nasdaq100)}개 → 중복 제거 후 {len(universe)}개")
    print(f"섹터 정보 확보율: {_valid_sector_ratio(meta):.0%} ({len(universe)}종목 중)")
    return universe, meta


def _get_shares_outstanding(ticker: str) -> float | None:
    """발행주식수를 가져옵니다. yfinance 버전에 따라 fast_info의 키 이름이 달라질 수 있어서
    여러 키/방법을 순서대로 시도합니다. 다 실패하면 None (시가총액 계산에서 그 종목만 제외)."""
    try:
        fi = yf.Ticker(ticker).fast_info
        for key in ("shares", "sharesOutstanding", "share_count", "shares_outstanding"):
            try:
                val = fi[key]
            except Exception:
                val = getattr(fi, key, None)
            if val:
                return float(val)
    except Exception:
        pass

    try:
        info = yf.Ticker(ticker).info
        val = info.get("sharesOutstanding")
        if val:
            return float(val)
    except Exception:
        pass

    return None


def fetch_shares_outstanding(tickers: list[str]) -> dict[str, float | None]:
    """종목별 발행주식수를 병렬로 가져옵니다. 시가총액 계산용인데, 자주 안 바뀌는 값이라
    (자사주 매입/증자 때만 바뀜) 이 무거운 개별 조회를 매일이 아니라 티커 목록 캐시가
    갱신되는 주기(기본 7일)에만 실행해서 매일 실행되는 파이프라인은 느려지지 않게 했어요."""
    result: dict[str, float | None] = {}
    print(f"발행주식수 조회를 시작합니다 (총 {len(tickers)}종목, 시가총액 계산용 — 시간이 좀 걸려요)...", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=SHARES_FETCH_WORKERS) as ex:
        futures = {ex.submit(_get_shares_outstanding, t): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            ticker = futures[fut]
            try:
                result[ticker] = fut.result()
            except Exception as e:
                print(f"  [경고] {ticker} 발행주식수 조회 실패: {e}", file=sys.stderr)
                result[ticker] = None
            done += 1
            if done % 50 == 0 or done == len(tickers):
                print(f"  발행주식수 {done}/{len(tickers)}...", file=sys.stderr)

    ok = sum(1 for v in result.values() if v)
    print(f"발행주식수 확보: {ok}/{len(tickers)}종목.", file=sys.stderr)
    return result


def load_or_update_tickers() -> tuple[list[str], dict[str, dict], dict[str, float | None]]:
    """(티커 목록, {티커: {name, sector}}, {티커: 발행주식수}) 를 돌려줍니다."""
    cached = None
    if TICKER_CACHE_PATH.exists():
        cached = json.loads(TICKER_CACHE_PATH.read_text(encoding="utf-8"))
        has_new_fields = "meta" in cached and "shares" in cached
        sector_ratio = _valid_sector_ratio(cached.get("meta", {})) if has_new_fields else 0.0
        sector_ok = sector_ratio >= MIN_VALID_SECTOR_RATIO
        fetched_at = datetime.fromisoformat(cached["fetched_at"])
        age_days = (datetime.now(timezone.utc) - fetched_at).days
        if has_new_fields and sector_ok and age_days < TICKER_CACHE_MAX_AGE_DAYS:
            print(f"종목 리스트 캐시 사용 ({age_days}일 전 갱신, {len(cached['tickers'])}종목, 섹터 확보율 {sector_ratio:.0%})")
            return cached["tickers"], cached["meta"], cached["shares"]
        if not has_new_fields:
            print("캐시에 종목명/섹터/발행주식수 정보가 없습니다(예전 형식). 한 번 새로 받아옵니다...")
        elif not sector_ok:
            print(f"캐시의 섹터 확보율이 너무 낮습니다({sector_ratio:.0%}). 다시 받아옵니다...")
        else:
            print(f"종목 리스트 캐시가 {age_days}일 지났습니다. 새로 받아옵니다...")

    try:
        tickers, meta = get_universe()
        shares = fetch_shares_outstanding(tickers)
    except Exception as e:
        if cached is not None and "tickers" in cached:
            print(f"[경고] 종목 리스트 갱신 실패({e}) → 기존 캐시({len(cached['tickers'])}종목)로 계속 진행합니다.")
            return cached["tickers"], cached.get("meta", {}), cached.get("shares", {})
        raise

    TICKER_CACHE_PATH.write_text(
        json.dumps(
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "tickers": tickers,
                "meta": meta,
                "shares": shares,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"종목 리스트를 새로 받아서 캐시에 저장했습니다 ({len(tickers)}개).")
    return tickers, meta, shares


def chunk_list(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_batch(tickers: list[str]) -> list[dict]:
    raw = yf.download(
        tickers,
        period="5d",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    rows = []
    for ticker in tickers:
        try:
            df = raw[ticker].dropna(how="all")
        except KeyError:
            rows.append({"ticker": ticker, "status": "failed"})
            continue

        if df.empty:
            rows.append({"ticker": ticker, "status": "no_data"})
            continue

        last = df.iloc[-1]
        last_date = df.index[-1].strftime("%Y-%m-%d")
        is_suspicious = bool(last["Open"] == last["High"] == last["Low"] == last["Close"])

        change_pct = None
        if len(df) >= 2:
            prev_close = df.iloc[-2]["Close"]
            if prev_close:
                change_pct = round((last["Close"] - prev_close) / prev_close * 100, 2)

        rows.append(
            {
                "ticker": ticker,
                "status": "suspicious" if is_suspicious else "ok",
                "date": last_date,
                "close": round(float(last["Close"]), 2),
                "change_pct": change_pct,
                "volume": int(last["Volume"]) if not pd.isna(last["Volume"]) else None,
            }
        )

    return rows


def fetch_all(tickers: list[str]) -> list[dict]:
    batches = chunk_list(tickers, BATCH_SIZE)
    all_rows = []

    for i, batch in enumerate(batches, start=1):
        print(f"[{i}/{len(batches)}] {len(batch)}개 티커 요청 중...", file=sys.stderr)

        for attempt in range(1, MAX_RETRY + 1):
            try:
                all_rows.extend(fetch_batch(batch))
                break
            except Exception as e:
                print(f"  실패 (시도 {attempt}/{MAX_RETRY}): {e}", file=sys.stderr)
                if attempt < MAX_RETRY:
                    time.sleep(RETRY_SLEEP)
                else:
                    all_rows.extend({"ticker": t, "status": "batch_failed"} for t in batch)

        if i < len(batches):
            time.sleep(random.uniform(*BATCH_SLEEP))

    return all_rows


def main():
    tickers, meta, shares = load_or_update_tickers()
    print(f"\n{len(tickers)}종목 종가 수집을 시작합니다...", file=sys.stderr)
    rows = fetch_all(tickers)

    for row in rows:
        info = meta.get(row["ticker"], {})
        row["name"] = info.get("name")
        row["sector"] = _clean_sector(info.get("sector"))

        shares_out = shares.get(row["ticker"])
        close = row.get("close")
        if shares_out and isinstance(close, (int, float)) and row.get("status") in ("ok", "suspicious"):
            row["market_cap_b"] = round(shares_out * close / 1_000_000_000, 2)
        else:
            row["market_cap_b"] = None

    ok_count = sum(1 for r in rows if r.get("status") in ("ok", "suspicious"))
    cap_count = sum(1 for r in rows if r.get("market_cap_b") is not None)
    sector_count = sum(1 for r in rows if r.get("sector"))
    print(
        f"\n총 {len(rows)}종목 중 성공 {ok_count}개 (시가총액 확보 {cap_count}개, 섹터 확보 {sector_count}개)",
        file=sys.stderr,
    )

    output = {
        "last_updated": datetime.now(KST).isoformat(),
        "count": len(rows),
        "rows": rows,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"저장 완료: {OUTPUT_PATH}", file=sys.stderr)


if __name__ == "__main__":
    main()
