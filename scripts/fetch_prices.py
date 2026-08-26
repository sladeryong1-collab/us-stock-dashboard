"""
나스닥100 + S&P500 종가 자동 수집 스크립트 (GitHub Actions용)
================================================================
이 스크립트는 GitHub Actions에서 매일 실행되는 걸 전제로 만들었어요.
(인터넷 제한이 없는 환경에서 도는 걸 가정합니다.)

동작 순서:
  1) tickers_cache.json이 있고 최신(TICKER_CACHE_MAX_AGE_DAYS 이내)이면 그대로 사용,
     아니면 S&P500은 SPY 홀딩스 파일(1순위)/위키피디아(대체), 나스닥100은 위키피디아에서
     리스트를 새로 받아서 캐시 갱신
  2) 종목을 배치(BATCH_SIZE)로 쪼개서 종가 + 전일대비 등락률을 수집
  3) 결과를 data/prices.json 으로 저장 (index.html이 이 파일을 읽어서 표시)

실패한 배치는 재시도하고, 그래도 안 되면 해당 종목만 "실패" 표시하고 넘어갑니다.
"""

import io
import json
import random
import re
import sys
import time
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

ROOT = Path(__file__).resolve().parent.parent
TICKER_CACHE_PATH = ROOT / "tickers_cache.json"
TICKER_CACHE_MAX_AGE_DAYS = 7
OUTPUT_PATH = ROOT / "data" / "prices.json"

KST = timezone(timedelta(hours=9))

# 위키피디아 등은 브라우저처럼 보이는 User-Agent가 없으면 403으로 막는 경우가 많아서
# requests로 직접 받을 때는 항상 이 헤더를 같이 보냅니다.
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
TICKER_PATTERN = re.compile(r"^[A-Z]{1,6}([.\-][A-Z]{1,2})?$")


def get_sp500_tickers_from_spy() -> list[str]:
    """SPY(State Street) 공식 보유종목 파일에서 S&P500 티커를 가져옵니다.
    위키피디아보다 신뢰도가 높은 1차 소스라 이걸 먼저 시도합니다."""
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
    tickers = table["Ticker"].dropna().astype(str).str.strip()
    tickers = tickers[tickers.str.match(TICKER_PATTERN)]
    tickers = tickers.str.replace(".", "-", regex=False).tolist()

    if len(tickers) < 400:  # 정상이면 500개 안팎 — 너무 적으면 파싱이 잘못된 것
        raise RuntimeError(f"SPY holdings에서 티커가 너무 적게({len(tickers)}개) 파싱됐습니다.")
    return tickers


def get_sp500_tickers_from_wikipedia() -> list[str]:
    """SPY 파일을 못 받아왔을 때 쓰는 대체 소스."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    table = pd.read_html(io.StringIO(resp.text))[0]
    tickers = table["Symbol"].tolist()
    return [t.replace(".", "-") for t in tickers]


def get_sp500_tickers() -> list[str]:
    try:
        tickers = get_sp500_tickers_from_spy()
        print(f"S&P500 티커를 SPY 홀딩스 파일에서 받아왔습니다 ({len(tickers)}개).", file=sys.stderr)
        return tickers
    except Exception as e:
        print(f"[경고] SPY 홀딩스 실패({e}) → 위키피디아로 대체합니다.", file=sys.stderr)
        return get_sp500_tickers_from_wikipedia()


def get_nasdaq100_tickers() -> list[str]:
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    for t in tables:
        cols = [str(c) for c in t.columns]
        if "Ticker" in cols:
            return [str(x).replace(".", "-") for x in t["Ticker"].tolist()]
    raise RuntimeError("나스닥100 표를 찾지 못했습니다. 위키 페이지 구조가 바뀐 것 같아요.")


def get_universe() -> list[str]:
    sp500 = get_sp500_tickers()
    nasdaq100 = get_nasdaq100_tickers()
    universe = sorted(set(sp500) | set(nasdaq100))
    print(f"S&P500 {len(sp500)}개 + 나스닥100 {len(nasdaq100)}개 → 중복 제거 후 {len(universe)}개")
    return universe


def load_or_update_tickers() -> list[str]:
    cached = None
    if TICKER_CACHE_PATH.exists():
        cached = json.loads(TICKER_CACHE_PATH.read_text(encoding="utf-8"))
        fetched_at = datetime.fromisoformat(cached["fetched_at"])
        age_days = (datetime.now(timezone.utc) - fetched_at).days
        if age_days < TICKER_CACHE_MAX_AGE_DAYS:
            print(f"종목 리스트 캐시 사용 ({age_days}일 전 갱신, {len(cached['tickers'])}종목)")
            return cached["tickers"]
        print(f"종목 리스트 캐시가 {age_days}일 지났습니다. 새로 받아옵니다...")

    try:
        tickers = get_universe()
    except Exception as e:
        if cached is not None:
            print(f"[경고] 종목 리스트 갱신 실패({e}) → 기존 캐시({len(cached['tickers'])}종목)로 계속 진행합니다.")
            return cached["tickers"]
        raise

    TICKER_CACHE_PATH.write_text(
        json.dumps({"fetched_at": datetime.now(timezone.utc).isoformat(), "tickers": tickers}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"종목 리스트를 새로 받아서 캐시에 저장했습니다 ({len(tickers)}개).")
    return tickers


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
    tickers = load_or_update_tickers()
    print(f"\n{len(tickers)}종목 종가 수집을 시작합니다...", file=sys.stderr)
    rows = fetch_all(tickers)

    ok_count = sum(1 for r in rows if r.get("status") in ("ok", "suspicious"))
    print(f"\n총 {len(rows)}종목 중 성공 {ok_count}개", file=sys.stderr)

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
