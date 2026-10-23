"""S2기법 스캐너 - GitHub Actions용

규칙
1. 포착일: 양봉(종가>시가)이면서
     - 거래대금 5000억 이상 & 시총 1조 이상, 또는
     - 거래대금 3000억 이상 & 시총 5조 이상
   인 날. 2026-08-01 이후의 포착일만 쓰고, 종목마다 가장 최근 포착일을 기준으로 본다.
2. 엔벨로프 = 20일 이동평균 +-20%
3. 포착일 다음 날부터 저가가 엔벨로프 하단 이하로 처음 내려가면 '터치'(첫자리 도달)
4. 하단까지 % = 엔벨로프 하단 / 종가 - 1 , -10% 이내면 '근접'
5. 새로 근접(또는 처음 터치)한 종목은 텔레그램으로 알린다. 처음 한 번은 조용히 기준만 저장한다.

참고
- 거래대금은 일봉 자료에 없어서 (고가+저가+종가)/3 x 거래량으로 계산한 추정값이라 HTS와 몇 % 다를 수 있다.
- 시총은 오늘 기준 값이다 (과거 시점 시총은 구할 수 없음).
- 실행할 때마다 처음(S2_SINCE)부터 다시 계산하므로 8월부터의 기록이 자동으로 채워진다.

실행하면 docs/data/ 안의 list.json, charts/코드.json, state.json 을 갱신한다.
환경변수: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SITE_URL, FORCE, DRY_RUN, S2_SINCE
"""
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import FinanceDataReader as fdr

ROOT = os.environ.get('SCAN_ROOT') or os.path.dirname(os.path.abspath(__file__))
KST = ZoneInfo('Asia/Seoul')
DATA_DIR = os.path.join(ROOT, 'docs', 'data')
LIST_JSON = os.path.join(DATA_DIR, 'list.json')
STATE_JSON = os.path.join(DATA_DIR, 'state.json')
UNIVERSE_CSV = os.path.join(ROOT, 'data', 's2_universe.csv')

ENV_PERIOD = 20
ENV_PCT = 20.0
NEAR_PCT = 10.0
RULES = [  # (이름, 최소 거래대금, 최소 시총)
    ('A', 500_000_000_000, 1_000_000_000_000),
    ('B', 300_000_000_000, 5_000_000_000_000),
]
MIN_MARCAP = min(r[2] for r in RULES)
SINCE = os.environ.get('S2_SINCE') or '2026-08-01'
CHART_BARS = 150
WARMUP_DAYS = 60            # SINCE 이전 이동평균 계산용 여유
MAX_WORKERS = 8
MAX_ERROR_RATIO = 0.2


def load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, obj, indent=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent, allow_nan=False)


def reference_date():
    start = (datetime.now(KST) - timedelta(days=14)).strftime('%Y-%m-%d')
    return fdr.DataReader('005930', start).index[-1].date()


def send_telegram(text):
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        print('텔레그램 설정(TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)이 없어 알림은 건너뜁니다')
        return False
    try:
        r = requests.post(f'https://api.telegram.org/bot{token}/sendMessage',
                          json={'chat_id': chat_id, 'text': text, 'disable_web_page_preview': True},
                          timeout=20)
        if r.status_code != 200:
            print(f'텔레그램 전송 실패: {r.status_code} {r.text[:200]}')
            return False
        return True
    except requests.RequestException as e:
        print(f'텔레그램 전송 실패: {e!r}')
        return False


def get_universe():
    try:
        lst = fdr.StockListing('KRX')
        code_col = 'Code' if 'Code' in lst.columns else 'Symbol'
        lst = lst.rename(columns={code_col: 'Code'})
        lst = lst[lst['Market'].astype(str).str.startswith('KOS')]
        lst = lst[~lst['Name'].str.contains(r'스팩|제\d+호|ETN|ETF', regex=True, na=False)]
        lst = lst[~lst['Name'].str.contains(r'우$|우B$|우\(전환\)$', regex=True, na=False)]
        if 'Dept' in lst.columns:
            lst = lst[~lst['Dept'].astype(str).str.contains('관리|SPAC', na=False)]
        print(f"KRX 목록 {len(lst)}종목, 시총 값 있는 종목 {int(lst['Marcap'].notna().sum())}개, "
              f"시총 최대 {lst['Marcap'].max()}")
        lst = lst[lst['Marcap'] >= MIN_MARCAP].sort_values('Marcap', ascending=False)
        if len(lst) == 0:
            raise RuntimeError('시총 1조 이상 종목이 0개입니다 (장중에는 시총 값이 비어 있을 수 있음)')
        uni = lst[['Code', 'Name', 'Market', 'Marcap']].reset_index(drop=True)
        uni['Code'] = uni['Code'].astype(str).str.zfill(6)
        os.makedirs(os.path.dirname(UNIVERSE_CSV), exist_ok=True)
        uni.to_csv(UNIVERSE_CSV, index=False)
        return uni
    except Exception as e:                                  # noqa: BLE001
        print(f'KRX 종목 목록 조회 실패: {e!r}')
        if os.path.exists(UNIVERSE_CSV):
            cached = pd.read_csv(UNIVERSE_CSV, dtype={'Code': str})
            if len(cached):
                print('-> 저장해 둔 data/s2_universe.csv 를 대신 사용합니다')
                return cached
        raise


def prepare(raw):
    df = raw.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()
    df = df[(df['Open'] > 0) & (df['Volume'] > 0)]           # 거래정지일 제외
    df['ma'] = df['Close'].rolling(ENV_PERIOD).mean()
    df['up'] = df['ma'] * (1 + ENV_PCT / 100)
    df['lo'] = df['ma'] * (1 - ENV_PCT / 100)
    df['amount'] = (df['High'] + df['Low'] + df['Close']) / 3 * df['Volume']
    return df


def trigger_rule(row, marcap):
    """포착 조건에 맞는 규칙 이름 (없으면 None). 양봉이 아니면 None"""
    if not row['Close'] > row['Open']:
        return None
    for name, min_amt, min_cap in RULES:
        if row['amount'] >= min_amt and marcap >= min_cap:
            return name
    return None


def analyze(df, marcap, since):
    """(결과 dict, 포착일 index) / 포착이 없으면 None"""
    since_ts = pd.Timestamp(since)
    trig = []
    for i in range(len(df)):
        if df.index[i] < since_ts:
            continue
        rule = trigger_rule(df.iloc[i], marcap)
        if rule:
            trig.append((i, rule))
    if not trig:
        return None
    i, rule = trig[-1]
    lo = df['lo']
    touch_i = None
    for j in range(i + 1, len(df)):
        if not pd.isna(lo.iloc[j]) and float(df['Low'].iloc[j]) <= float(lo.iloc[j]):
            touch_i = j
            break
    last = df.iloc[-1]
    if pd.isna(last['lo']) or len(df) < 2:
        return None
    close = float(last['Close'])
    prev = float(df['Close'].iloc[-2])
    lower = float(last['lo'])
    gap = (lower / close - 1) * 100
    res = {
        'trigger_date': df.index[i].date().isoformat(),
        'trigger_rule': rule,
        'trigger_amount': round(float(df['amount'].iloc[i]) / 1e8),          # 억
        'triggers': [df.index[k].date().isoformat() for k, _ in trig],
        'touched': touch_i is not None,
        'touch_date': df.index[touch_i].date().isoformat() if touch_i is not None else None,
        'close': close,
        'change_pct': round((close / prev - 1) * 100, 2) if prev else 0.0,
        'lower': round(lower, 2),
        'upper': round(float(last['up']), 2),
        'gap_pct': round(gap, 2),
        'near': bool(gap >= -NEAR_PCT),
        'touch_today': bool(float(last['Low']) <= lower),
    }
    return res


def make_chart(df, res):
    tail = df.iloc[-CHART_BARS:]

    def col(name, nd=2):
        return [None if pd.isna(x) else round(float(x), nd) for x in tail[name]]

    return {'t': [d.date().isoformat() for d in tail.index],
            'o': col('Open'), 'h': col('High'), 'l': col('Low'), 'c': col('Close'),
            'up': col('up'), 'lo': col('lo'),
            'triggers': res['triggers'], 'trigger_date': res['trigger_date'], 'touch_date': res['touch_date']}


def scan_one(code, marcap, start, asof):
    last_err = None
    for _ in range(3):
        try:
            raw = fdr.DataReader(code, start)
            if raw is None or raw.empty:
                return 'none', None
            if raw.index[-1].date() != asof:                # 오늘 봉이 없는 종목(거래정지 등)
                return 'none', None
            df = prepare(raw)
            res = analyze(df, marcap, SINCE)
            if res is None:
                return 'none', None
            return 'ok', (res, make_chart(df, res))
        except Exception as e:                              # noqa: BLE001
            last_err = repr(e)
            time.sleep(0.7)
    return 'error', last_err


def key_of(it):
    return f"{it['code']}@{it['trigger_date']}"


def alert_items(items, asof):
    """알림 후보: 아직 끝나지 않았거나(미터치) 오늘 처음 터치한 종목 중 근접한 것"""
    out = []
    for it in items:
        if not it['near']:
            continue
        if it['touched'] and it['touch_date'] != asof:
            continue
        out.append(it)
    return out


def build_messages(items, asof):
    site = os.environ.get('SITE_URL', '')
    msgs = []
    for k in range(0, len(items), 20):
        part = items[k:k + 20]
        lines = [f'S2 하단 근접 {len(items)}종목 ({asof})', '']
        for it in part:
            tag = ' · 하단 터치' if it['touched'] else ''
            lines.append(f"{it['name']} ({it['code']}) 종가 {it['close']:,.0f}원 ({it['change_pct']:+.1f}%)")
            lines.append(f"  하단까지 {it['gap_pct']:+.1f}%{tag} / 포착 {it['trigger_date']} ({it['trigger_amount']:,}억)")
        if site and k + 20 >= len(items):
            lines += ['', site.rstrip('/') + '/']
        msgs.append('\n'.join(lines))
    return msgs


def main():
    now = datetime.now(KST)
    force = os.environ.get('FORCE') == '1'
    dry = os.environ.get('DRY_RUN') == '1'
    asof = reference_date()
    if not force:
        if asof != now.date():
            print(f'오늘({now.date()})은 휴장이거나 아직 데이터가 없어요 (최근 거래일 {asof}). 종료합니다.')
            return 0
        if now.hour < 16:
            print('장 마감 후 데이터가 확정되기 전(16시 이전)이라 종료합니다.')
            return 0

    uni = get_universe()
    print(f'기준일 {asof} / 시총 {MIN_MARCAP / 1e12:.0f}조 이상 {len(uni)}종목 / 포착 시작일 {SINCE}')
    start = (pd.Timestamp(SINCE) - timedelta(days=WARMUP_DAYS + 40)).strftime('%Y-%m-%d')
    items, charts, errors, first_error = [], {}, 0, None
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_one, r.Code, float(r.Marcap), start, asof): (k, r)
                for k, r in enumerate(uni.itertuples(), start=1)}
        for f in as_completed(futs):
            rank, r = futs[f]
            status, payload = f.result()
            if status == 'ok':
                res, chart = payload
                items.append({'rank': rank, 'code': r.Code, 'name': r.Name, 'market': r.Market,
                              'marcap': int(r.Marcap), **res})
                charts[r.Code] = {**chart, 'code': r.Code, 'name': r.Name}
            elif status == 'error':
                errors += 1
                first_error = first_error or payload
    items.sort(key=lambda x: x['rank'])
    watching = [i for i in items if not i['touched']]
    near = [i for i in items if i['near'] and not i['touched']]
    print(f"포착 {len(items)}종목 (감시중 {len(watching)}, 터치 완료 {len(items) - len(watching)}), "
          f"하단 {NEAR_PCT:.0f}% 이내 {len(near)}종목, 조회 실패 {errors}종목")
    if len(uni) and errors / len(uni) > MAX_ERROR_RATIO:
        print(f'조회 실패가 너무 많아 결과를 저장하지 않습니다. 첫 에러: {first_error}')
        return 1

    for code, ch in charts.items():
        save_json(os.path.join(DATA_DIR, 'charts', f'{code}.json'), ch)
    save_json(LIST_JSON, {
        'updated_at': now.isoformat(timespec='seconds'), 'asof': asof.isoformat(), 'since': SINCE,
        'params': {'env_period': ENV_PERIOD, 'env_pct': ENV_PCT, 'near_pct': NEAR_PCT,
                   'rules': [{'name': n, 'min_amount': a, 'min_marcap': c} for n, a, c in RULES]},
        'items': items,
    }, indent=1)
    if dry:
        print('DRY_RUN: 상태 저장과 알림은 건너뜁니다')
        return 0

    cands = alert_items(items, asof.isoformat())
    state = load_json(STATE_JSON, None)
    exit_code = 0
    have_tg = bool(os.environ.get('TELEGRAM_BOT_TOKEN') and os.environ.get('TELEGRAM_CHAT_ID'))
    if state is None:
        print(f'처음 실행이라 알림 없이 현재 근접 {len(cands)}종목을 기준으로만 저장합니다')
        save_json(STATE_JSON, {'asof': asof.isoformat(), 'near': sorted(key_of(i) for i in cands)})
        return 0
    prev = set(state.get('near', []))
    fresh = [i for i in cands if key_of(i) not in prev]
    notified_ok = True
    if fresh:
        fresh.sort(key=lambda x: x['gap_pct'], reverse=True)
        notified_ok = all([send_telegram(m) for m in build_messages(fresh, asof.isoformat())])
        if notified_ok:
            print(f'텔레그램 알림 {len(fresh)}종목 전송')
        elif have_tg:
            exit_code = 1
    if notified_ok or not have_tg:
        save_json(STATE_JSON, {'asof': asof.isoformat(), 'near': sorted(key_of(i) for i in cands)})
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
