import os
import time
import csv
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import Error as PlaywrightError
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ===============================
# Presco ログイン＆CSV取得
# ===============================
def login_and_download_csv(max_retries=3):

    print("=" * 60)
    print(f"[{datetime.now()}] Presco自動同期を開始します（成果発生日時基準）")
    print("=" * 60)

    email = os.getenv("PRESCO_EMAIL")
    password = os.getenv("PRESCO_PASSWORD")

    if not email or not password:
        raise Exception("PRESCO_EMAIL または PRESCO_PASSWORD が未設定")

    for attempt in range(max_retries):
        try:
            return _attempt_login_and_download(email, password)
        except (PlaywrightError, PlaywrightTimeoutError) as e:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                print(f"リトライします {wait}秒待機...")
                time.sleep(wait)
            else:
                raise


def _attempt_login_and_download(email, password):

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://presco.ai/partner/")
            page.wait_for_selector('input[name="username"]')

            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)

            with page.expect_navigation():
                page.click('input[type="submit"]')

            print("ログイン成功")

            page.goto("https://presco.ai/partner/actionLog/list")
            time.sleep(5)

            # ===============================
            # 成果発生日時に変更
            # ===============================
            print("集計基準を成果発生日時に変更")

            try:
                selectors = [
                    'input[name="dateType"][value="actionDate"]',
                    'input[type="radio"][value="actionDate"]',
                    'label:has-text("成果発生日時")'
                ]
                for selector in selectors:
                    try:
                        page.click(selector, timeout=5000)
                        break
                    except:
                        continue
            except Exception as e:
                print("集計基準変更失敗:", e)

            time.sleep(1)

            # 1週間指定
            try:
                page.click('button:has-text("1週間")', timeout=5000)
            except:
                pass

            time.sleep(1)

            # 検索
            try:
                page.click('button:has-text("検索条件で絞り込む")', timeout=5000)
            except:
                pass

            time.sleep(5)

            page.wait_for_selector("#csv-link")

            with page.expect_download() as download_info:
                page.click("#csv-link")

            download = download_info.value
            csv_path = "/tmp/presco.csv"
            download.save_as(csv_path)

            print("CSV取得完了")

            return csv_path

        finally:
            browser.close()


# ===============================
# 日付関連（JST固定）
# ===============================
def get_cutoff_datetime():

    JST = ZoneInfo("Asia/Tokyo")
    now = datetime.now(JST)

    INITIAL = datetime(2026, 2, 20, 0, 0, 0, tzinfo=JST)

    yesterday = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    return max(INITIAL, yesterday)


def is_after_cutoff(date_string, cutoff):

    try:
        JST = ZoneInfo("Asia/Tokyo")
        dt = datetime.strptime(date_string, "%Y/%m/%d %H:%M:%S")
        dt = dt.replace(tzinfo=JST)
        return dt >= cutoff
    except:
        return False


# ===============================
# CSV変換
# ===============================
def extract_gclid(url):
    if not url:
        return ""
    match = re.search(r"gclid=([^&]+)", url)
    return match.group(1) if match else ""


def transform_csv(csv_path):

    target_sites = ["Fast Baito 介護特化", "Fast Baito"]

    cutoff = get_cutoff_datetime()
    print("カットオフ:", cutoff)

    with open(csv_path, "r", encoding="shift_jis", errors="ignore") as f:
        reader = list(csv.reader(f))

    data = reader[1:]  # ヘッダー除外

    results = []
    results.append(["Parameters:TimeZone=Asia/Tokyo"])
    results.append([
        "Google Click ID",
        "Conversion Name",
        "Conversion Time",
        "Conversion Value",
        "Conversion Currency"
    ])

    seen = set()

    for row in data:

        if len(row) < 18:
            continue

        site = row[5]
        if site not in target_sites:
            continue

        # 成果発生日時（D列）
        action_datetime = row[3]

        if not is_after_cutoff(action_datetime, cutoff):
            continue

        gclid = extract_gclid(row[12])
        if not gclid:
            continue

        if gclid in seen:
            continue

        seen.add(gclid)

        value = "3000" if site == "Fast Baito 介護特化" else str(int(float(row[17])))

        results.append([
            gclid,
            "介護オフラインCV" if site == "Fast Baito 介護特化" else "オフラインCV",
            action_datetime,
            value,
            "JPY"
        ])

    print("抽出件数:", len(results) - 2)

    return results


# ===============================
# Google Sheets書き込み
# ===============================
def upload_to_sheet(csv_path):

    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    sheet_id = os.getenv("SPREADSHEET_ID")

    creds = json.loads(creds_json)

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("成果情報_その他")

    data = transform_csv(csv_path)

    ws.clear()
    ws.update(values=data, range_name="A1")

    print("スプレッドシート更新完了")


# ===============================
# main
# ===============================
def main():

    csv_path = login_and_download_csv()
    upload_to_sheet(csv_path)

    print("完了")


if __name__ == "__main__":
    main()
