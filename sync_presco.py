import os
import time
import csv
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =====================================
# Presco ログイン＆CSV取得（昨日〜今日）
# =====================================
def login_and_download_csv():

    email = os.getenv("PRESCO_EMAIL")
    password = os.getenv("PRESCO_PASSWORD")

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        
        # ★タイムアウト対策：全体のデフォルト待機時間を60秒に延長
        context.set_default_timeout(60000)

        page = context.new_page()

        try:
            # ログイン
            page.goto("https://presco.ai/partner/", timeout=60000)
            page.wait_for_selector('input[name="username"]')

            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)

            with page.expect_navigation():
                page.click('input[type="submit"]')

            print("ログイン成功")

            # 成果一覧ページ
            # ★タイムアウト対策：timeout=60000を追加し、networkidleを削除してsleepを5秒に
            page.goto("https://presco.ai/partner/actionLog/list", timeout=60000)
            time.sleep(5)

            # 成果発生日時に変更
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

            time.sleep(1)

            # 🔥 昨日と今日の日付を動的に生成
            JST = ZoneInfo("Asia/Tokyo")
            today = datetime.now(JST)
            yesterday = today - timedelta(days=1)
            
            date_from = yesterday.strftime("%Y/%m/%d")
            date_to = today.strftime("%Y/%m/%d")

            # 🔥 カレンダーUIを無視して、直接inputのvalueを書き換える
            page.evaluate(f'document.getElementById("dateTimeFrom").value = "{date_from}"')
            page.evaluate(f'document.getElementById("dateTimeTo").value = "{date_to}"')
            time.sleep(1)

            # 検索ボタン（div）
            page.click('.filter-button--submit', timeout=10000)
            time.sleep(5)

            page.wait_for_selector("#csv-link")

            with page.expect_download() as download_info:
                page.click("#csv-link")

            download = download_info.value
            csv_path = "/tmp/presco_week.csv"
            download.save_as(csv_path)

            print(f"CSV取得完了（対象期間: {date_from} 〜 {date_to}）")

            return csv_path

        finally:
            browser.close()


# =====================================
# カットオフフィルタ
# =====================================
def get_cutoff_datetime():
    JST = ZoneInfo("Asia/Tokyo")
    # 🔥 自動運用のため、常に「昨日の0時0分0秒」をカットオフ日時に設定するよう変更
    today = datetime.now(JST)
    yesterday = today - timedelta(days=1)
    return yesterday.replace(hour=0, minute=0, second=0, microsecond=0)


def is_after_cutoff(date_string, cutoff):
    try:
        JST = ZoneInfo("Asia/Tokyo")
        dt = datetime.strptime(date_string, "%Y/%m/%d %H:%M:%S")
        dt = dt.replace(tzinfo=JST)
        return dt >= cutoff
    except:
        return False


# =====================================
# CSV変換
# =====================================
def extract_gclid(url):
    if not url:
        return ""
    match = re.search(r"gclid=([^&]+)", url)
    return match.group(1) if match else ""


def transform_csv(csv_path):

    target_sites = ["Fast Baito 介護特化", "Fast Baito"]
    cutoff = get_cutoff_datetime()

    print("カットオフ日時:", cutoff.strftime("%Y/%m/%d %H:%M:%S"))

    with open(csv_path, "r", encoding="shift_jis", errors="ignore") as f:
        reader = list(csv.reader(f))

    data = reader[1:]

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

        action_datetime = row[3]

        if not is_after_cutoff(action_datetime, cutoff):
            continue

        gclid = extract_gclid(row[12])
        if not gclid:
            continue

        if gclid in seen:
            continue

        seen.add(gclid)

        # ✅ 介護特化・通常どちらもCSVの値（row[17]）をそのまま使用
        value = str(int(float(row[17])))

        if site == "Fast Baito 介護特化":
            conv_name = "介護オフラインCV"
        else:
            conv_name = "オフラインCV"

        results.append([
            gclid,
            conv_name,
            action_datetime,
            value,
            "JPY"
        ])

    print("抽出件数:", len(results) - 2)

    return results


# =====================================
# Sheets書き込み
# =====================================
def upload_to_sheet(data):

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

    ws.clear()
    ws.update(values=data, range_name="A1")

    print("スプレッドシート更新完了")


# =====================================
# main
# =====================================
def main():

    csv_path = login_and_download_csv()
    transformed = transform_csv(csv_path)
    upload_to_sheet(transformed)

    print("完了")


if __name__ == "__main__":
    main()
