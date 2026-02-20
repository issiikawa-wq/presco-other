import os
import time
import csv
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ===============================
# Presco CSV取得（昨日 or 今日）
# ===============================
def download_csv_for_period(page, period_text, save_path):

    print(f"{period_text} を選択してCSV取得")

    try:
        page.click(f'button:has-text("{period_text}")', timeout=5000)
    except:
        page.click(f'text={period_text}', timeout=5000)

    time.sleep(1)

    try:
        page.click('button:has-text("検索条件で絞り込む")', timeout=5000)
    except:
        pass

    time.sleep(5)

    page.wait_for_selector("#csv-link")

    with page.expect_download() as download_info:
        page.click("#csv-link")

    download = download_info.value
    download.save_as(save_path)

    print(f"{period_text} CSV保存完了")


# ===============================
# ログイン & 2日分取得
# ===============================
def login_and_download():

    email = os.getenv("PRESCO_EMAIL")
    password = os.getenv("PRESCO_PASSWORD")

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = browser.new_context()
        page = context.new_page()

        try:
            # ログイン
            page.goto("https://presco.ai/partner/")
            page.wait_for_selector('input[name="username"]')

            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)

            with page.expect_navigation():
                page.click('input[type="submit"]')

            print("ログイン成功")

            # 成果一覧へ
            page.goto("https://presco.ai/partner/actionLog/list")
            page.wait_for_load_state("networkidle")
            time.sleep(3)

            # 成果発生日時へ変更
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

            # 昨日
            yesterday_path = "/tmp/presco_yesterday.csv"
            download_csv_for_period(page, "昨日", yesterday_path)

            # 今日
            today_path = "/tmp/presco_today.csv"
            download_csv_for_period(page, "今日", today_path)

            return yesterday_path, today_path

        finally:
            browser.close()


# ===============================
# CSVマージ
# ===============================
def extract_gclid(url):
    if not url:
        return ""
    match = re.search(r"gclid=([^&]+)", url)
    return match.group(1) if match else ""


def merge_csv(yesterday_path, today_path):

    target_sites = ["Fast Baito 介護特化", "Fast Baito"]

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

    for path in [yesterday_path, today_path]:

        with open(path, "r", encoding="shift_jis", errors="ignore") as f:
            reader = list(csv.reader(f))

        data = reader[1:]

        for row in data:

            if len(row) < 18:
                continue

            site = row[5]
            if site not in target_sites:
                continue

            gclid = extract_gclid(row[12])
            if not gclid:
                continue

            if gclid in seen:
                continue

            seen.add(gclid)

            action_datetime = row[3]

            if site == "Fast Baito 介護特化":
                value = "3000"
                conv_name = "介護オフラインCV"
            else:
                value = str(int(float(row[17])))
                conv_name = "オフラインCV"

            results.append([
                gclid,
                conv_name,
                action_datetime,
                value,
                "JPY"
            ])

    print("最終抽出件数:", len(results) - 2)

    return results


# ===============================
# Sheets書き込み
# ===============================
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


# ===============================
# main
# ===============================
def main():

    print("昨日＋今日 2日分取得開始")

    yesterday_path, today_path = login_and_download()

    merged_data = merge_csv(yesterday_path, today_path)

    upload_to_sheet(merged_data)

    print("完了")


if __name__ == "__main__":
    main()
