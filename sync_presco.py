import os
import time
import csv
import json
import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from playwright._impl._errors import Error as PlaywrightError

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ==========================================
# Presco ログイン + 2日分取得
# ==========================================
def login_and_download():

    email = os.getenv("PRESCO_EMAIL")
    password = os.getenv("PRESCO_PASSWORD")

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process"
            ]
        )

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )

        page = context.new_page()

        try:
            # ----------------------------------
            # ログインページアクセス（リトライ）
            # ----------------------------------
            for i in range(3):
                try:
                    page.goto("https://presco.ai/partner/", timeout=60000)
                    break
                except PlaywrightError:
                    print("ログインページアクセス再試行")
                    time.sleep(5)

            page.wait_for_selector('input[name="username"]', timeout=30000)

            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)

            with page.expect_navigation(timeout=60000):
                page.click('input[type="submit"]')

            print("ログイン成功")

            # ----------------------------------
            # 成果一覧ページへ
            # ----------------------------------
            page.goto("https://presco.ai/partner/actionLog/list", timeout=60000)
            page.wait_for_load_state("networkidle")
            time.sleep(3)

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

            yesterday_path = "/tmp/presco_yesterday.csv"
            today_path = "/tmp/presco_today.csv"

            download_csv_for_period(page, "yesterday", yesterday_path)
            download_csv_for_period(page, "today", today_path)

            return yesterday_path, today_path

        finally:
            browser.close()


# ==========================================
# 期間指定クリック（onclick直指定）
# ==========================================
def download_csv_for_period(page, period, save_path):

    print(f"{period} を選択")

    if period == "yesterday":
        page.click('a[onclick="setYesterday()"]', timeout=10000)
    elif period == "today":
        page.click('a[onclick="setToday()"]', timeout=10000)

    time.sleep(1)

    page.click('button:has-text("検索条件で絞り込む")', timeout=10000)
    time.sleep(5)

    page.wait_for_selector("#csv-link", timeout=30000)

    with page.expect_download(timeout=60000) as download_info:
        page.click("#csv-link")

    download = download_info.value
    download.save_as(save_path)

    print(f"{period} CSV保存完了")


# ==========================================
# CSVマージ
# ==========================================
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


# ==========================================
# Google Sheets 書き込み
# ==========================================
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


# ==========================================
# main
# ==========================================
def main():

    print("昨日＋今日 取得開始")

    yesterday_path, today_path = login_and_download()

    merged_data = merge_csv(yesterday_path, today_path)

    upload_to_sheet(merged_data)

    print("完了")


if __name__ == "__main__":
    main()
