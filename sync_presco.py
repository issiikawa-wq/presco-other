import os
import time
import csv
import json
import re
from datetime import datetime
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def login_and_download_csv():
    """
    Presco.aiにログインしてCSVをダウンロード
    集計基準：成果判定日時、期間：1週間で検索
    """
    print("=" * 60)
    print(f"[{datetime.now()}] Presco自動同期を開始します（介護特化・Fast Baito）")
    print("=" * 60)
    
    # 環境変数から認証情報を取得
    email = os.getenv('PRESCO_EMAIL')
    password = os.getenv('PRESCO_PASSWORD')
    
    if not email or not password:
        raise Exception('環境変数 PRESCO_EMAIL または PRESCO_PASSWORD が設定されていません')
    
    print(f"[{datetime.now()}] 処理を開始します")
    print(f"[{datetime.now()}] 認証情報を確認しました")
    
    with sync_playwright() as p:
        # ブラウザを起動（GitHub Actions用の設定）
        print(f"[{datetime.now()}] ブラウザを起動します")
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        
        # コンテキストを作成
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        context.set_default_timeout(60000)  # 60秒
        
        page = context.new_page()
        
        try:
            # ログインページにアクセス
            print(f"[{datetime.now()}] ログインページにアクセスします")
            page.goto('https://presco.ai/partner/', timeout=60000)
            
            # ログインフォームが表示されるまで待機
            page.wait_for_selector('input[name="username"]', timeout=10000)
            print(f"[{datetime.now()}] ログインフォームを確認しました")
            
            # ログイン情報を入力
            print(f"[{datetime.now()}] ログイン情報を入力します")
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            
            # ログインボタンをクリック
            print(f"[{datetime.now()}] ログインボタンをクリックします")
            with page.expect_navigation(timeout=60000):
                page.click('input[type="submit"][value="ログイン"]')
            
            # ログイン後のページ遷移を待機
            time.sleep(5)
            
            # ログイン成功を確認
            current_url = page.url
            print(f"[{datetime.now()}] 現在のURL: {current_url}")
            
            if 'login' in current_url or 'logout' in current_url:
                page.screenshot(path='/tmp/login_error.png')
                raise Exception('ログインに失敗しました')
            
            print(f"[{datetime.now()}] ログインに成功しました")
            
            # 成果一覧ページに移動
            print(f"[{datetime.now()}] 成果一覧ページに移動します")
            page.goto('https://presco.ai/partner/actionLog/list', timeout=60000)
            
            # ページが完全に読み込まれるまで待機
            time.sleep(3)
            
            # 集計基準を「成果判定日時」に変更
            print(f"[{datetime.now()}] 集計基準を「成果判定日時」に変更します")
            try:
                # ラジオボタンのセレクタを試行
                # 通常のラジオボタンの場合
                page.click('input[name="dateType"][value="judgeDate"]', timeout=5000)
            except:
                try:
                    # ラベルをクリックする方法
                    page.click('label:has-text("成果判定日時")', timeout=5000)
                except:
                    print(f"[{datetime.now()}] 警告: 集計基準の変更に失敗（デフォルトのまま続行）")
            
            time.sleep(1)
            
            # 期間を「1週間」に変更
            print(f"[{datetime.now()}] 期間を「1週間」に変更します")
            try:
                # ボタンやリンクの場合
                page.click('button:has-text("1週間"), a:has-text("1週間")', timeout=5000)
            except:
                try:
                    # クラス名やIDで指定する方法
                    page.click('.period-button:has-text("1週間")', timeout=5000)
                except:
                    print(f"[{datetime.now()}] 警告: 期間の変更に失敗（デフォルトのまま続行）")
            
            time.sleep(1)
            
            # 「検索条件で絞り込む」ボタンをクリック
            print(f"[{datetime.now()}] 検索条件で絞り込むをクリックします")
            try:
                # 複数のセレクタを試行
                selectors = [
                    'button:has-text("検索条件で絞り込む")',
                    'input[type="submit"][value="検索条件で絞り込む"]',
                    'button.filter-button--submit',
                    '.filter-button--submit'
                ]
                
                clicked = False
                for selector in selectors:
                    try:
                        page.click(selector, timeout=3000)
                        clicked = True
                        break
                    except:
                        continue
                
                if not clicked:
                    print(f"[{datetime.now()}] 警告: 検索ボタンのクリックに失敗")
                else:
                    # 検索結果の読み込みを待機
                    time.sleep(5)
                    print(f"[{datetime.now()}] 検索条件を適用しました")
                    
            except Exception as e:
                print(f"[{datetime.now()}] 警告: 検索ボタンのクリック中にエラー - {str(e)}")
            
            # CSVダウンロードボタンが表示されるまで待機
            page.wait_for_selector('#csv-link', state='visible', timeout=30000)
            print(f"[{datetime.now()}] CSVダウンロードボタンを確認しました")
            
            # 少し待機
            time.sleep(2)
            
            # CSVダウンロード
            print(f"[{datetime.now()}] CSVダウンロードを開始します")
            with page.expect_download(timeout=60000) as download_info:
                page.click('#csv-link')
                print(f"[{datetime.now()}] CSVダウンロードボタンをクリックしました")
            
            download = download_info.value
            
            # ファイルを保存
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = f'/tmp/presco_data_{timestamp}.csv'
            download.save_as(csv_path)
            
            print(f"[{datetime.now()}] CSVをダウンロードしました: {csv_path}")
            
            # ファイルサイズを確認
            file_size = os.path.getsize(csv_path)
            print(f"[{datetime.now()}] ファイルサイズ: {file_size} bytes")
            
            if file_size == 0:
                raise Exception('ダウンロードしたCSVファイルが空です')
            
            return csv_path
            
        except Exception as e:
            # エラー時はスクリーンショットを保存
            try:
                page.screenshot(path='/tmp/error_screenshot.png')
                print(f"[{datetime.now()}] エラー時のスクリーンショットを保存しました")
            except:
                pass
            raise e
            
        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")


def extract_gclid(referrer_url):
    """
    リファラURLからgclidを抽出
    """
    if not referrer_url:
        return ''
    
    match = re.search(r'gclid=([^&]+)', referrer_url)
    if match:
        return match.group(1)
    return ''


def format_datetime_for_google(date_string):
    """
    日時文字列をGoogle広告用フォーマットに変換
    YYYY/MM/DD HH:MM:SS 形式のまま返す（タイムゾーン指定なし）
    """
    try:
        # 入力: YYYY/MM/DD HH:MM:SS
        # 出力: YYYY/MM/DD HH:MM:SS（そのまま）
        dt = datetime.strptime(date_string, '%Y/%m/%d %H:%M:%S')
        return dt.strftime('%Y/%m/%d %H:%M:%S')
    except Exception as e:
        print(f"[{datetime.now()}] 日付変換エラー: {date_string} - {str(e)}")
        # フォールバック: +09:00などを削除して返す
        return date_string.split('+')[0].strip()


def get_conversion_name(site_name):
    """
    サイト名からコンバージョン名を取得
    """
    conversion_map = {
        'Fast Baito 介護特化': '介護オフラインCV',
        'Fast Baito': 'オフラインCV'
    }
    return conversion_map.get(site_name, site_name)


def transform_csv_data(csv_path, existing_gclids):
    """
    CSVデータをGoogle広告用フォーマットに変換
    複数サイトに対応し、重複チェックを実施
    """
    # 対象サイト名
    target_sites = ['Fast Baito 介護特化', 'Fast Baito']
    
    # カウンター
    total_rows = 0
    site_mismatch_count = 0
    no_gclid_count = 0
    duplicate_count = 0
    new_rows_count = 0
    
    # サイト別カウンター
    site_counts = {site: 0 for site in target_sites}
    
    # 変換後のデータ
    new_data = []
    
    # パラメータ行（1行目）
    parameter_row = ['Parameters:TimeZone=Asia/Tokyo']
    
    # ヘッダー行（2行目）
    header = [
        'Google Click ID',
        'Conversion Name',
        'Conversion Time',
        'Conversion Value',
        'Conversion Currency'
    ]
    
    # CSVを読み込み（複数のエンコーディングを試行）
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    data = None
    
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                data = list(reader)
            print(f"[{datetime.now()}] CSVを {encoding} で読み込みました")
            break
        except UnicodeDecodeError:
            continue
    
    if data is None:
        raise Exception('CSVファイルの読み込みに失敗しました')
    
    # ヘッダーをスキップ
    if len(data) > 0:
        data = data[1:]
    
    total_rows = len(data)
    print(f"[{datetime.now()}] 総データ数: {total_rows}行")
    
    # 各行を処理
    for i, row in enumerate(data, start=2):  # 行番号は2から（ヘッダー除く）
        if len(row) < 18:  # R列（index 17）まで必要
            continue
        
        # F列: サイト名
        site_name = row[5] if len(row) > 5 else ''
        
        # 対象サイトかチェック
        if site_name not in target_sites:
            site_mismatch_count += 1
            continue
        
        # D列: 成果発生日時
        action_datetime = row[3] if len(row) > 3 else ''
        
        # M列: リファラ
        referrer = row[12] if len(row) > 12 else ''
        
        # gclidを抽出
        gclid = extract_gclid(referrer)
        
        if not gclid:
            no_gclid_count += 1
            continue
        
        # 重複チェック
        if gclid in existing_gclids:
            duplicate_count += 1
            continue
        
        # コンバージョン名を取得
        conversion_name = get_conversion_name(site_name)
        
        # 日時を変換
        conversion_time = format_datetime_for_google(action_datetime)
        
        # 成果報酬単価を取得（R列）
        try:
            reward_price = int(float(row[17]))  # R列 = index 17
        except (ValueError, IndexError):
            reward_price = 0
            print(f"[{datetime.now()}] 警告: 成果報酬単価を取得できません（行{i}） - デフォルト0を使用")
        
        # Conversion Value を決定（介護オフラインCVは固定3000、それ以外は成果報酬単価）
        if conversion_name == '介護オフラインCV':
            conversion_value = '3000'
        else:
            conversion_value = str(reward_price)
        
        # データ行を構築
        new_row = [
            gclid,                      # A列: Google Click ID
            conversion_name,            # B列: Conversion Name
            conversion_time,            # C列: Conversion Time
            conversion_value,           # D列: Conversion Value
            'JPY'                       # E列: Conversion Currency
        ]
        
        new_data.append(new_row)
        new_rows_count += 1
        site_counts[site_name] += 1
        
        # 重複チェック用に追加
        existing_gclids.add(gclid)
    
    # 結果を表示
    print(f"[{datetime.now()}] 変換結果:")
    print(f"  - 総データ数: {total_rows}行")
    print(f"  - サイト名不一致で除外: {site_mismatch_count}行")
    print(f"  - GCLID未検出で除外: {no_gclid_count}行")
    print(f"  - 重複で除外: {duplicate_count}行")
    print(f"  - 新規追加: {new_rows_count}行")
    
    print(f"[{datetime.now()}] サイト別内訳:")
    for site, count in site_counts.items():
        if count > 0:
            print(f"  - {site}: {count}行 ({get_conversion_name(site)})")
    
    # パラメータ行 + ヘッダー + データ を返す
    if new_rows_count > 0:
        return [parameter_row, header] + new_data
    else:
        return []


def upload_to_spreadsheet(csv_path):
    """
    変換したデータをGoogle スプレッドシートにアップロード
    """
    print(f"[{datetime.now()}] Google Sheetsへのアップロードを開始します")
    
    # 環境変数から認証情報を取得
    credentials_json = os.getenv('GOOGLE_CREDENTIALS')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    
    if not credentials_json:
        raise Exception('環境変数 GOOGLE_CREDENTIALS が設定されていません')
    
    if not spreadsheet_id:
        raise Exception('環境変数 SPREADSHEET_ID が設定されていません')
    
    # 認証情報を読み込み
    credentials_dict = json.loads(credentials_json)
    
    # Google Sheets API の認証
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        credentials_dict,
        scope
    )
    
    gc = gspread.authorize(credentials)
    
    # スプレッドシートを開く
    spreadsheet = gc.open_by_key(spreadsheet_id)
    print(f"[{datetime.now()}] スプレッドシートを開きました")
    
    # ワークシート名
    sheet_name = '成果情報_その他'
    
    # ワークシートを取得または作成
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"[{datetime.now()}] ワークシート「{sheet_name}」を使用します")
    except:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=1000,
            cols=10
        )
        print(f"[{datetime.now()}] ワークシート「{sheet_name}」を作成しました")
    
    # 既存のGCLIDを取得（重複チェック用）
    # 最初の2行（パラメータ行とヘッダー）をスキップ
    existing_data = worksheet.get_all_values()
    existing_gclids = set()
    
    if len(existing_data) > 2:
        for row in existing_data[2:]:  # 3行目以降
            if len(row) > 0 and row[0]:  # A列にデータがある
                existing_gclids.add(row[0])
        print(f"[{datetime.now()}] 既存のGCLID数: {len(existing_gclids)}件")
    
    # CSVデータを変換
    transformed_data = transform_csv_data(csv_path, existing_gclids)
    
    if not transformed_data:
        print(f"[{datetime.now()}] 新規追加するデータがありません")
        return
    
    # 既存データがない場合はパラメータ行とヘッダーを含めて全て追加
    if len(existing_data) <= 1:
        print(f"[{datetime.now()}] 初回データ追加: パラメータ行 + ヘッダー + {len(transformed_data)-2}行")
        worksheet.clear()
        worksheet.append_rows(transformed_data, value_input_option='RAW')
    else:
        # 既存データがある場合はデータ行のみ追加（パラメータ行とヘッダーをスキップ）
        data_only = transformed_data[2:]  # パラメータ行とヘッダーを除く
        print(f"[{datetime.now()}] 新規データ {len(data_only)}行 を追加します")
        worksheet.append_rows(data_only, value_input_option='RAW')
    
    print(f"[{datetime.now()}] Google Sheetsへのアップロードが完了しました")
    print(f"[{datetime.now()}] スプレッドシートURL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def main():
    try:
        # CSVをダウンロード
        csv_path = login_and_download_csv()
        
        # Google Sheetsにアップロード
        upload_to_spreadsheet(csv_path)
        
        print("=" * 60)
        print(f"[{datetime.now()}] すべての処理が正常に完了しました")
        print("=" * 60)
        
    except Exception as e:
        print("=" * 60)
        print(f"[{datetime.now()}] エラーが発生しました: ")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
