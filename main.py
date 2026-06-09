# app.py
from flask import Flask, render_template, jsonify, request
import xml.etree.ElementTree as ET
import os
import sqlite3
from datetime import datetime
import json
import zipfile
import io
import threading
import time
import hashlib
from pathlib import Path
import platform
from datetime import timedelta
import shutil
import glob

app = Flask(__name__)
app.secret_key = 'junit_viewer_secret_key_2024'
app.config['TEMPLATES_AUTO_RELOAD'] = True

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = 'junit_reports.db'
CONFIG_PATH = 'config.json'
WATCHED_FOLDER = 'watched_reports'

os.makedirs(WATCHED_FOLDER, exist_ok=True)

monitoring_active = False
monitoring_thread = None

def normalize_path(path):
    """Нормализует путь для работы с сетевыми дисками"""
    if not path:
        return path
    
    # Конвертируем обратные слеши в прямые 
    path = str(path).replace('\\', '/')
    
    # Для UNC путей (\\server\share)
    if path.startswith('//') or path.startswith('\\\\'):
        if platform.system() == 'Windows':
            path = path.replace('/', '\\')
        return path
    
    return path

def ensure_network_access(path):
    """Проверяет доступность сетевого пути"""
    if not path:
        return False
    
    path = normalize_path(path)
    
    # Проверяем доступность
    if os.path.exists(path):
        return True
    
    # дополнительные проверки
    if platform.system() == 'Windows':
        try:
            # Пробуем получить список файлов
            if os.path.exists(path):
                return True
        except:
            pass
        
        print(f"⚠️ Network path not accessible: {path}")
        print(f"   Убедитесь что:")
        print(f"   1. Сетевой диск смонтирован (net use)")
        print(f"   2. У вас есть права доступа")
        print(f"   3. Путь правильный")
        return False
    
    return False

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Таблица для групп отчетов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS report_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT UNIQUE NOT NULL,
            created_date TEXT NOT NULL,
            created_date_str TEXT NOT NULL,
            group_data TEXT NOT NULL,
            file_count INTEGER,
            combined_summary TEXT,
            is_auto BOOLEAN DEFAULT 0,
            display_name TEXT DEFAULT ''
        )
    ''')
    
    # Таблица для обработанных файлов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            file_hash TEXT NOT NULL,
            processed_date TEXT NOT NULL,
            group_id TEXT
        )
    ''')
    
    # Индексы
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_date ON report_groups(created_date_str)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed_hash ON processed_files(file_hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed_path ON processed_files(file_path)')
    
    conn.commit()
    
    # Проверяем создание таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📋 Tables in DB: {[t[0] for t in tables]}")
    
    conn.close()
    print("✅ Database initialized")

def load_config():
    default_config = {
        'theme': 'dark',
        'auto_monitoring': True,
        'watched_folder': WATCHED_FOLDER,
        'telegram_enabled': False,
        'telegram_token': '',
        'telegram_chat_id': '',
        'scan_interval': 30,
        'notify_on_new': True,
        'notify_on_failure': True
    }
    
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
            return config
    else:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        return default_config

def save_config(config):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

config = load_config()

class JUnitWebParser:
    @staticmethod
    def parse_xml(filepath: str):
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        suites_data = []
        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0
        total_time = 0.0
        
        if root.tag == 'testsuites':
            suites = root.findall('testsuite')
        else:
            suites = [root]
        
        for suite in suites:
            suite_tests = int(suite.get('tests', 0))
            suite_failures = int(suite.get('failures', 0))
            suite_errors = int(suite.get('errors', 0))
            suite_skipped = int(suite.get('skipped', 0))
            suite_time = float(suite.get('time', 0.0))
            
            total_tests += suite_tests
            total_failures += suite_failures
            total_errors += suite_errors
            total_skipped += suite_skipped
            total_time += suite_time
            
            test_cases = []
            for case in suite.findall('testcase'):
                case_data = {
                    'name': case.get('name', 'unknown'),
                    'classname': case.get('classname', 'unknown'),
                    'time': float(case.get('time', 0.0)),
                    'status': 'passed',
                    'message': None,
                    'traceback': None
                }
                
                failure = case.find('failure')
                error = case.find('error')
                skipped = case.find('skipped')
                
                if failure is not None:
                    case_data['status'] = 'failed'
                    case_data['message'] = failure.get('message', '')
                    case_data['traceback'] = failure.text
                elif error is not None:
                    case_data['status'] = 'error'
                    case_data['message'] = error.get('message', '')
                    case_data['traceback'] = error.text
                elif skipped is not None:
                    case_data['status'] = 'skipped'
                    case_data['message'] = skipped.get('message', '')
                
                test_cases.append(case_data)
            
            suites_data.append({
                'name': suite.get('name', 'unknown'),
                'tests': suite_tests,
                'failures': suite_failures,
                'errors': suite_errors,
                'skipped': suite_skipped,
                'time': suite_time,
                'test_cases': test_cases
            })
        
        passed = total_tests - total_failures - total_errors - total_skipped
        pass_rate = round(passed / total_tests * 100, 2) if total_tests > 0 else 0
        
        status = 'perfect' if pass_rate == 100 else ('good' if pass_rate >= 80 else ('warning' if pass_rate >= 50 else 'critical'))
        
        return {
            'suites': suites_data,
            'summary': {
                'total_tests': total_tests,
                'total_failures': total_failures,
                'total_errors': total_errors,
                'total_skipped': total_skipped,
                'total_passed': passed,
                'total_time': round(total_time, 3),
                'pass_rate': pass_rate,
                'status': status
            }
        }

def get_file_hash(filepath):
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()

def is_file_processed(filepath):
    file_hash = get_file_hash(filepath)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM processed_files WHERE file_hash = ?', (file_hash,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_file_processed(filepath, group_id):
    file_hash = get_file_hash(filepath)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO processed_files (file_path, file_hash, processed_date, group_id)
        VALUES (?, ?, ?, ?)
    ''', (filepath, file_hash, datetime.now().isoformat(), group_id))
    conn.commit()
    conn.close()

def save_group_to_db(group_data, group_id, is_auto=False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    now = datetime.now()
    
    display_name = group_data.get('display_name', '')
    if not display_name and group_data.get('reports'):
        if len(group_data['reports']) == 1:
            display_name = group_data['reports'][0].get('filename', 'Report')
        else:
            display_name = f"Batch ({len(group_data['reports'])} files)"
    
    cursor.execute('''
        INSERT OR REPLACE INTO report_groups 
        (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        group_id,
        now.isoformat(),
        now.strftime('%Y-%m-%d'),
        json.dumps(group_data, ensure_ascii=False),
        group_data['file_count'],
        json.dumps(group_data['combined_summary'], ensure_ascii=False),
        1 if is_auto else 0,
        display_name
    ))
    
    conn.commit()
    conn.close()

def process_file(filepath):
    try:
        parser = JUnitWebParser()
        data = parser.parse_xml(filepath)
        filename = os.path.basename(filepath)
        data['filename'] = filename
        
        combined_summary = data['summary']
        combined_summary['total_files'] = 1
        
        group_id = f"auto_{datetime.now().timestamp()}_{filename}"
        group_data = {
            'reports': [data],
            'combined_summary': combined_summary,
            'created_at': datetime.now().isoformat(),
            'file_count': 1,
            'is_auto': True,
            'display_name': filename
        }
        
        save_group_to_db(group_data, group_id, is_auto=True)
        mark_file_processed(filepath, group_id)
        
        print(f"✅ Auto-processed: {filename}")
        return group_id
    except Exception as e:
        print(f"❌ Error processing {filepath}: {e}")
        return None

def scan_watched_folder():
    if not config['auto_monitoring']:
        return
    
    watched_path = config['watched_folder']
    if not watched_path:
        return
    
    # Нормализуем путь
    watched_path = normalize_path(watched_path)
    
    # Проверяем доступность сетевого пути
    if not ensure_network_access(watched_path):
        print(f"❌ Network path not available: {watched_path}")
        return
    
    # Создаем папку если её нет
    try:
        if not os.path.exists(watched_path):
            os.makedirs(watched_path, exist_ok=True)
            print(f"📁 Created: {watched_path}")
            return
    except Exception as e:
        print(f"⚠️ Cannot create/access {watched_path}: {e}")
        return
    
    # Сканируем
    try:
        files = []
        try:
            for f in os.listdir(watched_path):
                if f.lower().endswith('.xml'):
                    files.append(os.path.join(watched_path, f))
        except PermissionError:
            print(f"❌ Permission denied: {watched_path}")
            return
        except OSError as e:
            print(f"❌ OS error: {e}")
            return
        
        new_files = [f for f in files if not is_file_processed(f)]
        
        for filepath in new_files:
            print(f"📄 Found: {os.path.basename(filepath)}")
            process_file(filepath)
            
    except Exception as e:
        print(f"⚠️ Scan error: {e}")

def monitoring_loop():
    global monitoring_active
    print("🔄 Monitoring started")
    while monitoring_active:
        try:
            scan_watched_folder()
        except Exception as e:
            print(f"⚠️ Monitoring error: {e}")
        time.sleep(config['scan_interval'])

def start_monitoring():
    global monitoring_active, monitoring_thread
    if monitoring_active:
        return
    monitoring_active = True
    monitoring_thread = threading.Thread(target=monitoring_loop, daemon=True)
    monitoring_thread.start()
    print("✅ Monitoring thread started")

def stop_monitoring():
    global monitoring_active
    monitoring_active = False
    print("⏹ Monitoring stopped")

if config['auto_monitoring']:
    start_monitoring()

init_db()

# ========== FLASK ROUTES ==========

@app.route('/')
def index():
    return render_template('index.html', config=config)

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html', config=config)

@app.route('/settings')
def settings_page():
    return render_template('settings.html', config=config)

@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    global config
    if request.method == 'GET':
        return jsonify(config)
    else:
        new_config = request.json
        old_auto = config.get('auto_monitoring')
        config.update(new_config)
        save_config(config)
        
        if old_auto != config.get('auto_monitoring'):
            if config.get('auto_monitoring'):
                start_monitoring()
            else:
                stop_monitoring()
        
        return jsonify({'success': True, 'config': config})

@app.route('/api/dates')
def get_dates():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT created_date_str FROM report_groups ORDER BY created_date_str DESC')
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()
    return jsonify({'dates': dates})

@app.route('/api/reports/by-date/<date_str>')
def get_reports_by_date(date_str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT group_id, created_date, file_count, group_data, is_auto, display_name
        FROM report_groups 
        WHERE created_date_str = ?
        ORDER BY created_date DESC
    ''', (date_str,))
    
    rows = cursor.fetchall()
    conn.close()
    
    groups = []
    for row in rows:
        group_data = json.loads(row[3])
        display_name = row[5] if len(row) > 5 and row[5] else group_data.get('display_name', '')
        if not display_name and group_data.get('reports'):
            if len(group_data['reports']) == 1:
                display_name = group_data['reports'][0].get('filename', 'Report')
            else:
                display_name = f"Batch ({len(group_data['reports'])} files)"
        
        groups.append({
            'group_id': row[0],
            'created_date': row[1],
            'file_count': row[2],
            'is_auto': bool(row[4]),
            'display_name': display_name,
            'combined_summary': group_data.get('combined_summary', {})
        })
    
    return jsonify({'reports': [], 'groups': groups, 'date': date_str})

@app.route('/api/scan', methods=['POST'])
def manual_scan():
    scan_watched_folder()
    return jsonify({'success': True, 'message': 'Scan completed'})

@app.route('/api/check-path', methods=['POST'])
def check_path():
    """Проверяет доступность пути (для сетевых дисков)"""
    data = request.json
    path = data.get('path', '')
    
    path = normalize_path(path)
    
    result = {
        'path': path,
        'exists': False,
        'accessible': False,
        'is_network': False,
        'message': ''
    }
    
    # Определяем сетевой путь
    if path.startswith('\\\\') or path.startswith('//'):
        result['is_network'] = True
        result['message'] = 'Network path (UNC)'
    elif ':' in path and len(path) > 2 and path[1] == ':' and path[2] not in ['\\', '/']:
        # Монтированный сетевой диск
        result['is_network'] = True
        result['message'] = 'Network drive'
    
    try:
        if os.path.exists(path):
            result['exists'] = True
            result['accessible'] = True
            result['message'] = '✅ Accessible'
        else:
            result['message'] = '❌ Path not found'
    except Exception as e:
        result['message'] = f'⚠️ Error: {str(e)[:50]}'
    
    return jsonify(result)

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        xml_files = [f for f in files if f.filename.endswith('.xml')]
        
        if not xml_files:
            return jsonify({'error': 'No XML files found'}), 400
        
        parser = JUnitWebParser()
        reports_data = []
        combined_summary = {
            'total_tests': 0,
            'total_failures': 0,
            'total_errors': 0,
            'total_skipped': 0,
            'total_passed': 0,
            'total_time': 0.0
        }
        
        for file in xml_files:
            temp_path = f"temp_{datetime.now().timestamp()}_{file.filename}"
            file.save(temp_path)
            
            data = parser.parse_xml(temp_path)
            data['filename'] = file.filename
            reports_data.append(data)
            
            combined_summary['total_tests'] += data['summary']['total_tests']
            combined_summary['total_failures'] += data['summary']['total_failures']
            combined_summary['total_errors'] += data['summary']['total_errors']
            combined_summary['total_skipped'] += data['summary']['total_skipped']
            combined_summary['total_passed'] += data['summary']['total_passed']
            combined_summary['total_time'] += data['summary']['total_time']
            
            os.remove(temp_path)
        
        if combined_summary['total_tests'] > 0:
            combined_summary['pass_rate'] = round(
                combined_summary['total_passed'] / combined_summary['total_tests'] * 100, 2
            )
        
        pass_rate = combined_summary['pass_rate']
        if pass_rate == 100:
            combined_summary['status'] = 'perfect'
        elif pass_rate >= 80:
            combined_summary['status'] = 'good'
        elif pass_rate >= 50:
            combined_summary['status'] = 'warning'
        else:
            combined_summary['status'] = 'critical'
        
        group_id = str(int(datetime.now().timestamp()))
        
        if len(xml_files) == 1:
            display_name = xml_files[0].filename
        else:
            display_name = f"Batch ({len(xml_files)} files)"
        
        group_data = {
            'reports': reports_data,
            'combined_summary': combined_summary,
            'created_at': datetime.now().isoformat(),
            'file_count': len(xml_files),
            'is_auto': False,
            'display_name': display_name
        }
        
        save_group_to_db(group_data, group_id, is_auto=False)
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'file_count': len(xml_files),
            'combined_summary': combined_summary,
            'display_name': display_name
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/upload/zip', methods=['POST'])
def upload_zip():
    try:
        if 'zipfile' not in request.files:
            return jsonify({'error': 'No zip file uploaded'}), 400
        
        zip_file = request.files['zipfile']
        if zip_file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not zip_file.filename.endswith('.zip'):
            return jsonify({'error': 'Only ZIP files are supported'}), 400
        
        zip_content = zip_file.read()
        zip_io = io.BytesIO(zip_content)
        
        reports_data = []
        combined_summary = {
            'total_tests': 0,
            'total_failures': 0,
            'total_errors': 0,
            'total_skipped': 0,
            'total_passed': 0,
            'total_time': 0.0,
            'total_files': 0
        }
        
        parser = JUnitWebParser()
        
        with zipfile.ZipFile(zip_io, 'r') as zf:
            for file_info in zf.filelist:
                if file_info.filename.endswith('.xml'):
                    xml_content = zf.read(file_info.filename)
                    temp_path = f"temp_{datetime.now().timestamp()}_{os.path.basename(file_info.filename)}"
                    with open(temp_path, 'wb') as f:
                        f.write(xml_content)
                    
                    try:
                        data = parser.parse_xml(temp_path)
                        data['filename'] = os.path.basename(file_info.filename)
                        reports_data.append(data)
                        
                        combined_summary['total_tests'] += data['summary']['total_tests']
                        combined_summary['total_failures'] += data['summary']['total_failures']
                        combined_summary['total_errors'] += data['summary']['total_errors']
                        combined_summary['total_skipped'] += data['summary']['total_skipped']
                        combined_summary['total_passed'] += data['summary']['total_passed']
                        combined_summary['total_time'] += data['summary']['total_time']
                        combined_summary['total_files'] += 1
                    finally:
                        os.remove(temp_path)
        
        if not reports_data:
            return jsonify({'error': 'No XML files found in ZIP archive'}), 400
        
        if combined_summary['total_tests'] > 0:
            combined_summary['pass_rate'] = round(
                combined_summary['total_passed'] / combined_summary['total_tests'] * 100, 2
            )
        
        pass_rate = combined_summary['pass_rate']
        if pass_rate == 100:
            combined_summary['status'] = 'perfect'
        elif pass_rate >= 80:
            combined_summary['status'] = 'good'
        elif pass_rate >= 50:
            combined_summary['status'] = 'warning'
        else:
            combined_summary['status'] = 'critical'
        
        group_id = str(int(datetime.now().timestamp()))
        display_name = f"ZIP ({len(reports_data)} files)"
        
        group_data = {
            'reports': reports_data,
            'combined_summary': combined_summary,
            'created_at': datetime.now().isoformat(),
            'file_count': len(reports_data),
            'is_auto': False,
            'display_name': display_name
        }
        
        save_group_to_db(group_data, group_id, is_auto=False)
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'file_count': len(reports_data),
            'combined_summary': combined_summary,
            'display_name': display_name
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/report/<group_id>')
def view_report(group_id):
    return render_template('group_report.html', group_id=group_id, config=config)

@app.route('/api/report/<group_id>')
def get_report_data(group_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return jsonify(json.loads(result[0]))
    return jsonify({'error': 'Report not found'}), 404

@app.route('/api/delete-report/<group_id>', methods=['DELETE'])
def delete_report(group_id):
    """Удаляет отчет из базы данных"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем существует ли отчет
        cursor.execute('SELECT display_name FROM report_groups WHERE group_id = ?', (group_id,))
        report = cursor.fetchone()
        
        if not report:
            conn.close()
            return jsonify({'error': 'Report not found'}), 404
        
        # Удаляем отчет
        cursor.execute('DELETE FROM report_groups WHERE group_id = ?', (group_id,))
        
        # Также удаляем связанные записи из processed_files (если есть)
        cursor.execute('DELETE FROM processed_files WHERE group_id = ?', (group_id,))
        
        conn.commit()
        conn.close()
        
        print(f"🗑 Deleted report: {report[0]} ({group_id})")
        return jsonify({'success': True, 'message': f'Report "{report[0]}" deleted'})
        
    except Exception as e:
        print(f"❌ Delete error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/delete-all-reports', methods=['DELETE'])
def delete_all_reports():
    """Удаляет ВСЕ отчеты из базы данных"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем количество
        cursor.execute('SELECT COUNT(*) FROM report_groups')
        count = cursor.fetchone()[0]
        
        # Удаляем все
        cursor.execute('DELETE FROM report_groups')
        cursor.execute('DELETE FROM processed_files')
        
        conn.commit()
        conn.close()
        
        print(f"🗑 Deleted ALL {count} reports")
        return jsonify({'success': True, 'message': f'Deleted {count} reports'})
        
    except Exception as e:
        print(f"❌ Delete error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/clear-processed-files', methods=['DELETE'])
def clear_processed_files():
    """Очищает историю обработанных файлов (не удаляя отчеты)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM processed_files')
        count = cursor.fetchone()[0]
        
        cursor.execute('DELETE FROM processed_files')
        
        conn.commit()
        conn.close()
        
        print(f"🗑 Cleared {count} processed file records")
        return jsonify({'success': True, 'message': f'Cleared {count} processed file records'})
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500
    
# ========== DATA MANAGEMENT ENDPOINTS ==========

@app.route('/api/export', methods=['GET'])
def export_data():
    """Экспорт всех данных в JSON"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем все отчеты
        cursor.execute('SELECT group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name FROM report_groups')
        reports = cursor.fetchall()
        
        # Получаем историю обработанных файлов
        cursor.execute('SELECT file_path, file_hash, processed_date, group_id FROM processed_files')
        processed = cursor.fetchall()
        
        conn.close()
        
        export_data = {
            'export_date': datetime.now().isoformat(),
            'version': '1.0',
            'reports': [
                {
                    'group_id': r[0],
                    'created_date': r[1],
                    'created_date_str': r[2],
                    'group_data': json.loads(r[3]),
                    'file_count': r[4],
                    'combined_summary': json.loads(r[5]),
                    'is_auto': bool(r[6]),
                    'display_name': r[7]
                }
                for r in reports
            ],
            'processed_files': [
                {
                    'file_path': p[0],
                    'file_hash': p[1],
                    'processed_date': p[2],
                    'group_id': p[3]
                }
                for p in processed
            ]
        }
        
        return jsonify(export_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/import', methods=['POST'])
def import_data():
    """Импорт данных из JSON"""
    try:
        data = request.json
        
        if not data or 'reports' not in data:
            return jsonify({'error': 'Invalid data format'}), 400
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Импортируем отчеты
        for report in data['reports']:
            cursor.execute('''
                INSERT OR REPLACE INTO report_groups 
                (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                report['group_id'],
                report['created_date'],
                report['created_date_str'],
                json.dumps(report['group_data']),
                report['file_count'],
                json.dumps(report['combined_summary']),
                1 if report['is_auto'] else 0,
                report['display_name']
            ))
        
        # Импортируем обработанные файлы
        if 'processed_files' in data:
            for pf in data['processed_files']:
                cursor.execute('''
                    INSERT OR REPLACE INTO processed_files (file_path, file_hash, processed_date, group_id)
                    VALUES (?, ?, ?, ?)
                ''', (pf['file_path'], pf['file_hash'], pf['processed_date'], pf['group_id']))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Imported {len(data["reports"])} reports'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/backup', methods=['POST'])
def backup_db():
    """Создание бэкапа базы данных"""
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(backup_dir, f'vunit_backup_{timestamp}.db')
        
        shutil.copy2(DB_PATH, backup_path)
        
        # Удаляем старые бэкапы (оставляем 10)
        backups = sorted(glob.glob(os.path.join(backup_dir, 'vunit_backup_*.db')))
        for old_backup in backups[:-10]:
            os.remove(old_backup)
        
        return jsonify({'success': True, 'message': f'Backup created: {os.path.basename(backup_path)}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/restore', methods=['POST'])
def restore_db():
    """Восстановление из последнего бэкапа"""
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        backups = sorted(glob.glob(os.path.join(backup_dir, 'vunit_backup_*.db')))
        
        if not backups:
            return jsonify({'error': 'No backups found'}), 404
        
        latest_backup = backups[-1]
        
        # Создаем бэкап текущей БД перед восстановлением
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pre_restore_backup = os.path.join(backup_dir, f'pre_restore_{timestamp}.db')
        shutil.copy2(DB_PATH, pre_restore_backup)
        
        # Восстанавливаем
        shutil.copy2(latest_backup, DB_PATH)
        
        return jsonify({'success': True, 'message': f'Restored from: {os.path.basename(latest_backup)}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cleanup', methods=['DELETE'])
def cleanup_old_reports():
    """Удаляет отчеты старше 30 дней"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Дата 30 дней назад
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Получаем ID для удаления
        cursor.execute('SELECT group_id FROM report_groups WHERE created_date_str < ?', (cutoff_date,))
        old_reports = cursor.fetchall()
        
        # Удаляем отчеты
        cursor.execute('DELETE FROM report_groups WHERE created_date_str < ?', (cutoff_date,))
        deleted_count = cursor.rowcount
        
        # Удаляем связанные processed_files (оставляем их на всякий случай)
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Deleted {deleted_count} old reports'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("JUnit Enterprise Test Intelligence Platform")
    print("=" * 60)
    print(f"URLs:")
    print(f"  Main:     http://localhost:5000")
    print(f"  Dashboard: http://localhost:5000/dashboard")
    print(f"  Settings:  http://localhost:5000/settings")
    print(f"Watch folder: {config['watched_folder']}")
    print(f"Monitoring: {'ACTIVE' if config['auto_monitoring'] else 'OFF'}")
    print("=" * 60)
    app.run(debug=False, host='0.0.0.0', port=5000)