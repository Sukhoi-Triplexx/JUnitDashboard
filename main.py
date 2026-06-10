# app.py
from flask import Flask, render_template, jsonify, request, send_file
import xml.etree.ElementTree as ET
import os
import sqlite3
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json
import zipfile
import io
import threading
import time
import hashlib
from pathlib import Path
import platform
import signal
import gc
from functools import wraps
import traceback
import shutil
import glob
import secrets
from urllib.parse import urlparse
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import csv
from io import StringIO, BytesIO

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['MAX_CONTENT_LENGTH'] = 300 * 1024 * 1024  # 300MB
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

# ========== КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ ==========
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///vunit_reports.db')
USE_POSTGRES = DATABASE_URL.startswith('postgresql://')
db_pool = None

def get_db_connection():
    if USE_POSTGRES:
        return db_pool.getconn()
    else:
        conn = sqlite3.connect('vunit_reports.db')
        conn.row_factory = sqlite3.Row
        return conn

def return_db_connection(conn):
    if USE_POSTGRES and db_pool:
        db_pool.putconn(conn)
    else:
        conn.close()

def init_db():
    if USE_POSTGRES:
        init_postgres_db()
    else:
        init_sqlite_db()

def init_sqlite_db():
    conn = sqlite3.connect('vunit_reports.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS report_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT UNIQUE NOT NULL,
            created_date TEXT NOT NULL,
            created_date_str TEXT NOT NULL,
            group_data TEXT NOT NULL,
            file_count INTEGER DEFAULT 0,
            combined_summary TEXT,
            is_auto BOOLEAN DEFAULT 0,
            display_name TEXT DEFAULT '',
            pass_rate REAL DEFAULT 0,
            total_tests INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            file_hash TEXT NOT NULL,
            processed_date TEXT NOT NULL,
            group_id TEXT,
            file_size INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'viewer',
            created_at TEXT NOT NULL,
            last_login TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            details TEXT,
            ip_address TEXT,
            created_at TEXT NOT NULL
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_date ON report_groups(created_date_str)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_pass_rate ON report_groups(pass_rate)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed_hash ON processed_files(file_hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)')
    
    conn.commit()
    conn.close()
    print("✅ SQLite database initialized")

def init_postgres_db():
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS report_groups (
                id SERIAL PRIMARY KEY,
                group_id TEXT UNIQUE NOT NULL,
                created_date TIMESTAMP NOT NULL,
                created_date_str DATE NOT NULL,
                group_data JSONB NOT NULL,
                file_count INTEGER DEFAULT 0,
                combined_summary JSONB,
                is_auto BOOLEAN DEFAULT FALSE,
                display_name TEXT DEFAULT '',
                pass_rate REAL DEFAULT 0,
                total_tests INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id SERIAL PRIMARY KEY,
                file_path TEXT UNIQUE NOT NULL,
                file_hash TEXT NOT NULL,
                processed_date TIMESTAMP NOT NULL,
                group_id TEXT,
                file_size BIGINT DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'viewer',
                created_at TIMESTAMP NOT NULL,
                last_login TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                created_at TIMESTAMP NOT NULL
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_date ON report_groups(created_date_str)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_pass_rate ON report_groups(pass_rate)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed_hash ON processed_files(file_hash)')
        
        conn.commit()
        db_pool.putconn(conn)
        print("✅ PostgreSQL database initialized with connection pool")
        
    except Exception as e:
        print(f"❌ PostgreSQL init error: {e}")
        print("   Falling back to SQLite")
        global USE_POSTGRES
        USE_POSTGRES = False
        init_sqlite_db()

# ========== ОТКАЗОУСТОЙЧИВОСТЬ ==========

FILE_PROCESS_TIMEOUT = 300
MAX_FILE_SIZE = 300 * 1024 * 1024
MAX_CONCURRENT_UPLOADS = 3

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil not installed. Install with: pip install psutil")

upload_semaphore = threading.Semaphore(MAX_CONCURRENT_UPLOADS)

if platform.system() == 'Windows':
    def with_timeout(seconds=FILE_PROCESS_TIMEOUT):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = [None]
                error = [None]
                
                def target():
                    try:
                        result[0] = func(*args, **kwargs)
                    except Exception as e:
                        error[0] = e
                
                thread = threading.Thread(target=target)
                thread.daemon = True
                thread.start()
                thread.join(timeout=seconds)
                
                if thread.is_alive():
                    raise TimeoutError(f"Function {func.__name__} timed out after {seconds}s")
                if error[0]:
                    raise error[0]
                return result[0]
            return wrapper
        return decorator
else:
    def with_timeout(seconds=FILE_PROCESS_TIMEOUT):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Function {func.__name__} timed out after {seconds}s")
                
                original_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(seconds)
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, original_handler)
                return result
            return wrapper
        return decorator

def with_semaphore(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        acquired = upload_semaphore.acquire(timeout=60)
        if not acquired:
            return jsonify({'error': 'Server busy, please try again later'}), 503
        try:
            return func(*args, **kwargs)
        finally:
            upload_semaphore.release()
    return wrapper

def retry_on_db_failure(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay * (attempt + 1))
            return None
        return wrapper
    return decorator

def safe_parse(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ET.ParseError as e:
            print(f"❌ XML Parse Error: {e}")
            return None
        except MemoryError as e:
            print(f"❌ Memory Error: {e}")
            gc.collect()
            return None
        except TimeoutError as e:
            print(f"❌ Timeout Error: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            traceback.print_exc()
            return None
    return wrapper

def check_memory():
    if not PSUTIL_AVAILABLE:
        return True
    try:
        mem = psutil.virtual_memory()
        if mem.percent > 90:
            print(f"⚠️ Low memory: {mem.percent}%")
            gc.collect()
            return False
        return True
    except:
        return True

def audit_log(user_id, action, details, ip_address):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if USE_POSTGRES:
            cursor.execute(
                "INSERT INTO audit_log (user_id, action, details, ip_address, created_at) VALUES (%s, %s, %s, %s, %s)",
                (user_id, action, details, ip_address, datetime.now())
            )
        else:
            cursor.execute(
                "INSERT INTO audit_log (user_id, action, details, ip_address, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, action, details, ip_address, datetime.now().isoformat())
            )
        conn.commit()
        if USE_POSTGRES:
            return_db_connection(conn)
        else:
            conn.close()
    except Exception as e:
        print(f"⚠️ Audit log error: {e}")

# ========== ПАРСЕР ==========

class StreamingJUnitParser:
    @staticmethod
    def parse_large_xml(filepath, chunk_size=1000):
        try:
            context = ET.iterparse(filepath, events=('end',))
            
            suite_data = {
                'name': 'unknown',
                'tests': 0,
                'failures': 0,
                'errors': 0,
                'skipped': 0,
                'time': 0.0,
                'test_cases': []
            }
            
            for event, elem in context:
                if elem.tag == 'testsuite':
                    if suite_data['test_cases']:
                        yield suite_data
                    
                    suite_data = {
                        'name': elem.get('name', 'unknown'),
                        'tests': int(elem.get('tests', 0)),
                        'failures': int(elem.get('failures', 0)),
                        'errors': int(elem.get('errors', 0)),
                        'skipped': int(elem.get('skipped', 0)),
                        'time': float(elem.get('time', 0.0)),
                        'test_cases': []
                    }
                    elem.clear()
                    
                elif elem.tag == 'testcase':
                    test_case = {
                        'name': elem.get('name', 'unknown'),
                        'classname': elem.get('classname', 'unknown'),
                        'time': float(elem.get('time', 0.0)),
                        'status': 'passed',
                        'message': None,
                        'traceback': None
                    }
                    
                    for child in elem:
                        if child.tag in ('failure', 'error', 'skipped'):
                            test_case['status'] = child.tag
                            test_case['message'] = child.get('message', '')
                            test_case['traceback'] = child.text
                            break
                    
                    suite_data['test_cases'].append(test_case)
                    elem.clear()
                    
                    if len(suite_data['test_cases']) >= chunk_size:
                        yield suite_data
                        suite_data['test_cases'] = []
            
            if suite_data['test_cases']:
                yield suite_data
                
        except Exception as e:
            print(f"❌ Streaming parser error: {e}")
            yield None

class JUnitWebParser:
    @staticmethod
    @safe_parse
    @with_timeout(FILE_PROCESS_TIMEOUT)
    def parse_xml(filepath: str):
        file_size = os.path.getsize(filepath)
        
        if file_size > MAX_FILE_SIZE:
            raise MemoryError(f"File too large: {file_size} bytes. Max: {MAX_FILE_SIZE} bytes")
        
        if not check_memory():
            raise MemoryError("System memory is too low")
        
        if file_size > 10 * 1024 * 1024:
            print(f"📦 Using streaming parser: {os.path.basename(filepath)} ({file_size / 1024 / 1024:.1f}MB)")
            return JUnitWebParser._parse_large_xml(filepath)
        
        return JUnitWebParser._parse_normal_xml(filepath)
    
    @staticmethod
    def _parse_normal_xml(filepath: str):
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        suites_data = []
        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0
        total_time = 0.0
        
        suites = root.findall('testsuite') if root.tag == 'testsuites' else [root]
        
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
    
    @staticmethod
    def _parse_large_xml(filepath: str):
        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0
        total_time = 0.0
        suites_data = []
        
        for suite_chunk in StreamingJUnitParser.parse_large_xml(filepath):
            if suite_chunk is None:
                continue
                
            total_tests += suite_chunk.get('tests', 0)
            total_failures += suite_chunk.get('failures', 0)
            total_errors += suite_chunk.get('errors', 0)
            total_skipped += suite_chunk.get('skipped', 0)
            total_time += suite_chunk.get('time', 0.0)
            
            suites_data.append({
                'name': suite_chunk.get('name', 'unknown'),
                'tests': suite_chunk.get('tests', 0),
                'failures': suite_chunk.get('failures', 0),
                'errors': suite_chunk.get('errors', 0),
                'skipped': suite_chunk.get('skipped', 0),
                'time': suite_chunk.get('time', 0.0),
                'test_cases': suite_chunk.get('test_cases', [])
            })
            
            gc.collect()
        
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

parser = JUnitWebParser()

# ========== КОНФИГУРАЦИЯ ==========

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'config.json')
WATCHED_FOLDER = os.environ.get('WATCHED_FOLDER', os.path.join(ROOT_DIR, 'watched_reports'))

os.makedirs(WATCHED_FOLDER, exist_ok=True)
os.makedirs(os.path.join(ROOT_DIR, 'backups'), exist_ok=True)

def load_config():
    default_config = {
        'theme': 'orange',
        'auto_monitoring': True,
        'watched_folder': WATCHED_FOLDER,
        'telegram_enabled': False,
        'telegram_token': '',
        'telegram_chat_id': '',
        'email_enabled': False,
        'smtp_server': '',
        'smtp_port': 587,
        'smtp_user': '',
        'smtp_password': '',
        'email_from': '',
        'email_to': '',
        'email_on_new_report': True,
        'email_on_failure': True,
        'scan_interval': 30,
        'notify_on_new': True,
        'notify_on_failure': True,
        'postgres_enabled': False,
        'pg_host': 'localhost',
        'pg_port': 5432,
        'pg_database': 'vunit',
        'pg_user': 'postgres',
        'pg_password': '',
        'auto_refresh_enabled': True,
        'refresh_interval': 30
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

# Загружаем конфиг ОДИН раз
config = load_config()

# Определяем тип базы данных на основе конфига
if config.get('postgres_enabled', False):
    DATABASE_URL = f"postgresql://{config['pg_user']}:{config['pg_password']}@{config['pg_host']}:{config['pg_port']}/{config['pg_database']}"
    USE_POSTGRES = True
    print(f"✅ Using PostgreSQL: {config['pg_host']}:{config['pg_port']}/{config['pg_database']}")
else:
    USE_POSTGRES = False
    print("✅ Using SQLite")

def save_config(config):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

# ========== БЭКАПЫ ==========

def backup_database():
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if USE_POSTGRES:
            backup_path = os.path.join(backup_dir, f'vunit_pg_backup_{timestamp}.sql')
            db_url = urlparse(DATABASE_URL)
            cmd = f'PGPASSWORD={db_url.password} pg_dump -h {db_url.hostname} -p {db_url.port or 5432} -U {db_url.username} -d {db_url.path[1:]} > "{backup_path}"'
            os.system(cmd)
        else:
            backup_path = os.path.join(backup_dir, f'vunit_sqlite_backup_{timestamp}.db')
            shutil.copy2('vunit_reports.db', backup_path)
        
        print(f"✅ Database backup: {backup_path}")
        
        pattern = os.path.join(backup_dir, 'vunit_*_backup_*')
        backups = sorted(glob.glob(pattern))
        for old_backup in backups[:-10]:
            os.remove(old_backup)
    except Exception as e:
        print(f"⚠️ Backup error: {e}")

def backup_loop():
    while True:
        time.sleep(86400)
        backup_database()

backup_thread = threading.Thread(target=backup_loop, daemon=True)
backup_thread.start()

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========

def normalize_path(path):
    if not path:
        return path
    path = str(path).replace('\\', '/')
    if path.startswith('//') or path.startswith('\\\\'):
        if platform.system() == 'Windows':
            path = path.replace('/', '\\')
    return path

def get_file_hash(filepath):
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            hasher.update(chunk)
    return hasher.hexdigest()

@retry_on_db_failure()
def save_group_to_db(group_data, group_id, is_auto=False):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now()
    summary = group_data.get('combined_summary', {})
    pass_rate = summary.get('pass_rate', 0)
    total_tests = summary.get('total_tests', 0)
    
    display_name = group_data.get('display_name', '')
    if not display_name and group_data.get('reports'):
        if len(group_data['reports']) == 1:
            display_name = group_data['reports'][0].get('filename', 'Report')
        else:
            display_name = f"Batch ({len(group_data['reports'])} files)"
    
    if USE_POSTGRES:
        cursor.execute('''
            INSERT INTO report_groups 
            (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name, pass_rate, total_tests)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (group_id) DO UPDATE SET
                group_data = EXCLUDED.group_data,
                combined_summary = EXCLUDED.combined_summary,
                pass_rate = EXCLUDED.pass_rate,
                total_tests = EXCLUDED.total_tests
        ''', (
            group_id, now, now.date().isoformat(),
            json.dumps(group_data), group_data['file_count'],
            json.dumps(summary), is_auto, display_name, pass_rate, total_tests
        ))
    else:
        cursor.execute('''
            INSERT OR REPLACE INTO report_groups 
            (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name, pass_rate, total_tests)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            group_id, now.isoformat(), now.strftime('%Y-%m-%d'),
            json.dumps(group_data), group_data['file_count'],
            json.dumps(summary), 1 if is_auto else 0, display_name, pass_rate, total_tests
        ))
    
    conn.commit()
    return_db_connection(conn)

def process_file(filepath):
    try:
        data = parser.parse_xml(filepath)
        if data is None:
            print(f"❌ Failed to parse: {filepath}")
            return None
            
        filename = os.path.basename(filepath)
        data['filename'] = filename
        
        group_id = f"auto_{datetime.now().timestamp()}_{filename}"
        group_data = {
            'reports': [data],
            'combined_summary': data['summary'],
            'created_at': datetime.now().isoformat(),
            'file_count': 1,
            'is_auto': True,
            'display_name': filename
        }
        
        save_group_to_db(group_data, group_id, is_auto=True)
        
        file_hash = get_file_hash(filepath)
        conn = get_db_connection()
        cursor = conn.cursor()
        if USE_POSTGRES:
            cursor.execute('''
                INSERT INTO processed_files (file_path, file_hash, processed_date, group_id, file_size)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (file_path) DO UPDATE SET
                    processed_date = EXCLUDED.processed_date
            ''', (filepath, file_hash, datetime.now(), group_id, os.path.getsize(filepath)))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO processed_files (file_path, file_hash, processed_date, group_id, file_size)
                VALUES (?, ?, ?, ?, ?)
            ''', (filepath, file_hash, datetime.now().isoformat(), group_id, os.path.getsize(filepath)))
        conn.commit()
        return_db_connection(conn)
        
        print(f"✅ Auto-processed: {filename}")
        return group_id
    except Exception as e:
        print(f"❌ Error processing {filepath}: {e}")
        traceback.print_exc()
        return None

def scan_watched_folder():
    """Сканирует папку и обрабатывает новые XML файлы"""
    if not config['auto_monitoring']:
        return
    
    watched_path = normalize_path(config['watched_folder'])
    if not os.path.exists(watched_path):
        print(f"⚠️ Watched folder does not exist: {watched_path}")
        return
    
    try:
        # Получаем все XML файлы с их modification time
        all_files = []
        for root, dirs, files in os.walk(watched_path):
            for file in files:
                if file.lower().endswith('.xml'):
                    filepath = os.path.join(root, file)
                    all_files.append({
                        'path': filepath,
                        'name': file,
                        'mtime': os.path.getmtime(filepath),
                        'size': os.path.getsize(filepath)
                    })
        
        if not all_files:
            print(f"📁 No XML files found in {watched_path}")
            return
        
        #print(f"🔍 Scanning {len(all_files)} XML files in {watched_path}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        new_files = []
        
        for file_info in all_files:
            filepath = file_info['path']
            
            # Получаем хэш файла
            file_hash = get_file_hash(filepath)
            
            # Проверяем, существует ли файл в processed_files
            if USE_POSTGRES:
                cursor.execute('SELECT file_path, file_hash, processed_date FROM processed_files WHERE file_path = %s', (filepath,))
            else:
                cursor.execute('SELECT file_path, file_hash, processed_date FROM processed_files WHERE file_path = ?', (filepath,))
            
            existing = cursor.fetchone()
            
            if not existing:
                # Файл вообще не обработан
                new_files.append(file_info)
               # print(f"  🆕 New file (not in DB): {file_info['name']}")
            else:
                # Проверяем, изменился ли файл
                existing_hash = existing[1] if USE_POSTGRES else existing[1]
                if existing_hash != file_hash:
                    # Файл изменился - нужно обработать заново
                    new_files.append(file_info)
                    #print(f"  🔄 Changed file: {file_info['name']} (hash mismatch)")
                else:
                    # Файл уже обработан и не менялся
                    print(f"  ⏭️ Already processed: {file_info['name']}")
        
        #print(f"📊 Found: {len(new_files)} new/changed files, {len(all_files) - len(new_files)} unchanged")
        
        # Обрабатываем новые файлы
        if new_files:
            print(f"📄 Processing {len(new_files)} files...")
            for file_info in new_files:
                print(f"  → {file_info['name']}")
                group_id = process_file(file_info['path'])
                if group_id:
                    print(f"    ✅ Processed successfully, group_id: {group_id}")
                else:
                    print(f"    ❌ Failed to process")
            
            # Принудительно обновляем кэш дат в браузере
            try:
                # Можно отправить уведомление через WebSocket или просто записать в лог
                print(f"📢 New reports available: {len(new_files)} file(s)")
            except:
                pass
        else:
            print("✅ No new or changed files found")
        
        return_db_connection(conn)
        
    except Exception as e:
        print(f"⚠️ Scan error: {e}")
        traceback.print_exc()

monitoring_active = False
monitoring_thread = None

def monitoring_loop():
    global monitoring_active
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
    print("✅ Monitoring started")

def stop_monitoring():
    global monitoring_active
    monitoring_active = False
    print("⏹ Monitoring stopped")

# Инициализируем БД ПОСЛЕ определения USE_POSTGRES
init_db()

if config['auto_monitoring']:
    start_monitoring()

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

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': USE_POSTGRES and db_pool is not None or os.path.exists('vunit_reports.db'),
        'monitoring': config['auto_monitoring']
    }), 200

@app.route('/metrics')
def metrics():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT COUNT(*) FROM report_groups')
        total_reports = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM processed_files')
        total_processed = cursor.fetchone()[0]
        cursor.execute('SELECT AVG(pass_rate) FROM report_groups')
        avg_rate = cursor.fetchone()[0] or 0
    else:
        cursor.execute('SELECT COUNT(*) FROM report_groups')
        total_reports = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM processed_files')
        total_processed = cursor.fetchone()[0]
        cursor.execute('SELECT AVG(pass_rate) FROM report_groups')
        avg_rate = cursor.fetchone()[0] or 0
    
    return_db_connection(conn)
    
    return jsonify({
        'total_reports': total_reports,
        'total_processed_files': total_processed,
        'average_pass_rate': round(avg_rate, 2),
        'monitoring_active': config['auto_monitoring']
    }), 200

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
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT DISTINCT created_date_str FROM report_groups ORDER BY created_date_str DESC')
    else:
        cursor.execute('SELECT DISTINCT created_date_str FROM report_groups ORDER BY created_date_str DESC')
    
    dates = [row[0] for row in cursor.fetchall()]
    return_db_connection(conn)
    return jsonify({'dates': dates})

@app.route('/api/reports/by-date/<date_str>')
def get_reports_by_date(date_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('''
            SELECT group_id, created_date, file_count, group_data, is_auto, display_name
            FROM report_groups 
            WHERE created_date_str = %s
            ORDER BY created_date DESC
        ''', (date_str,))
    else:
        cursor.execute('''
            SELECT group_id, created_date, file_count, group_data, is_auto, display_name
            FROM report_groups 
            WHERE created_date_str = ?
            ORDER BY created_date DESC
        ''', (date_str,))
    
    rows = cursor.fetchall()
    return_db_connection(conn)
    
    groups = []
    for row in rows:
        group_data = json.loads(row[3]) if isinstance(row[3], str) else row[3]
        groups.append({
            'group_id': row[0],
            'created_date': row[1],
            'file_count': row[2],
            'is_auto': bool(row[4]),
            'display_name': row[5] or group_data.get('display_name', 'Report'),
            'combined_summary': group_data.get('combined_summary', {})
        })
    
    return jsonify({'reports': [], 'groups': groups, 'date': date_str})

@app.route('/api/scan', methods=['POST'])
def manual_scan():
    scan_watched_folder()
    return jsonify({'success': True, 'message': 'Scan completed'})

@app.route('/api/check-path', methods=['POST'])
def check_path():
    data = request.json
    path = normalize_path(data.get('path', ''))
    result = {
        'path': path,
        'exists': os.path.exists(path) if path else False,
        'is_network': path.startswith('\\\\') or path.startswith('//')
    }
    return jsonify(result)

@app.route('/upload', methods=['POST'])
@with_semaphore
def upload_file():
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        xml_files = [f for f in files if f.filename and f.filename.endswith('.xml')]
        
        if not xml_files:
            return jsonify({'error': 'No XML files found'}), 400
        
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
            if data is None:
                os.remove(temp_path)
                return jsonify({'error': f'Failed to parse {file.filename}'}), 400
            
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
        display_name = xml_files[0].filename if len(xml_files) == 1 else f"Batch ({len(xml_files)} files)"
        
        group_data = {
            'reports': reports_data,
            'combined_summary': combined_summary,
            'created_at': datetime.now().isoformat(),
            'file_count': len(xml_files),
            'is_auto': False,
            'display_name': display_name
        }
        
        save_group_to_db(group_data, group_id, is_auto=False)
        audit_log(None, 'upload', f'Uploaded {len(xml_files)} files', request.remote_addr)
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'file_count': len(xml_files),
            'combined_summary': combined_summary,
            'display_name': display_name
        })
        
    except MemoryError as e:
        return jsonify({'error': f'File too large: {str(e)}'}), 413
    except TimeoutError as e:
        return jsonify({'error': f'Processing timeout: {str(e)}'}), 408
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/upload/zip', methods=['POST'])
@with_semaphore
def upload_zip():
    try:
        if 'zipfile' not in request.files:
            return jsonify({'error': 'No zip file uploaded'}), 400
        
        zip_file = request.files['zipfile']
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
            'total_time': 0.0
        }
        
        with zipfile.ZipFile(zip_io, 'r') as zf:
            for file_info in zf.filelist:
                if file_info.filename.endswith('.xml'):
                    xml_content = zf.read(file_info.filename)
                    temp_path = f"temp_{datetime.now().timestamp()}_{os.path.basename(file_info.filename)}"
                    with open(temp_path, 'wb') as f:
                        f.write(xml_content)
                    
                    try:
                        data = parser.parse_xml(temp_path)
                        if data:
                            data['filename'] = os.path.basename(file_info.filename)
                            reports_data.append(data)
                            
                            combined_summary['total_tests'] += data['summary']['total_tests']
                            combined_summary['total_failures'] += data['summary']['total_failures']
                            combined_summary['total_errors'] += data['summary']['total_errors']
                            combined_summary['total_skipped'] += data['summary']['total_skipped']
                            combined_summary['total_passed'] += data['summary']['total_passed']
                            combined_summary['total_time'] += data['summary']['total_time']
                    finally:
                        os.remove(temp_path)
        
        if not reports_data:
            return jsonify({'error': 'No valid XML files found in ZIP'}), 400
        
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
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/report/<group_id>')
def view_report(group_id):
    return render_template('group_report.html', group_id=group_id, config=config)

@app.route('/api/report/<group_id>')
def get_report_data(group_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = %s', (group_id,))
    else:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
    
    result = cursor.fetchone()
    return_db_connection(conn)
    
    if result:
        group_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
        return jsonify(group_data)
    return jsonify({'error': 'Report not found'}), 404

@app.route('/api/delete-report/<group_id>', methods=['DELETE'])
def delete_report(group_id):
    """Удаляет отчет из БД и физические файлы из папки"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Сначала получаем информацию об отчете и связанных файлах
        if USE_POSTGRES:
            cursor.execute('SELECT display_name, group_data FROM report_groups WHERE group_id = %s', (group_id,))
        else:
            cursor.execute('SELECT display_name, group_data FROM report_groups WHERE group_id = ?', (group_id,))
        
        report = cursor.fetchone()
        
        if not report:
            return_db_connection(conn)
            return jsonify({'error': 'Report not found'}), 404
        
        display_name = report[0]
        
        # Находим связанные файлы
        if USE_POSTGRES:
            cursor.execute('SELECT file_path FROM processed_files WHERE group_id = %s', (group_id,))
        else:
            cursor.execute('SELECT file_path FROM processed_files WHERE group_id = ?', (group_id,))
        
        files_to_delete = cursor.fetchall()
        
        # Удаляем физические файлы из watched_folder
        deleted_files = []
        failed_deletes = []
        
        for file_row in files_to_delete:
            file_path = file_row[0] if USE_POSTGRES else file_row[0]
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    deleted_files.append(os.path.basename(file_path))
                    print(f"🗑️ Deleted file: {file_path}")
                except Exception as e:
                    failed_deletes.append({'file': os.path.basename(file_path), 'error': str(e)})
                    print(f"❌ Failed to delete {file_path}: {e}")
            else:
                print(f"⚠️ File not found (already deleted): {file_path}")
        
        # Удаляем записи из БД
        if USE_POSTGRES:
            cursor.execute('DELETE FROM report_groups WHERE group_id = %s', (group_id,))
            cursor.execute('DELETE FROM processed_files WHERE group_id = %s', (group_id,))
        else:
            cursor.execute('DELETE FROM report_groups WHERE group_id = ?', (group_id,))
            cursor.execute('DELETE FROM processed_files WHERE group_id = ?', (group_id,))
        
        conn.commit()
        return_db_connection(conn)
        
        audit_log(None, 'delete', f'Deleted report: {display_name} (files: {len(deleted_files)})', request.remote_addr)
        
        return jsonify({
            'success': True, 
            'message': f'Report "{display_name}" deleted',
            'deleted_files': deleted_files,
            'deleted_count': len(deleted_files),
            'failed_deletes': failed_deletes
        })
        
    except Exception as e:
        print(f"❌ Delete error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Замените существующую функцию delete_all_reports на эту:

@app.route('/api/delete-all-reports', methods=['DELETE'])
def delete_all_reports():
    """Удаляет ВСЕ отчеты из БД и ВСЕ файлы из папки"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем все файлы для удаления
        if USE_POSTGRES:
            cursor.execute('SELECT file_path FROM processed_files')
        else:
            cursor.execute('SELECT file_path FROM processed_files')
        
        all_files = cursor.fetchall()
        
        # Получаем статистику
        if USE_POSTGRES:
            cursor.execute('SELECT COUNT(*) FROM report_groups')
            report_count = cursor.fetchone()[0]
        else:
            cursor.execute('SELECT COUNT(*) FROM report_groups')
            report_count = cursor.fetchone()[0]
        
        # Удаляем физические файлы
        deleted_files = []
        failed_deletes = []
        
        for file_row in all_files:
            file_path = file_row[0] if USE_POSTGRES else file_row[0]
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    deleted_files.append(os.path.basename(file_path))
                except Exception as e:
                    failed_deletes.append({'file': os.path.basename(file_path), 'error': str(e)})
        
        # Очищаем таблицы БД
        if USE_POSTGRES:
            cursor.execute('DELETE FROM report_groups')
            cursor.execute('DELETE FROM processed_files')
        else:
            cursor.execute('DELETE FROM report_groups')
            cursor.execute('DELETE FROM processed_files')
        
        conn.commit()
        return_db_connection(conn)
        
        audit_log(None, 'delete_all', f'Deleted all {report_count} reports and {len(deleted_files)} files', request.remote_addr)
        
        return jsonify({
            'success': True, 
            'message': f'Deleted {report_count} reports and {len(deleted_files)} files',
            'deleted_files_count': len(deleted_files),
            'failed_deletes': failed_deletes
        })
        
    except Exception as e:
        print(f"❌ Delete all error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-csv/<group_id>', methods=['GET'])
def export_csv(group_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = %s', (group_id,))
    else:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
    
    result = cursor.fetchone()
    return_db_connection(conn)
    
    if not result:
        return jsonify({'error': 'Report not found'}), 404
    
    group_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
    
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Test Name', 'Classname', 'Status', 'Time (s)', 'Message'])
    
    for report in group_data.get('reports', []):
        for suite in report.get('suites', []):
            for test in suite.get('test_cases', []):
                writer.writerow([
                    test.get('name', ''),
                    test.get('classname', ''),
                    test.get('status', ''),
                    test.get('time', 0),
                    test.get('message', '') or ''
                ])
    
    output.seek(0)
    return send_file(
        BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'vunit_report_{group_id}.csv'
    )

# ========== EMAIL FUNCTIONS ==========

def send_email_report(subject, html_content):
    if not config.get('email_enabled', False):
        return False
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = config.get('email_from')
        msg['To'] = config.get('email_to')
        
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
            server.starttls()
            server.login(config['smtp_user'], config['smtp_password'])
            server.send_message(msg)
        
        print(f"📧 Email sent: {subject}")
        return True
    except Exception as e:
        print(f"❌ Email error: {e}")
        return False

@app.route('/api/test-email', methods=['POST'])
def test_email():
    if not config.get('email_enabled'):
        return jsonify({'error': 'Email disabled'}), 400
    
    html = '<h1 style="color:#ff6b00">VUnit Test</h1><p>Email configuration works!</p>'
    success = send_email_report('[VUnit] Test Email', html)
    return jsonify({'success': success})

# ========== BACKUP ENDPOINTS ==========

@app.route('/api/backups/list', methods=['GET'])
def list_backups():
    backup_dir = os.path.join(ROOT_DIR, 'backups')
    backups = []
    
    if os.path.exists(backup_dir):
        pattern = os.path.join(backup_dir, 'vunit_*_backup_*')
        for filepath in sorted(glob.glob(pattern), reverse=True):
            stat = os.stat(filepath)
            backups.append({
                'name': os.path.basename(filepath),
                'size': stat.st_size,
                'size_mb': round(stat.st_size / 1024 / 1024, 2),
                'date': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
    
    return jsonify({'backups': backups})

@app.route('/api/backup', methods=['POST'])
def create_backup():
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if USE_POSTGRES:
            backup_path = os.path.join(backup_dir, f'vunit_pg_backup_{timestamp}.sql')
            return jsonify({'success': True, 'message': f'Backup created: {os.path.basename(backup_path)}'})
        else:
            backup_path = os.path.join(backup_dir, f'vunit_sqlite_backup_{timestamp}.db')
            shutil.copy2('vunit_reports.db', backup_path)
            return jsonify({'success': True, 'message': f'Backup created: {os.path.basename(backup_path)}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup/download/<filename>', methods=['GET'])
def download_backup(filename):
    backup_dir = os.path.join(ROOT_DIR, 'backups')
    filepath = os.path.join(backup_dir, filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'Backup not found'}), 404
    
    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route('/api/backup/restore/<filename>', methods=['POST'])
def restore_backup(filename):
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        backup_path = os.path.join(backup_dir, filename)
        
        if not os.path.exists(backup_path):
            return jsonify({'error': 'Backup not found'}), 404
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pre_restore = os.path.join(backup_dir, f'pre_restore_{timestamp}.db')
        
        if USE_POSTGRES:
            return jsonify({'error': 'Restore for PostgreSQL not implemented yet'}), 501
        else:
            shutil.copy2('vunit_reports.db', pre_restore)
            shutil.copy2(backup_path, 'vunit_reports.db')
        
        audit_log(None, 'restore', f'Restored from backup: {filename}', request.remote_addr)
        return jsonify({'success': True, 'message': f'Restored from {filename}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup/delete/<filename>', methods=['DELETE'])
def delete_backup(filename):
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        filepath = os.path.join(backup_dir, filename)
        
        if not os.path.exists(filepath):
            return jsonify({'error': 'Backup not found'}), 404
        
        os.remove(filepath)
        return jsonify({'success': True, 'message': f'Deleted {filename}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/backup/cleanup', methods=['POST'])
def cleanup_backups():
    try:
        backup_dir = os.path.join(ROOT_DIR, 'backups')
        pattern = os.path.join(backup_dir, 'vunit_*_backup_*')
        backups = sorted(glob.glob(pattern))
        
        deleted = []
        for old_backup in backups[:-10]:
            os.remove(old_backup)
            deleted.append(os.path.basename(old_backup))
        
        return jsonify({'success': True, 'deleted': len(deleted), 'files': deleted})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT COUNT(*) FROM report_groups')
        total_reports = cursor.fetchone()[0]
        cursor.execute('SELECT SUM(file_count) FROM report_groups')
        total_files = cursor.fetchone()[0] or 0
        cursor.execute('SELECT AVG(pass_rate) FROM report_groups')
        avg_pass_rate = cursor.fetchone()[0] or 0
        cursor.execute('SELECT COUNT(*) FROM report_groups WHERE created_date_str > NOW() - INTERVAL \'7 days\'')
        weekly_reports = cursor.fetchone()[0]
    else:
        cursor.execute('SELECT COUNT(*) FROM report_groups')
        total_reports = cursor.fetchone()[0]
        cursor.execute('SELECT SUM(file_count) FROM report_groups')
        total_files = cursor.fetchone()[0] or 0
        cursor.execute('SELECT AVG(pass_rate) FROM report_groups')
        avg_pass_rate = cursor.fetchone()[0] or 0
        cursor.execute('SELECT COUNT(*) FROM report_groups WHERE created_date_str > date("now", "-7 days")')
        weekly_reports = cursor.fetchone()[0]
    
    return_db_connection(conn)
    
    return jsonify({
        'total_reports': total_reports,
        'total_test_files': total_files,
        'avg_pass_rate': round(avg_pass_rate, 2),
        'weekly_reports': weekly_reports,
        'database': 'PostgreSQL' if USE_POSTGRES else 'SQLite'
    })

# ========== PDF EXPORT ==========

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib.enums import TA_CENTER
    from reportlab.graphics.shapes import Drawing, Rect
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("⚠️ reportlab not installed. PDF export disabled. Install with: pip install reportlab")

@app.route('/api/export-pdf/<group_id>', methods=['GET'])
def export_pdf(group_id):
    if not REPORTLAB_AVAILABLE:
        return jsonify({'error': 'PDF export not available. Install: pip install reportlab'}), 501
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            cursor.execute('SELECT group_data FROM report_groups WHERE group_id = %s', (group_id,))
        else:
            cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
        
        result = cursor.fetchone()
        return_db_connection(conn)
        
        if not result:
            return jsonify({'error': 'Report not found'}), 404
        
        group_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=15*mm, leftMargin=15*mm, topMargin=20*mm, bottomMargin=15*mm)
        styles = getSampleStyleSheet()
        story = []
        
        title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=28, textColor=colors.HexColor('#ff6b00'), alignment=TA_CENTER, spaceAfter=20)
        story.append(Paragraph("VUNIT", title_style))
        story.append(Paragraph("TEST INTELLIGENCE PLATFORM", styles['Normal']))
        story.append(Spacer(1, 20))
        
        line = Drawing(500, 1)
        line.add(Rect(0, 0, 500, 1, fillColor=colors.HexColor('#ff6b00'), strokeColor=None))
        story.append(line)
        story.append(Spacer(1, 20))
        
        summary = group_data.get('combined_summary', {})
        metrics_data = [
            ['PASSED', 'FAILED', 'ERRORS', 'TOTAL TESTS'],
            [
                str(summary.get('total_passed', 0)),
                str(summary.get('total_failures', 0)),
                str(summary.get('total_errors', 0)),
                str(summary.get('total_tests', 0))
            ]
        ]
        
        metrics_table = Table(metrics_data, colWidths=[80, 80, 80, 80])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
            ('FONTSIZE', (0, 1), (-1, 1), 14),
        ]))
        story.append(metrics_table)
        story.append(Spacer(1, 20))
        
        story.append(Paragraph(f"<b>Report ID:</b> {group_id}", styles['Normal']))
        story.append(Paragraph(f"<b>Created:</b> {group_data.get('created_at', '-')}", styles['Normal']))
        story.append(Paragraph(f"<b>Files:</b> {group_data.get('file_count', 0)}", styles['Normal']))
        story.append(Paragraph(f"<b>Pass Rate:</b> {summary.get('pass_rate', 0)}%", styles['Normal']))
        
        doc.build(story)
        buffer.seek(0)
        
        return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name=f'vunit_report_{group_id}.pdf')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ========== API KEY MANAGEMENT ==========

API_KEYS_FILE = os.path.join(ROOT_DIR, 'api_keys.json')

def load_api_keys():
    if os.path.exists(API_KEYS_FILE):
        with open(API_KEYS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_api_keys(keys):
    with open(API_KEYS_FILE, 'w', encoding='utf-8') as f:
        json.dump(keys, f, indent=2, ensure_ascii=False)

def validate_api_key(api_key):
    keys = load_api_keys()
    if api_key in keys:
        keys[api_key]['last_used'] = datetime.now().isoformat()
        save_api_keys(keys)
        return True
    return False

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if not api_key or not validate_api_key(api_key):
            return jsonify({'error': 'Invalid or missing API key'}), 401
        return f(*args, **kwargs)
    return decorated

# ========== API ENDPOINTS ==========

@app.route('/api/v1/health', methods=['GET'])
def api_health():
    return jsonify({
        'status': 'healthy',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat(),
        'database': 'postgresql' if USE_POSTGRES else 'sqlite'
    })

@app.route('/api/v1/keys', methods=['GET'])
def get_api_keys():
    keys = load_api_keys()
    key_list = []
    for key_id, key_data in keys.items():
        key_list.append({
            'id': key_data.get('id', key_id[:8]),
            'masked': key_data['masked'],
            'created': key_data['created'],
            'last_used': key_data.get('last_used', 'Never')
        })
    return jsonify({'keys': key_list})

@app.route('/api/v1/keys/generate', methods=['POST'])
def generate_api_key():
    try:
        keys = load_api_keys()
        new_key = secrets.token_urlsafe(32)
        key_id = f"key_{len(keys) + 1}_{int(datetime.now().timestamp())}"
        
        keys[new_key] = {
            'id': key_id,
            'masked': new_key[:8] + '...' + new_key[-8:],
            'created': datetime.now().isoformat(),
            'last_used': None
        }
        
        save_api_keys(keys)
        audit_log(None, 'api_key_generated', f'Generated API key: {key_id}', request.remote_addr)
        
        return jsonify({
            'success': True,
            'key': new_key,
            'masked': keys[new_key]['masked'],
            'message': 'Copy this key now. You won\'t see it again!'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/keys/<key_id>', methods=['DELETE'])
def revoke_api_key(key_id):
    try:
        keys = load_api_keys()
        found_key = None
        for key, data in keys.items():
            if data.get('id') == key_id:
                found_key = key
                break
        
        if not found_key:
            return jsonify({'error': 'Key not found'}), 404
        
        del keys[found_key]
        save_api_keys(keys)
        audit_log(None, 'api_key_revoked', f'Revoked API key: {key_id}', request.remote_addr)
        
        return jsonify({'success': True, 'message': 'API key revoked'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/keys/validate', methods=['POST'])
def validate_key():
    data = request.json
    api_key = data.get('api_key')
    if not api_key:
        return jsonify({'valid': False, 'error': 'No key provided'}), 400
    
    is_valid = validate_api_key(api_key)
    return jsonify({'valid': is_valid})

@app.route('/api/v1/reports', methods=['GET'])
@require_api_key
def api_get_reports():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT group_id, created_date, file_count, display_name, combined_summary FROM report_groups ORDER BY created_date DESC LIMIT %s OFFSET %s', (per_page, (page - 1) * per_page))
        cursor.execute('SELECT COUNT(*) FROM report_groups')
    else:
        cursor.execute('SELECT group_id, created_date, file_count, display_name, combined_summary FROM report_groups ORDER BY created_date DESC LIMIT ? OFFSET ?', (per_page, (page - 1) * per_page))
        cursor.execute('SELECT COUNT(*) FROM report_groups')
    
    rows = cursor.fetchall()
    total = cursor.fetchone()[0] if USE_POSTGRES else cursor.fetchone()[0]
    return_db_connection(conn)
    
    reports = []
    for row in rows:
        summary = json.loads(row[4]) if isinstance(row[4], str) else row[4]
        reports.append({
            'id': row[0],
            'created_at': row[1],
            'file_count': row[2],
            'name': row[3],
            'pass_rate': summary.get('pass_rate', 0),
            'total_tests': summary.get('total_tests', 0),
            'status': summary.get('status', 'unknown')
        })
    
    return jsonify({
        'reports': reports,
        'pagination': {'page': page, 'per_page': per_page, 'total': total, 'pages': (total + per_page - 1) // per_page}
    })

@app.route('/api/v1/reports/<group_id>', methods=['GET'])
@require_api_key
def api_get_report(group_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = %s', (group_id,))
    else:
        cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
    
    result = cursor.fetchone()
    return_db_connection(conn)
    
    if not result:
        return jsonify({'error': 'Report not found'}), 404
    
    group_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
    return jsonify(group_data)

@app.route('/api/v1/upload', methods=['POST'])
@require_api_key
def api_upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Empty filename'}), 400
        
        is_zip = file.filename.endswith('.zip')
        is_xml = file.filename.endswith('.xml')
        
        if not (is_zip or is_xml):
            return jsonify({'error': 'Only XML or ZIP files are supported'}), 400
        
        temp_path = f"temp_api_{datetime.now().timestamp()}_{file.filename}"
        file.save(temp_path)
        
        if is_zip:
            reports_data = []
            combined_summary = {'total_tests': 0, 'total_failures': 0, 'total_errors': 0, 'total_skipped': 0, 'total_passed': 0, 'total_time': 0.0}
            
            with zipfile.ZipFile(temp_path, 'r') as zf:
                for file_info in zf.filelist:
                    if file_info.filename.endswith('.xml'):
                        xml_content = zf.read(file_info.filename)
                        inner_temp = f"temp_inner_{datetime.now().timestamp()}_{os.path.basename(file_info.filename)}"
                        with open(inner_temp, 'wb') as f:
                            f.write(xml_content)
                        try:
                            data = parser.parse_xml(inner_temp)
                            if data:
                                data['filename'] = os.path.basename(file_info.filename)
                                reports_data.append(data)
                                combined_summary['total_tests'] += data['summary']['total_tests']
                                combined_summary['total_failures'] += data['summary']['total_failures']
                                combined_summary['total_errors'] += data['summary']['total_errors']
                                combined_summary['total_skipped'] += data['summary']['total_skipped']
                                combined_summary['total_passed'] += data['summary']['total_passed']
                                combined_summary['total_time'] += data['summary']['total_time']
                        finally:
                            os.remove(inner_temp)
            
            os.remove(temp_path)
            if not reports_data:
                return jsonify({'error': 'No valid XML files in ZIP'}), 400
            
            if combined_summary['total_tests'] > 0:
                combined_summary['pass_rate'] = round(combined_summary['total_passed'] / combined_summary['total_tests'] * 100, 2)
            
            group_id = f"api_{datetime.now().timestamp()}"
            display_name = f"API_ZIP_{os.path.basename(file.filename)}"
        else:
            data = parser.parse_xml(temp_path)
            os.remove(temp_path)
            if data is None:
                return jsonify({'error': 'Failed to parse XML'}), 400
            
            data['filename'] = file.filename
            reports_data = [data]
            combined_summary = data['summary']
            group_id = f"api_{datetime.now().timestamp()}"
            display_name = f"API_{file.filename}"
        
        pass_rate = combined_summary['pass_rate']
        if pass_rate == 100:
            combined_summary['status'] = 'perfect'
        elif pass_rate >= 80:
            combined_summary['status'] = 'good'
        elif pass_rate >= 50:
            combined_summary['status'] = 'warning'
        else:
            combined_summary['status'] = 'critical'
        
        group_data = {
            'reports': reports_data,
            'combined_summary': combined_summary,
            'created_at': datetime.now().isoformat(),
            'file_count': len(reports_data),
            'is_auto': False,
            'display_name': display_name
        }
        
        save_group_to_db(group_data, group_id, is_auto=False)
        audit_log(None, 'api_upload', f'API upload: {display_name}', request.remote_addr)
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'file_count': len(reports_data),
            'summary': combined_summary,
            'message': f'Successfully processed {len(reports_data)} file(s)'
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-postgres', methods=['POST'])
def test_postgres_connection():
    """Тестирует подключение к PostgreSQL"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port', 5432)
        database = data.get('database')
        user = data.get('user')
        password = data.get('password')
        
        if not all([host, database, user]):
            return jsonify({'success': False, 'error': 'Missing required fields'}), 400
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5
        )
        conn.close()
        
        return jsonify({'success': True, 'message': 'Connection successful'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/postgres/config', methods=['GET', 'POST'])
def postgres_config():
    """Получение/обновление настроек PostgreSQL"""
    if request.method == 'GET':
        return jsonify({
            'enabled': config.get('postgres_enabled', False),
            'host': config.get('pg_host', 'localhost'),
            'port': config.get('pg_port', 5432),
            'database': config.get('pg_database', 'vunit'),
            'user': config.get('pg_user', 'postgres'),
            'password': config.get('pg_password', '')
        })
    else:
        data = request.json
        config['postgres_enabled'] = data.get('enabled', False)
        config['pg_host'] = data.get('host', 'localhost')
        config['pg_port'] = data.get('port', 5432)
        config['pg_database'] = data.get('database', 'vunit')
        config['pg_user'] = data.get('user', 'postgres')
        config['pg_password'] = data.get('password', '')
        save_config(config)
        
        # Если включён PostgreSQL, обновляем DATABASE_URL
        if config['postgres_enabled']:
            os.environ['DATABASE_URL'] = f"postgresql://{config['pg_user']}:{config['pg_password']}@{config['pg_host']}:{config['pg_port']}/{config['pg_database']}"
        
        return jsonify({'success': True, 'config': config})
    
# ========== DATA EXPORT/IMPORT ==========

@app.route('/api/export', methods=['GET'])
def export_all_data():
    """Экспорт всех данных в JSON"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем все отчёты
        if USE_POSTGRES:
            cursor.execute('SELECT group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name FROM report_groups')
        else:
            cursor.execute('SELECT group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name FROM report_groups')
        
        reports = cursor.fetchall()
        
        # Получаем обработанные файлы
        if USE_POSTGRES:
            cursor.execute('SELECT file_path, file_hash, processed_date, group_id, file_size FROM processed_files')
        else:
            cursor.execute('SELECT file_path, file_hash, processed_date, group_id, file_size FROM processed_files')
        
        processed = cursor.fetchall()
        
        return_db_connection(conn)
        
        export_data = {
            'export_date': datetime.now().isoformat(),
            'version': '1.0',
            'reports': [
                {
                    'group_id': r[0],
                    'created_date': r[1],
                    'created_date_str': r[2],
                    'group_data': json.loads(r[3]) if isinstance(r[3], str) else r[3],
                    'file_count': r[4],
                    'combined_summary': json.loads(r[5]) if isinstance(r[5], str) else r[5],
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
                    'group_id': p[3],
                    'file_size': p[4]
                }
                for p in processed
            ]
        }
        
        return jsonify(export_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/import', methods=['POST'])
def import_all_data():
    """Импорт данных из JSON"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Проверяем структуру (может быть как полный экспорт, так и просто массив reports)
        reports = data.get('reports', [])
        if not reports and isinstance(data, list):
            reports = data
        
        if not reports:
            return jsonify({'error': 'Invalid data format: missing reports array'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        imported_count = 0
        for report in reports:
            # Извлекаем данные с проверкой
            group_id = report.get('group_id')
            if not group_id:
                continue
            
            created_date = report.get('created_date', datetime.now().isoformat())
            created_date_str = report.get('created_date_str', datetime.now().strftime('%Y-%m-%d'))
            group_data = report.get('group_data', {})
            file_count = report.get('file_count', 0)
            combined_summary = report.get('combined_summary', {})
            is_auto = report.get('is_auto', False)
            display_name = report.get('display_name', '')
            pass_rate = combined_summary.get('pass_rate', 0)
            total_tests = combined_summary.get('total_tests', 0)
            
            if USE_POSTGRES:
                cursor.execute('''
                    INSERT INTO report_groups 
                    (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name, pass_rate, total_tests)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (group_id) DO UPDATE SET
                        group_data = EXCLUDED.group_data,
                        combined_summary = EXCLUDED.combined_summary,
                        display_name = EXCLUDED.display_name,
                        pass_rate = EXCLUDED.pass_rate,
                        total_tests = EXCLUDED.total_tests
                ''', (
                    group_id, created_date, created_date_str,
                    json.dumps(group_data), file_count,
                    json.dumps(combined_summary), is_auto, display_name,
                    pass_rate, total_tests
                ))
            else:
                cursor.execute('''
                    INSERT OR REPLACE INTO report_groups 
                    (group_id, created_date, created_date_str, group_data, file_count, combined_summary, is_auto, display_name, pass_rate, total_tests)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    group_id, created_date, created_date_str,
                    json.dumps(group_data), file_count,
                    json.dumps(combined_summary), 1 if is_auto else 0, display_name,
                    pass_rate, total_tests
                ))
            imported_count += 1
        
        # Импортируем обработанные файлы, если есть
        processed_files = data.get('processed_files', [])
        for pf in processed_files:
            file_path = pf.get('file_path')
            if not file_path:
                continue
                
            file_hash = pf.get('file_hash', '')
            processed_date = pf.get('processed_date', datetime.now().isoformat())
            group_id = pf.get('group_id', '')
            file_size = pf.get('file_size', 0)
            
            if USE_POSTGRES:
                cursor.execute('''
                    INSERT INTO processed_files (file_path, file_hash, processed_date, group_id, file_size)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (file_path) DO UPDATE SET
                        processed_date = EXCLUDED.processed_date,
                        file_hash = EXCLUDED.file_hash
                ''', (file_path, file_hash, processed_date, group_id, file_size))
            else:
                cursor.execute('''
                    INSERT OR REPLACE INTO processed_files (file_path, file_hash, processed_date, group_id, file_size)
                    VALUES (?, ?, ?, ?, ?)
                ''', (file_path, file_hash, processed_date, group_id, file_size))
        
        conn.commit()
        return_db_connection(conn)
        
        return jsonify({'success': True, 'message': f'Imported {imported_count} reports'})
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/cleanup', methods=['DELETE'])
def cleanup_old_reports():
    """Удаляет отчеты старше 30 дней"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        if USE_POSTGRES:
            cursor.execute('DELETE FROM report_groups WHERE created_date_str < %s', (cutoff_date,))
        else:
            cursor.execute('DELETE FROM report_groups WHERE created_date_str < ?', (cutoff_date,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        return_db_connection(conn)
        
        return jsonify({'success': True, 'message': f'Deleted {deleted_count} old reports'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/reports/batch', methods=['POST'])
def get_reports_batch():
    """Получение данных нескольких отчётов за один запрос"""
    try:
        data = request.json
        group_ids = data.get('group_ids', [])
        
        if not group_ids:
            return jsonify({'error': 'No group_ids provided'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        results = {}
        for group_id in group_ids:
            if USE_POSTGRES:
                cursor.execute('SELECT group_data FROM report_groups WHERE group_id = %s', (group_id,))
            else:
                cursor.execute('SELECT group_data FROM report_groups WHERE group_id = ?', (group_id,))
            
            row = cursor.fetchone()
            if row:
                group_data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                suites = []
                for report in group_data.get('reports', []):
                    for suite in report.get('suites', []):
                        suite_name = suite.get('name')
                        if suite_name and suite_name != 'unknown':
                            suites.append(suite_name)
                
                # Убираем дубликаты
                unique_suites = list(set(suites))
                results[group_id] = {
                    'suites': unique_suites[:3],
                    'suites_count': len(unique_suites)
                }
            else:
                results[group_id] = {'suites': [], 'suites_count': 0}
        
        return_db_connection(conn)
        return jsonify(results)
        
    except Exception as e:
        print(f"Batch error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/debug/groups')
def debug_groups():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT group_id, display_name FROM report_groups')
    rows = cursor.fetchall()
    return_db_connection(conn)
    return jsonify([{'group_id': r[0], 'display_name': r[1]} for r in rows])

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 VUnit Enterprise Platform")
    print("=" * 60)
    print(f"Database: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
    print(f"URLs:")
    print(f"  Main:      http://localhost:5000")
    print(f"  Dashboard: http://localhost:5000/dashboard")
    print(f"  Settings:  http://localhost:5000/settings")
    print(f"  Health:    http://localhost:5000/health")
    print(f"  Metrics:   http://localhost:5000/metrics")
    print(f"Watch folder: {config['watched_folder']}")
    print(f"Monitoring: {'ACTIVE' if config['auto_monitoring'] else 'OFF'}")
    print("=" * 60)
    
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)