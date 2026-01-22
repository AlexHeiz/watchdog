import os
import sys
import sqlite3
import logging
import time
import re
import queue
from datetime import datetime
from typing import Optional

import uvicorn
from fastapi import FastAPI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler, RegexMatchingEventHandler
import threading
import environ


env = environ.Env()
environ.Env.read_env()
WATCH_PATH = env.str("WATCH_PATH")
print(WATCH_PATH)
os.makedirs(WATCH_PATH, exist_ok=True)
DB_PATH = "files.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)-4s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("watch_log.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
file_queue = queue.Queue()


class ObserverWard(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            self.try_size(event.src_path)
            logger.info(f"event=on_created")

    def on_deleted(self, event):
        print(event)

    def on_moved(self, event):
        if not event.is_directory:
            self.try_size(event.dest_path)
            logger.info(f"event=on_moved")

    def try_size(self, path: str):
        if not re.match(r'^test_\d+\.txt$', os.path.basename(path)):
            return
        try:
            start_size = os.path.getsize(path)
            time.sleep(2)
            end_size = os.path.getsize(path)
            if start_size != end_size:
                logger.info(f"event=stabilizing file= {os.path.basename(path)} "
                            f"start_size= {start_size} "
                            f"end_size= {end_size} - skipping")
                return
            logger.info(f"event=processing  size={end_size} - stabilized")
        except OSError as e:
            logger.warning(f"event=failed error=stabilize failed {e}")
            return
        file_queue.put(path)
        logger.info(f"event=queued file={os.path.basename(path)} "
                    f"queue_size={file_queue.qsize()}")


def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT,
            status TEXT,
            lines INTEGER,
            word_count INTEGER,
            error TEXT NULL,
            created_at DATETIME,
            processed_at TEXT NULL
        )
    ''')
    conn.commit()
    conn.close()


init_database()


def inserting(path: str, status: str,
              lines: Optional[int] = None, word_count: Optional[int] = None,
              error: Optional[str] = None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    try:
        cursor.execute('''
            INSERT INTO files (path, status, lines, word_count, error, created_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (path, status, lines, word_count, error, now, now if status == "OK" else None))
        conn.commit()
    except sqlite3.IntegrityError:
        cursor.execute('''
            UPDATE files
            SET status = ?, lines = ?, word_count = ?, error = ?, processed_at = ?
            WHERE path = ?
        ''', (status, lines, word_count, error, now if status == "OK" else None, path))
        conn.commit()
    finally:
        conn.close()


def last_files(limit: int = 20):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT path, status, lines, word_count, error, created_at, processed_at
        FROM files ORDER BY id DESC LIMIT ?
    ''', (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "path": r[0],
            "status": r[1],
            "lines": r[2],
            "word_count": r[3],
            "error": r[4],
            "created_at": r[5],
            "processed_at": r[6]
        }
        for r in rows
    ]


def stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM files')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM files WHERE status = "OK"')
    ok = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM files WHERE status = "FAILED"')
    failed = cursor.fetchone()[0]
    conn.close()
    return {"total": total, "ok": ok, "failed": failed}


def worker():
    while True:
        try:
            path = file_queue.get(timeout=5)
            logger.info(f"event=processing path={path}")
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    lines = len(f.readlines())
                    word_count = len(f.read().split())
                status = "OK"
                inserting(path, status, lines=lines, word_count=word_count)
                logger.info(f"event=processed path={path} status={status} lines={lines} word_count={word_count}")
            except Exception as e:
                status = "FAILED"
                inserting(path, status, error=str(e))
                logger.error(f"event=failed path={path} status={status} error={str(e)}")
                file_queue.task_done()
                continue
            file_queue.task_done()
        except queue.Empty:
            logger.debug("event=worker_idle queue_empty")
            continue
        except Exception as e:
            logger.error(f"event=worker_crashed error={str(e)}")
            continue


worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()
observer = Observer()
event_handler = ObserverWard()
observer.schedule(event_handler, WATCH_PATH, recursive=False)
observer.start()
logger.info(f"event = watchdog_started path={WATCH_PATH}")
app = FastAPI(title="File Monitor Service")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stats")
def set_stats():
    stats_data = stats()
    return {
        **stats_data,
        "queued": file_queue.qsize()
    }


@app.get("/files")
def files():
    return last_files(20)


if __name__ == "__main__":
    logger.info("event=start status=service_started")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
    except KeyboardInterrupt:
        logger.info("event=shutting_down")
        observer.stop()
        file_queue.put(None)
        worker_thread.join()
        observer.join()
        logger.info("event=stop status=service_stopped")
