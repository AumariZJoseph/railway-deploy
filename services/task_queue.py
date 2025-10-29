# services/task_queue.py
import asyncio
import threading
from queue import Queue, Empty
import time
import uuid
from typing import Dict, Callable, Any
import logging

logger = logging.getLogger(__name__)

class BackgroundTaskQueue:
    def __init__(self, max_workers: int = 2):
        self.task_queue = Queue()
        self.results: Dict[str, Dict] = {}
        self.max_workers = max_workers
        self.workers = []
        self.stop_signal = threading.Event()
        
        # Start worker threads
        for i in range(max_workers):
            worker = threading.Thread(target=self._worker, daemon=True, name=f"Worker-{i+1}")
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"BackgroundTaskQueue started with {max_workers} workers")

    def _worker(self):
        while not self.stop_signal.is_set():
            try:
                # Get task with timeout to allow checking stop_signal
                task_id, func, args, kwargs = self.task_queue.get(timeout=1.0)
                try:
                    logger.info(f"Worker processing task {task_id}")
                    result = func(*args, **kwargs)
                    self.results[task_id] = {
                        "status": "completed",
                        "result": result,
                        "completed_at": time.time()
                    }
                    logger.info(f"Task {task_id} completed successfully")
                except Exception as e:
                    self.results[task_id] = {
                        "status": "failed", 
                        "error": str(e),
                        "completed_at": time.time()
                    }
                    logger.error(f"Task {task_id} failed: {str(e)}")
                finally:
                    self.task_queue.task_done()
                    
            except Empty:
                continue  # Timeout, check stop_signal again

    def submit_task(self, func: Callable, *args, **kwargs) -> str:
        """Submit a task to the background queue and return task ID"""
        task_id = str(uuid.uuid4())
        self.task_queue.put((task_id, func, args, kwargs))
        self.results[task_id] = {"status": "queued", "submitted_at": time.time()}
        logger.info(f"Task {task_id} submitted to queue")
        return task_id

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get the status of a task"""
        return self.results.get(task_id, {"status": "not_found"})

    def wait_for_completion(self, task_id: str, timeout: float = None) -> bool:
        """Wait for a specific task to complete"""
        start_time = time.time()
        while True:
            status = self.get_task_status(task_id)
            if status["status"] in ["completed", "failed"]:
                return True
            
            if timeout and (time.time() - start_time) > timeout:
                return False
            
            time.sleep(0.1)  # Small delay to prevent busy waiting

    def shutdown(self):
        """Gracefully shutdown the task queue"""
        logger.info("Shutting down BackgroundTaskQueue...")
        self.stop_signal.set()
        self.task_queue.join()  # Wait for all tasks to complete
        logger.info("BackgroundTaskQueue shutdown complete")

# Global instance
background_queue = BackgroundTaskQueue(max_workers=2)  # 2 concurrent file processings