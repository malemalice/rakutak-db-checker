import schedule
import time
import threading
from typing import Callable, Dict, Any
import logging
from server.health import update_execution_status

logger = logging.getLogger(__name__)

class ValidationScheduler:
    def __init__(self, interval_minutes: int, validation_func: Callable):
        """
        Initialize the validation scheduler.
        
        Args:
            interval_minutes (int): Interval between validations in minutes
            validation_func (Callable): Function to execute for validation
        """
        self.interval_minutes = interval_minutes
        self.validation_func = validation_func
        self.is_running = False
        self.thread = None

    def _run_validation(self) -> None:
        """
        Execute the validation function and update status.
        """
        try:
            update_execution_status("running")
            result = self.validation_func()
            update_execution_status("completed", result)
            logger.info("Validation completed successfully")
        except Exception as e:
            error_msg = f"Validation failed: {str(e)}"
            logger.error(error_msg)
            update_execution_status("failed", {"error": error_msg})

    def _scheduler_loop(self) -> None:
        """
        Main scheduler loop that runs the validation at specified intervals.
        """
        schedule.every(self.interval_minutes).minutes.do(self._run_validation)
        
        # Run immediately on start
        self._run_validation()
        
        while self.is_running:
            schedule.run_pending()
            time.sleep(1)

    def start(self) -> None:
        """
        Start the scheduler in a separate thread.
        """
        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        self.is_running = True
        self.thread = threading.Thread(target=self._scheduler_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Scheduler started with {self.interval_minutes} minute interval")

    def stop(self) -> None:
        """
        Stop the scheduler.
        """
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return

        self.is_running = False
        if self.thread:
            self.thread.join()
        logger.info("Scheduler stopped") 