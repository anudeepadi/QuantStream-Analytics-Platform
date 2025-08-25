"""
ETL Pipeline Orchestrator for managing and coordinating streaming jobs.

This orchestrator manages the entire ETL pipeline including:
- Bronze, Silver, Gold layer streaming jobs
- Technical indicators calculation
- Anomaly detection
- Job monitoring and health checks
- Error handling and recovery
"""

import time
import threading
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import Enum

import yaml
import structlog
from pyspark.sql.streaming import StreamingQuery

from .streaming.bronze_layer import BronzeLayerJob
from .streaming.silver_layer import SilverLayerJob
from .streaming.gold_layer import GoldLayerJob
from .streaming.technical_indicators import TechnicalIndicatorsJob
from .streaming.anomaly_detection import AnomalyDetectionJob

logger = structlog.get_logger(__name__)


class JobStatus(Enum):
    """Job status enumeration."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    RESTARTING = "restarting"


@dataclass
class JobInfo:
    """Information about a streaming job."""
    name: str
    job_instance: Any
    query: Optional[StreamingQuery] = None
    status: JobStatus = JobStatus.STOPPED
    start_time: Optional[float] = None
    last_error: Optional[str] = None
    restart_count: int = 0
    config: Optional[Dict[str, Any]] = None


class ETLPipelineOrchestrator:
    """
    Orchestrator for managing the entire ETL streaming pipeline.
    
    Features:
    - Multi-job coordination
    - Dependency management
    - Health monitoring
    - Automatic restart on failure
    - Graceful shutdown
    - Performance monitoring
    - Configuration management
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the ETL pipeline orchestrator.
        
        Args:
            config_path: Path to the ETL configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = logger.bind(component="ETLOrchestrator")
        
        # Job management
        self.jobs: Dict[str, JobInfo] = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.shutdown_event = threading.Event()
        self.monitoring_thread: Optional[threading.Thread] = None
        
        # Job dependencies (jobs that must be running before starting dependent jobs)
        self.job_dependencies = {
            "silver-layer": ["bronze-layer"],
            "gold-layer": ["silver-layer"],
            "technical-indicators": ["gold-layer"],
            "anomaly-detection": ["silver-layer"]
        }
        
        # Initialize jobs
        self._initialize_jobs()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info("Configuration loaded successfully", config_path=config_path)
            return config
        except Exception as e:
            logger.error("Failed to load configuration", config_path=config_path, error=str(e))
            raise
            
    def _initialize_jobs(self) -> None:
        """Initialize all streaming jobs."""
        try:
            # Initialize Bronze layer job
            if self.config.get("bronze_layer", {}).get("enabled", True):
                bronze_job = BronzeLayerJob(self.config)
                self.jobs["bronze-layer"] = JobInfo(
                    name="bronze-layer",
                    job_instance=bronze_job,
                    config=self.config.get("bronze_layer", {})
                )
                
            # Initialize Silver layer job
            if self.config.get("silver_layer", {}).get("enabled", True):
                silver_job = SilverLayerJob(self.config)
                self.jobs["silver-layer"] = JobInfo(
                    name="silver-layer",
                    job_instance=silver_job,
                    config=self.config.get("silver_layer", {})
                )
                
            # Initialize Gold layer job
            if self.config.get("gold_layer", {}).get("enabled", True):
                gold_job = GoldLayerJob(self.config)
                self.jobs["gold-layer"] = JobInfo(
                    name="gold-layer",
                    job_instance=gold_job,
                    config=self.config.get("gold_layer", {})
                )
                
            # Initialize Technical Indicators job
            if self.config.get("technical_indicators", {}).get("enabled", True):
                indicators_job = TechnicalIndicatorsJob(self.config)
                self.jobs["technical-indicators"] = JobInfo(
                    name="technical-indicators",
                    job_instance=indicators_job,
                    config=self.config.get("technical_indicators", {})
                )
                
            # Initialize Anomaly Detection job
            if self.config.get("anomaly_detection", {}).get("enabled", True):
                anomaly_job = AnomalyDetectionJob(self.config)
                self.jobs["anomaly-detection"] = JobInfo(
                    name="anomaly-detection",
                    job_instance=anomaly_job,
                    config=self.config.get("anomaly_detection", {})
                )
                
            self.logger.info("Jobs initialized", job_count=len(self.jobs))
            
        except Exception as e:
            self.logger.error("Failed to initialize jobs", error=str(e))
            raise
            
    def start_pipeline(self) -> None:
        """Start the entire ETL pipeline."""
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Start jobs in dependency order
            start_order = self._get_job_start_order()
            
            for job_name in start_order:
                if job_name in self.jobs:
                    self._start_job(job_name)
                    
            # Start monitoring thread
            self._start_monitoring()
            
            self.logger.info("ETL pipeline started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start ETL pipeline", error=str(e))
            self.stop_pipeline()
            raise
            
    def stop_pipeline(self) -> None:
        """Stop the entire ETL pipeline gracefully."""
        try:
            self.logger.info("Stopping ETL pipeline")
            
            # Signal shutdown
            self.shutdown_event.set()
            
            # Stop monitoring
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=30)
                
            # Stop jobs in reverse dependency order
            stop_order = list(reversed(self._get_job_start_order()))
            
            for job_name in stop_order:
                if job_name in self.jobs:
                    self._stop_job(job_name)
                    
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            self.logger.info("ETL pipeline stopped")
            
        except Exception as e:
            self.logger.error("Error stopping ETL pipeline", error=str(e))
            
    def _get_job_start_order(self) -> List[str]:
        """Get the order in which jobs should be started based on dependencies."""
        ordered_jobs = []
        remaining_jobs = set(self.jobs.keys())
        
        while remaining_jobs:
            # Find jobs with no unsatisfied dependencies
            ready_jobs = []
            for job_name in remaining_jobs:
                dependencies = self.job_dependencies.get(job_name, [])
                if all(dep in ordered_jobs for dep in dependencies):
                    ready_jobs.append(job_name)
                    
            if not ready_jobs:
                # No jobs ready - possible circular dependency
                self.logger.warning("Possible circular dependency detected", 
                                  remaining_jobs=list(remaining_jobs))
                ready_jobs = list(remaining_jobs)  # Start remaining jobs anyway
                
            for job_name in ready_jobs:
                ordered_jobs.append(job_name)
                remaining_jobs.remove(job_name)
                
        return ordered_jobs
        
    def _start_job(self, job_name: str) -> bool:
        """Start a specific job."""
        try:
            job_info = self.jobs[job_name]
            
            if job_info.status in [JobStatus.RUNNING, JobStatus.STARTING]:
                self.logger.warning("Job already running or starting", job_name=job_name)
                return False
                
            self.logger.info("Starting job", job_name=job_name)
            job_info.status = JobStatus.STARTING
            job_info.start_time = time.time()
            
            # Wait for dependencies
            if not self._wait_for_dependencies(job_name):
                job_info.status = JobStatus.FAILED
                job_info.last_error = "Dependencies not satisfied"
                return False
                
            # Start the streaming query
            query = job_info.job_instance.run()
            job_info.query = query
            job_info.status = JobStatus.RUNNING
            
            self.logger.info("Job started successfully", 
                           job_name=job_name, 
                           query_id=query.id)
            return True
            
        except Exception as e:
            self.logger.error("Failed to start job", job_name=job_name, error=str(e))
            job_info.status = JobStatus.FAILED
            job_info.last_error = str(e)
            return False
            
    def _stop_job(self, job_name: str, timeout: int = 60) -> bool:
        """Stop a specific job."""
        try:
            job_info = self.jobs[job_name]
            
            if job_info.status in [JobStatus.STOPPED, JobStatus.STOPPING]:
                self.logger.warning("Job already stopped or stopping", job_name=job_name)
                return True
                
            self.logger.info("Stopping job", job_name=job_name)
            job_info.status = JobStatus.STOPPING
            
            if job_info.query and job_info.query.isActive:
                job_info.job_instance.stop(job_info.query, timeout)
                
            job_info.status = JobStatus.STOPPED
            job_info.query = None
            
            self.logger.info("Job stopped successfully", job_name=job_name)
            return True
            
        except Exception as e:
            self.logger.error("Failed to stop job", job_name=job_name, error=str(e))
            job_info.status = JobStatus.FAILED
            job_info.last_error = str(e)
            return False
            
    def _wait_for_dependencies(self, job_name: str, timeout: int = 300) -> bool:
        """Wait for job dependencies to be satisfied."""
        dependencies = self.job_dependencies.get(job_name, [])
        if not dependencies:
            return True
            
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_ready = True
            for dep_name in dependencies:
                if dep_name not in self.jobs:
                    all_ready = False
                    break
                    
                dep_job = self.jobs[dep_name]
                if dep_job.status != JobStatus.RUNNING:
                    all_ready = False
                    break
                    
            if all_ready:
                return True
                
            time.sleep(5)  # Wait 5 seconds before checking again
            
        self.logger.error("Timeout waiting for dependencies", 
                         job_name=job_name, 
                         dependencies=dependencies)
        return False
        
    def _start_monitoring(self) -> None:
        """Start the monitoring thread."""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            return
            
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name="ETL-Monitor",
            daemon=True
        )
        self.monitoring_thread.start()
        self.logger.info("Monitoring thread started")
        
    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        monitoring_interval = self.config.get("monitoring", {}).get("interval_seconds", 30)
        
        while not self.shutdown_event.wait(monitoring_interval):
            try:
                self._check_job_health()
                self._collect_metrics()
            except Exception as e:
                self.logger.error("Error in monitoring loop", error=str(e))
                
    def _check_job_health(self) -> None:
        """Check health of all running jobs."""
        for job_name, job_info in self.jobs.items():
            try:
                if job_info.status == JobStatus.RUNNING and job_info.query:
                    if not job_info.query.isActive:
                        # Job has failed
                        exception_info = job_info.query.exception()
                        error_msg = str(exception_info) if exception_info else "Unknown error"
                        
                        self.logger.error("Job failed", 
                                        job_name=job_name, 
                                        error=error_msg)
                        
                        job_info.status = JobStatus.FAILED
                        job_info.last_error = error_msg
                        
                        # Attempt restart if configured
                        if self._should_restart_job(job_info):
                            self._restart_job(job_name)
                            
            except Exception as e:
                self.logger.error("Error checking job health", 
                                job_name=job_name, 
                                error=str(e))
                
    def _should_restart_job(self, job_info: JobInfo) -> bool:
        """Determine if a failed job should be restarted."""
        max_restarts = self.config.get("error_handling", {}).get("max_restart_attempts", 3)
        auto_restart = self.config.get("error_handling", {}).get("auto_restart_failed_queries", True)
        
        return (auto_restart and 
                job_info.restart_count < max_restarts and
                job_info.status == JobStatus.FAILED)
                
    def _restart_job(self, job_name: str) -> None:
        """Restart a failed job."""
        try:
            job_info = self.jobs[job_name]
            job_info.restart_count += 1
            job_info.status = JobStatus.RESTARTING
            
            self.logger.info("Restarting job", 
                           job_name=job_name, 
                           restart_count=job_info.restart_count)
            
            # Wait before restart
            restart_delay = self.config.get("error_handling", {}).get("restart_delay_seconds", 60)
            time.sleep(restart_delay)
            
            # Restart the job
            if self._start_job(job_name):
                self.logger.info("Job restarted successfully", job_name=job_name)
            else:
                self.logger.error("Failed to restart job", job_name=job_name)
                
        except Exception as e:
            self.logger.error("Error restarting job", job_name=job_name, error=str(e))
            
    def _collect_metrics(self) -> None:
        """Collect metrics from all running jobs."""
        try:
            pipeline_metrics = {
                "timestamp": time.time(),
                "pipeline_status": self.get_pipeline_status(),
                "jobs": {}
            }
            
            for job_name, job_info in self.jobs.items():
                if job_info.query and job_info.status == JobStatus.RUNNING:
                    job_metrics = job_info.job_instance.get_query_status(job_info.query)
                    pipeline_metrics["jobs"][job_name] = job_metrics
                    
            # Log metrics (in production, would send to metrics system)
            self.logger.info("Pipeline metrics collected", metrics=pipeline_metrics)
            
        except Exception as e:
            self.logger.error("Error collecting metrics", error=str(e))
            
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get overall pipeline status."""
        status_counts = {}
        for job_info in self.jobs.values():
            status = job_info.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
            
        total_jobs = len(self.jobs)
        running_jobs = status_counts.get("running", 0)
        failed_jobs = status_counts.get("failed", 0)
        
        if failed_jobs > 0:
            overall_status = "degraded"
        elif running_jobs == total_jobs:
            overall_status = "healthy"
        elif running_jobs > 0:
            overall_status = "partial"
        else:
            overall_status = "stopped"
            
        return {
            "overall_status": overall_status,
            "total_jobs": total_jobs,
            "status_counts": status_counts,
            "job_details": {
                name: {
                    "status": info.status.value,
                    "start_time": info.start_time,
                    "restart_count": info.restart_count,
                    "last_error": info.last_error
                }
                for name, info in self.jobs.items()
            }
        }
        
    def get_job_status(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        if job_name not in self.jobs:
            return None
            
        job_info = self.jobs[job_name]
        status = {
            "name": job_name,
            "status": job_info.status.value,
            "start_time": job_info.start_time,
            "restart_count": job_info.restart_count,
            "last_error": job_info.last_error
        }
        
        if job_info.query and job_info.status == JobStatus.RUNNING:
            query_status = job_info.job_instance.get_query_status(job_info.query)
            status["query_details"] = query_status
            
        return status
        
    def restart_job_manual(self, job_name: str) -> bool:
        """Manually restart a specific job."""
        if job_name not in self.jobs:
            self.logger.error("Job not found", job_name=job_name)
            return False
            
        try:
            # Stop the job first
            self._stop_job(job_name)
            
            # Wait a moment
            time.sleep(5)
            
            # Start the job
            return self._start_job(job_name)
            
        except Exception as e:
            self.logger.error("Manual job restart failed", job_name=job_name, error=str(e))
            return False
            
    def update_job_config(self, job_name: str, new_config: Dict[str, Any]) -> bool:
        """Update configuration for a specific job (requires restart)."""
        if job_name not in self.jobs:
            self.logger.error("Job not found", job_name=job_name)
            return False
            
        try:
            job_info = self.jobs[job_name]
            
            # Stop the job if running
            if job_info.status == JobStatus.RUNNING:
                self._stop_job(job_name)
                
            # Update configuration
            job_info.config.update(new_config)
            
            # Recreate job instance with new config
            # This is simplified - in practice, you'd need to handle config merging properly
            self.logger.info("Job configuration updated", job_name=job_name)
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to update job config", job_name=job_name, error=str(e))
            return False