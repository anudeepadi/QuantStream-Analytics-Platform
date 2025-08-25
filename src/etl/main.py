#!/usr/bin/env python3
"""
Main entry point for the QuantStream ETL pipeline.

This script provides the main interface for running the ETL pipeline
with different modes and configurations.
"""

import sys
import signal
import argparse
import logging
from pathlib import Path
from typing import Optional

import structlog

from .pipeline_orchestrator import ETLPipelineOrchestrator


def setup_logging(log_level: str = "INFO") -> None:
    """Setup structured logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s"
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def signal_handler(orchestrator: ETLPipelineOrchestrator):
    """Create signal handler for graceful shutdown."""
    def handler(signum, frame):
        logger = structlog.get_logger(__name__)
        logger.info("Received shutdown signal", signal=signum)
        orchestrator.stop_pipeline()
        sys.exit(0)
    return handler


def run_pipeline(config_path: str, mode: str = "full") -> None:
    """
    Run the ETL pipeline.
    
    Args:
        config_path: Path to configuration file
        mode: Pipeline mode (full, bronze-only, silver-only, etc.)
    """
    logger = structlog.get_logger(__name__)
    
    try:
        logger.info("Starting QuantStream ETL Pipeline", 
                   config_path=config_path, 
                   mode=mode)
        
        # Initialize orchestrator
        orchestrator = ETLPipelineOrchestrator(config_path)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler(orchestrator))
        signal.signal(signal.SIGTERM, signal_handler(orchestrator))
        
        # Start pipeline based on mode
        if mode == "full":
            orchestrator.start_pipeline()
            
        elif mode == "bronze-only":
            # Start only Bronze layer
            if "bronze-layer" in orchestrator.jobs:
                orchestrator._start_job("bronze-layer")
                orchestrator._start_monitoring()
                
        elif mode == "silver-only":
            # Start Bronze and Silver layers
            if "bronze-layer" in orchestrator.jobs:
                orchestrator._start_job("bronze-layer")
            if "silver-layer" in orchestrator.jobs:
                orchestrator._start_job("silver-layer")
            orchestrator._start_monitoring()
            
        elif mode == "gold-only":
            # Start Bronze, Silver, and Gold layers
            for job_name in ["bronze-layer", "silver-layer", "gold-layer"]:
                if job_name in orchestrator.jobs:
                    orchestrator._start_job(job_name)
            orchestrator._start_monitoring()
            
        else:
            raise ValueError(f"Unknown mode: {mode}")
            
        logger.info("ETL Pipeline started successfully")
        
        # Keep the main thread alive
        import time
        while True:
            time.sleep(60)
            status = orchestrator.get_pipeline_status()
            logger.info("Pipeline status check", status=status["overall_status"])
            
            # Check if any critical jobs failed
            if status["overall_status"] == "degraded":
                failed_jobs = [
                    name for name, details in status["job_details"].items() 
                    if details["status"] == "failed"
                ]
                logger.warning("Pipeline degraded - jobs failed", failed_jobs=failed_jobs)
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        orchestrator.stop_pipeline()
        
    except Exception as e:
        logger.error("Pipeline execution failed", error=str(e))
        if 'orchestrator' in locals():
            orchestrator.stop_pipeline()
        raise


def validate_config(config_path: str) -> bool:
    """
    Validate configuration file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        True if configuration is valid
    """
    logger = structlog.get_logger(__name__)
    
    try:
        import yaml
        
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            
        # Basic validation
        required_sections = ["kafka", "bronze_layer", "silver_layer", "gold_layer"]
        missing_sections = [section for section in required_sections if section not in config]
        
        if missing_sections:
            logger.error("Missing required configuration sections", 
                        missing_sections=missing_sections)
            return False
            
        # Validate Kafka configuration
        kafka_config = config.get("kafka", {})
        if not kafka_config.get("bootstrap_servers"):
            logger.error("Kafka bootstrap_servers not configured")
            return False
            
        # Validate layer configurations
        for layer in ["bronze_layer", "silver_layer", "gold_layer"]:
            layer_config = config.get(layer, {})
            if not layer_config.get("output_path"):
                logger.error(f"{layer} output_path not configured")
                return False
                
        logger.info("Configuration validation passed")
        return True
        
    except Exception as e:
        logger.error("Configuration validation failed", error=str(e))
        return False


def check_status(config_path: str) -> None:
    """
    Check status of running pipeline.
    
    Args:
        config_path: Path to configuration file
    """
    logger = structlog.get_logger(__name__)
    
    try:
        # This would connect to a running pipeline instance
        # For now, just print a message
        logger.info("Pipeline status check not implemented yet")
        print("Pipeline status check functionality coming soon...")
        
    except Exception as e:
        logger.error("Status check failed", error=str(e))


def stop_pipeline(config_path: str) -> None:
    """
    Stop running pipeline gracefully.
    
    Args:
        config_path: Path to configuration file
    """
    logger = structlog.get_logger(__name__)
    
    try:
        # This would connect to a running pipeline instance and stop it
        # For now, just print a message
        logger.info("Pipeline stop command not implemented yet")
        print("Pipeline stop functionality coming soon...")
        
    except Exception as e:
        logger.error("Pipeline stop failed", error=str(e))


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="QuantStream ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python -m src.etl.main run --config config/etl/streaming_config.yaml

  # Run only Bronze layer
  python -m src.etl.main run --config config/etl/streaming_config.yaml --mode bronze-only

  # Validate configuration
  python -m src.etl.main validate --config config/etl/streaming_config.yaml

  # Check pipeline status
  python -m src.etl.main status --config config/etl/streaming_config.yaml
        """
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run the ETL pipeline")
    run_parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file"
    )
    run_parser.add_argument(
        "--mode",
        choices=["full", "bronze-only", "silver-only", "gold-only"],
        default="full",
        help="Pipeline execution mode"
    )
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate configuration")
    validate_parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file"
    )
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check pipeline status")
    status_parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file"
    )
    
    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop running pipeline")
    stop_parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if not args.command:
        parser.print_help()
        return
        
    # Validate config file exists
    if hasattr(args, 'config'):
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"Error: Configuration file not found: {config_path}")
            sys.exit(1)
            
    # Execute command
    try:
        if args.command == "run":
            run_pipeline(args.config, args.mode)
            
        elif args.command == "validate":
            if validate_config(args.config):
                print("Configuration is valid ✓")
            else:
                print("Configuration validation failed ✗")
                sys.exit(1)
                
        elif args.command == "status":
            check_status(args.config)
            
        elif args.command == "stop":
            stop_pipeline(args.config)
            
    except Exception as e:
        logger = structlog.get_logger(__name__)
        logger.error("Command execution failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()