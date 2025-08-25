#!/usr/bin/env python3
"""
Performance test runner for the QuantStream Analytics Platform.

This script runs comprehensive performance benchmarks and generates
detailed performance reports for the ingestion pipeline.
"""

import asyncio
import json
import time
import sys
import os
from pathlib import Path
from datetime import datetime
import subprocess
import argparse

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))


class PerformanceTestRunner:
    """Orchestrate and run performance tests."""
    
    def __init__(self, output_dir: str = "performance_reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.results = {}
        
    def run_pytest_with_capture(self, test_file: str, markers: str = None) -> dict:
        """Run pytest and capture output."""
        cmd = [
            sys.executable, "-m", "pytest",
            test_file,
            "-v", "-s", "--tb=short",
            "--no-header", "--no-summary"
        ]
        
        if markers:
            cmd.extend(["-m", markers])
            
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "success": result.returncode == 0
            }
        except subprocess.TimeoutExpired:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": "Test timed out after 300 seconds",
                "success": False
            }
        except Exception as e:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": str(e),
                "success": False
            }
    
    def extract_benchmark_results(self, output: str) -> dict:
        """Extract benchmark results from test output."""
        results = {}
        current_test = None
        
        lines = output.split('\n')
        for line in lines:
            line = line.strip()
            
            # Detect test sections
            if "===" in line and "Benchmark" in line:
                current_test = line.replace("===", "").strip()
                results[current_test] = {}
            
            # Extract metrics
            elif current_test and ":" in line:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    
                    # Try to convert numeric values
                    try:
                        if "." in value and not value.endswith("s"):
                            value = float(value.replace("ms", "").replace("MB", "").replace("%", "").replace("msg/s", ""))
                        elif value.replace(".", "").replace(",", "").isdigit():
                            value = int(value.replace(",", ""))
                    except:
                        pass  # Keep as string
                    
                    results[current_test][key] = value
        
        return results
    
    def run_throughput_benchmarks(self) -> dict:
        """Run throughput benchmark tests."""
        print("Running throughput benchmarks...")
        
        test_file = Path(__file__).parent / "test_throughput_benchmarks.py"
        result = self.run_pytest_with_capture(str(test_file), "performance")
        
        if result["success"]:
            benchmark_results = self.extract_benchmark_results(result["stdout"])
            return {
                "status": "success",
                "benchmarks": benchmark_results,
                "raw_output": result["stdout"]
            }
        else:
            return {
                "status": "failed",
                "error": result["stderr"],
                "raw_output": result["stdout"]
            }
    
    def run_latency_benchmarks(self) -> dict:
        """Run latency benchmark tests."""
        print("Running latency benchmarks...")
        
        test_file = Path(__file__).parent / "test_latency_benchmarks.py"
        result = self.run_pytest_with_capture(str(test_file), "performance")
        
        if result["success"]:
            benchmark_results = self.extract_benchmark_results(result["stdout"])
            return {
                "status": "success", 
                "benchmarks": benchmark_results,
                "raw_output": result["stdout"]
            }
        else:
            return {
                "status": "failed",
                "error": result["stderr"],
                "raw_output": result["stdout"]
            }
    
    def run_all_benchmarks(self) -> dict:
        """Run all performance benchmarks."""
        print("Starting comprehensive performance benchmark suite...")
        start_time = time.time()
        
        results = {
            "test_run": {
                "timestamp": datetime.now().isoformat(),
                "platform": sys.platform,
                "python_version": sys.version,
            },
            "throughput": self.run_throughput_benchmarks(),
            "latency": self.run_latency_benchmarks(),
        }
        
        end_time = time.time()
        results["test_run"]["duration_seconds"] = end_time - start_time
        
        return results
    
    def generate_performance_report(self, results: dict) -> str:
        """Generate a comprehensive performance report."""
        report_lines = [
            "# QuantStream Analytics Platform - Performance Benchmark Report",
            f"Generated: {results['test_run']['timestamp']}",
            f"Platform: {results['test_run']['platform']}",
            f"Duration: {results['test_run']['duration_seconds']:.2f} seconds",
            "",
            "## Executive Summary",
            ""
        ]
        
        # Extract key metrics for summary
        summary_metrics = {}
        
        # Throughput metrics
        if results["throughput"]["status"] == "success":
            throughput_benchmarks = results["throughput"]["benchmarks"]
            for test_name, metrics in throughput_benchmarks.items():
                if "Avg Throughput" in metrics:
                    summary_metrics[f"{test_name} - Throughput"] = f"{metrics['Avg Throughput']:.0f} msg/s"
        
        # Latency metrics  
        if results["latency"]["status"] == "success":
            latency_benchmarks = results["latency"]["benchmarks"]
            for test_name, metrics in latency_benchmarks.items():
                if "P99 Latency" in metrics:
                    summary_metrics[f"{test_name} - P99 Latency"] = f"{metrics['P99 Latency']:.2f}ms"
        
        # Add summary metrics
        for metric_name, value in summary_metrics.items():
            report_lines.append(f"- **{metric_name}**: {value}")
        
        report_lines.extend([
            "",
            "## Performance Requirements Validation",
            ""
        ])
        
        # Check against requirements
        requirements_met = []
        requirements_failed = []
        
        # Check 500K+ events/second requirement
        max_throughput = 0
        if results["throughput"]["status"] == "success":
            for test_name, metrics in results["throughput"]["benchmarks"].items():
                if "Max Throughput" in metrics:
                    max_throughput = max(max_throughput, metrics["Max Throughput"])
        
        if max_throughput >= 500000:
            requirements_met.append(f"✅ Throughput requirement: {max_throughput:.0f} msg/s >= 500,000 msg/s")
        else:
            requirements_failed.append(f"❌ Throughput requirement: {max_throughput:.0f} msg/s < 500,000 msg/s")
        
        # Check sub-100ms latency requirement
        max_p99_latency = 0
        if results["latency"]["status"] == "success":
            for test_name, metrics in results["latency"]["benchmarks"].items():
                if "P99 Latency" in metrics:
                    max_p99_latency = max(max_p99_latency, metrics["P99 Latency"])
        
        if max_p99_latency < 100:
            requirements_met.append(f"✅ Latency requirement: {max_p99_latency:.2f}ms < 100ms (P99)")
        else:
            requirements_failed.append(f"❌ Latency requirement: {max_p99_latency:.2f}ms >= 100ms (P99)")
        
        # Add requirement results
        for req in requirements_met:
            report_lines.append(req)
        for req in requirements_failed:
            report_lines.append(req)
        
        # Detailed results
        report_lines.extend([
            "",
            "## Detailed Results",
            ""
        ])
        
        # Throughput results
        report_lines.extend([
            "### Throughput Benchmarks",
            ""
        ])
        
        if results["throughput"]["status"] == "success":
            for test_name, metrics in results["throughput"]["benchmarks"].items():
                report_lines.extend([
                    f"#### {test_name}",
                    ""
                ])
                for key, value in metrics.items():
                    report_lines.append(f"- {key}: {value}")
                report_lines.append("")
        else:
            report_lines.extend([
                "❌ Throughput benchmarks failed:",
                f"```",
                results["throughput"].get("error", "Unknown error"),
                "```",
                ""
            ])
        
        # Latency results
        report_lines.extend([
            "### Latency Benchmarks", 
            ""
        ])
        
        if results["latency"]["status"] == "success":
            for test_name, metrics in results["latency"]["benchmarks"].items():
                report_lines.extend([
                    f"#### {test_name}",
                    ""
                ])
                for key, value in metrics.items():
                    report_lines.append(f"- {key}: {value}")
                report_lines.append("")
        else:
            report_lines.extend([
                "❌ Latency benchmarks failed:",
                f"```",
                results["latency"].get("error", "Unknown error"),
                "```",
                ""
            ])
        
        return "\n".join(report_lines)
    
    def save_results(self, results: dict):
        """Save benchmark results to files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON results
        json_file = self.output_dir / f"performance_results_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save markdown report
        report = self.generate_performance_report(results)
        md_file = self.output_dir / f"performance_report_{timestamp}.md"
        with open(md_file, 'w') as f:
            f.write(report)
        
        print(f"Results saved to:")
        print(f"  JSON: {json_file}")
        print(f"  Report: {md_file}")
        
        return json_file, md_file


def main():
    """Main function to run performance tests."""
    parser = argparse.ArgumentParser(description="Run QuantStream performance benchmarks")
    parser.add_argument(
        "--output-dir", "-o",
        default="performance_reports",
        help="Output directory for reports (default: performance_reports)"
    )
    parser.add_argument(
        "--test-type", "-t",
        choices=["throughput", "latency", "all"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    
    args = parser.parse_args()
    
    runner = PerformanceTestRunner(args.output_dir)
    
    if args.test_type == "throughput":
        results = {"throughput": runner.run_throughput_benchmarks()}
    elif args.test_type == "latency":
        results = {"latency": runner.run_latency_benchmarks()}
    else:
        results = runner.run_all_benchmarks()
    
    # Save results
    json_file, md_file = runner.save_results(results)
    
    # Print summary
    print("\n" + "=" * 60)
    print("PERFORMANCE BENCHMARK SUMMARY")
    print("=" * 60)
    
    if "throughput" in results:
        print(f"Throughput Tests: {'✅ PASSED' if results['throughput']['status'] == 'success' else '❌ FAILED'}")
    
    if "latency" in results:
        print(f"Latency Tests: {'✅ PASSED' if results['latency']['status'] == 'success' else '❌ FAILED'}")
    
    print(f"\nDetailed report: {md_file}")
    
    # Return appropriate exit code
    all_passed = all(
        r["status"] == "success" 
        for r in results.values() 
        if isinstance(r, dict) and "status" in r
    )
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())