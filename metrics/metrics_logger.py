import psutil
import time
import json
from datetime import datetime
from collections import defaultdict

class MetricsLogger:
    def __init__(self, service_name, log_interval_seconds=5):
        self.service_name = service_name
        self.log_file_path = f"/app/metrics/{service_name}.log"
        self.error_log_path = f"/app/metrics/{service_name}.error.log"
        self.log_interval_seconds = log_interval_seconds

        self._latency_sum = 0.0
        self._message_count = 0
        self._error_count = 0

        self._latency_sum_ep = defaultdict(float)
        self._message_count_ep = defaultdict(int)
        self._error_count_ep = defaultdict(int)
        self._status_code_ep = defaultdict(lambda: defaultdict(int))
        self._status_code_counts = {}

        self._cache_hits = 0
        self._cache_misses = 0

        self._last_log_time = time.time()

    def record_success(self, latency_seconds):
        self._latency_sum += latency_seconds
        self._message_count += 1
        self._log_metrics()

    def record_error(self, error_message):
        self._error_count += 1
        if self.error_log_path:
            with open(self.error_log_path, "a") as f:
                timestamp = datetime.utcnow().isoformat() + "Z"
                f.write(f"[{timestamp}] ERROR: {error_message}\n")
        self._log_metrics()

    def record_cache_hit(self):
        self._cache_hits += 1
        self._log_metrics()

    def record_cache_miss(self):
        self._cache_misses += 1
        self._log_metrics()

    def record_request(self, endpoint, status_code, latency_seconds):
        self._message_count_ep[endpoint] += 1
        self._latency_sum_ep[endpoint] += latency_seconds
        self._status_code_ep[endpoint][status_code] = self._status_code_ep[endpoint].get(status_code,0) + 1
        self._log_detailed_metrics()

    def record_error_ep(self, endpoint, status_code, error_message):
        self._error_count_ep[endpoint] += 1
        if self.error_log_path:
            with open(self.error_log_path, "a") as f:
                timestamp = datetime.utcnow().isoformat() + "Z"
                f.write(f"[{timestamp}] ERROR ({endpoint}): {error_message}\n")
        self._status_code_ep[endpoint][status_code] = self._status_code_ep[endpoint].get(status_code,0) + 1
        self._log_detailed_metrics()

    def _log_metrics(self):
        now = time.time()
        if now - self._last_log_time >= self.log_interval_seconds:
            self._log_aggregated_metrics()
            self._last_log_time = now
            self._latency_sum = 0.0
            self._message_count = 0
            self._error_count = 0
            self._cache_hits = 0
            self._cache_misses = 0

    def _log_aggregated_metrics(self):
        if self._message_count == 0 and self._error_count == 0:
            return

        avg_latency_ms = (self._latency_sum / self._message_count) * 1000 if self._message_count else 0
        throughput = self._message_count / self.log_interval_seconds
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_percent = psutil.virtual_memory().percent

        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "latency_ms": avg_latency_ms,
            "throughput": throughput,
            "cpu": cpu_percent,
            "mem_usage": memory_percent
        }

        total_cache = self._cache_hits + self._cache_misses

        if total_cache > 0:
            cache_hit_ratio = (self._cache_hits / total_cache)
            log_entry['cache_hit_ratio'] = cache_hit_ratio
            log_entry['cache_hits'] = self._cache_hits
            log_entry['cache_misses'] = self._cache_misses

        with open(self.log_file_path, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

    def _log_detailed_metrics(self):
        now = time.time()
        if now - self._last_log_time >= self.log_interval_seconds:
            if not self._message_count_ep and not self._error_count_ep:
                return

            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent

            aggregated_metrics = {}
            for endpoint in set(list(self._message_count_ep.keys()) + list(self._error_count_ep.keys())):
                count = self._message_count_ep.get(endpoint, 0)
                error_count = self._error_count_ep.get(endpoint, 0)
                avg_latency_ms = (self._latency_sum_ep[endpoint] / count) * 1000 if count else 0
                throughput = count / self.log_interval_seconds if self.log_interval_seconds else 0

                aggregated_metrics[endpoint] = {
                    "request_count": count,
                    "error_count": error_count,
                    "latency_ms": avg_latency_ms,
                    "req_per_sec": throughput,
                }

                for status_code, status_count in self._status_code_ep[endpoint].items():
                    aggregated_metrics[endpoint][str(status_code)] = status_count


            log_entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "cpu": cpu_percent,
                "mem_usage": memory_percent,
                "endpoints": aggregated_metrics
            }

            with open(self.log_file_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")

            
            self._latency_sum_ep = defaultdict(float)
            self._message_count_ep = defaultdict(int)
            self._error_count_ep = defaultdict(int)
            self._status_code_ep = defaultdict(lambda: defaultdict(int))
            self._last_log_time = now
