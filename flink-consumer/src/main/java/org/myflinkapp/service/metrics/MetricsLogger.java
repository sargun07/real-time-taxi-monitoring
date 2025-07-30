package org.myflinkapp.service.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsLogger {

    private final String serviceName;
    private final String operatorName;
    private final String logFilePath;
    private final String errorLogPath;
    private final int logIntervalSeconds;

    private double latencySum = 0.0;
    private int messageCount = 0;
    private int errorCount = 0;

    private final Map<String, Double> latencySumEp = new ConcurrentHashMap<>();
    private final Map<String, Integer> messageCountEp = new ConcurrentHashMap<>();
    private final Map<String, Integer> errorCountEp = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Integer>> statusCodeEp = new ConcurrentHashMap<>();

    private int cacheHits = 0;
    private int cacheMisses = 0;

    private long lastLogTimeBasic;
    private long lastLogTimeDetailed;
    private final Object basicLogLock = new Object();

    private final ObjectMapper mapper = new ObjectMapper();
    private final SystemInfo systemInfo = new SystemInfo();
    private final CentralProcessor processor = systemInfo.getHardware().getProcessor();
    private final GlobalMemory memory = systemInfo.getHardware().getMemory();
    private long[] prevTicks;

    public MetricsLogger(String serviceName, String operator, int logIntervalSeconds) {
        this.serviceName = serviceName;
        this.operatorName = operator;
        this.logIntervalSeconds = logIntervalSeconds;
        this.logFilePath = "/app/metrics/" + serviceName + ".log";
        this.errorLogPath = "/app/metrics/" + serviceName + ".error.log";
        this.lastLogTimeBasic = System.currentTimeMillis();
        this.lastLogTimeDetailed = System.currentTimeMillis();
        this.prevTicks = processor.getSystemCpuLoadTicks();
    }

    public void recordSuccess(double latencySeconds) {
        latencySum += latencySeconds;
        messageCount++;
        maybeLog();
    }

    public void recordError(String errorMessage) {
        errorCount++;
        logError(errorMessage);
        maybeLog();
    }

    public void recordCacheHit() {
        cacheHits++;
        maybeLog();
    }

    public void recordCacheMiss() {
        cacheMisses++;
        maybeLog();
    }

    public void recordRequest(String endpoint, int statusCode, double latencySeconds) {
        latencySumEp.merge(endpoint, latencySeconds, Double::sum);
        messageCountEp.merge(endpoint, 1, Integer::sum);

        statusCodeEp.putIfAbsent(endpoint, new ConcurrentHashMap<>());
        statusCodeEp.get(endpoint).merge(statusCode, 1, Integer::sum);

        maybeLogDetailed();
    }

    public void recordErrorEp(String endpoint, int statusCode, String errorMessage) {
        errorCountEp.merge(endpoint, 1, Integer::sum);

        statusCodeEp.putIfAbsent(endpoint, new ConcurrentHashMap<>());
        statusCodeEp.get(endpoint).merge(statusCode, 1, Integer::sum);

        logError("ERROR (" + endpoint + "): " + errorMessage);
        maybeLogDetailed();
    }

    private void maybeLog() {
        long now = System.currentTimeMillis();
        synchronized (basicLogLock) {
            if (now - lastLogTimeBasic >= logIntervalSeconds * 1000L) {
                logAggregatedMetrics();
                resetCounters();
                lastLogTimeBasic = now;
            }
        }
    }

    private void maybeLogDetailed() {
        long now = System.currentTimeMillis();
        if (now - lastLogTimeDetailed >= logIntervalSeconds * 1000L) {
            logDetailedMetrics();
            resetEndpointCounters();
            lastLogTimeDetailed = now;
        }
    }

    private void logAggregatedMetrics() {
        if (messageCount == 0 && errorCount == 0)
            return;

        double avgLatencyMs = messageCount > 0 ? (latencySum / messageCount) : 0;
        double throughput = messageCount / (double) logIntervalSeconds;

        Map<String, Object> logEntry = new HashMap<>();
        logEntry.put("operator_name", this.operatorName);
        logEntry.put("timestamp", Instant.now().toString());
        logEntry.put("latency_ms", Math.round(avgLatencyMs * 100.0) / 100.0);
        logEntry.put("throughput", throughput);
        logEntry.put("cpu", getCpuLoadPercent());
        logEntry.put("mem_usage", getMemoryUsagePercent());

        int totalCache = cacheHits + cacheMisses;
        if (totalCache > 0) {
            logEntry.put("cache_hit_ratio", cacheHits / (double) totalCache);
            logEntry.put("cache_hits", cacheHits);
            logEntry.put("cache_misses", cacheMisses);
        }

        writeToFile(logFilePath, logEntry);
    }

    private void logDetailedMetrics() {
        if (messageCountEp.isEmpty() && errorCountEp.isEmpty())
            return;

        Map<String, Object> endpoints = new HashMap<>();
        for (String ep : new HashSet<>(messageCountEp.keySet())) {
            int count = messageCountEp.getOrDefault(ep, 0);
            int err = errorCountEp.getOrDefault(ep, 0);
            double avgLatencyMs = count > 0 ? (latencySumEp.getOrDefault(ep, 0.0) / count) * 1000 : 0;

            Map<String, Object> epData = new HashMap<>();
            epData.put("request_count", count);
            epData.put("error_count", err);
            epData.put("latency_ms", avgLatencyMs);
            epData.put("req_per_sec", count / (double) logIntervalSeconds);

            Map<Integer, Integer> codes = statusCodeEp.getOrDefault(ep, new HashMap<>());
            for (Map.Entry<Integer, Integer> entry : codes.entrySet()) {
                epData.put(String.valueOf(entry.getKey()), entry.getValue());
            }

            endpoints.put(ep, epData);
        }

        Map<String, Object> logEntry = new HashMap<>();
        logEntry.put("timestamp", Instant.now().toString());
        logEntry.put("cpu", getCpuLoadPercent());
        logEntry.put("mem_usage", getMemoryUsagePercent());
        logEntry.put("endpoints", endpoints);

        writeToFile(logFilePath, logEntry);
    }

    private double getCpuLoadPercent() {
        long[] newTicks = processor.getSystemCpuLoadTicks();
        double load = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100.0;
        prevTicks = newTicks;
        return load;
    }

    private double getMemoryUsagePercent() {
        return 100.0 * (memory.getTotal() - memory.getAvailable()) / memory.getTotal();
    }

    private void logError(String errorMessage) {
        try (FileWriter writer = new FileWriter(errorLogPath, true)) {
            writer.write("[" + Instant.now().toString() + "] " + errorMessage + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToFile(String path, Map<String, Object> data) {
        try (FileWriter writer = new FileWriter(path, true)) {
            writer.write(mapper.writeValueAsString(data) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void resetCounters() {
        latencySum = 0.0;
        messageCount = 0;
        errorCount = 0;
        cacheHits = 0;
        cacheMisses = 0;
    }

    private void resetEndpointCounters() {
        latencySumEp.clear();
        messageCountEp.clear();
        errorCountEp.clear();
        statusCodeEp.clear();
    }
}
