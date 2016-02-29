package ipa.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class ConsistencyLatencyTracker implements LatencyTracker {

    private static final Set<Class<? extends DriverException>> EXCLUDED_EXCEPTIONS = ImmutableSet.of(
            UnavailableException.class, // this is done via the snitch and is usually very fast
            OverloadedException.class,
            BootstrappingException.class,
            UnpreparedException.class,
            QueryValidationException.class // query validation also happens at early stages in the coordinator
    );

    private final ConcurrentMap<Host, HostLatencyTracker> latencies = new ConcurrentHashMap<Host, HostLatencyTracker>();

    private volatile long cachedMin = -1L;
    private volatile Host cachedMinHost = null;

    private final long scale;
    private final long retryPeriod;
    private final int minMeasure;
    private final ConsistencyLevel consistencyLevel;

    public ConsistencyLatencyTracker(ConsistencyLevel consistencyLevel, long scale, long retryPeriod, int minMeasure) {
        this.scale = scale;
        this.retryPeriod = retryPeriod;
        this.minMeasure = minMeasure;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (shouldConsiderNewLatency(statement, exception)) {
            HostLatencyTracker hostTracker = latencies.get(host);
            if (hostTracker == null) {
                hostTracker = new HostLatencyTracker(scale, (30L * minMeasure) / 100L);
                HostLatencyTracker old = latencies.putIfAbsent(host, hostTracker);
                if (old != null)
                    hostTracker = old;
            }
            hostTracker.add(newLatencyNanos);
        }
    }

    private boolean shouldConsiderNewLatency(Statement statement, Exception exception) {
        if (statement.getConsistencyLevel() != consistencyLevel) return false;
        // query was successful: always consider
        if (exception == null) return true;
        // filter out "fast" errors
        if (EXCLUDED_EXCEPTIONS.contains(exception.getClass())) return false;
        return true;
    }

    public void updateMin() {
        long newMin = Long.MAX_VALUE;
        long now = System.nanoTime();
        Host minHost = null;
        for (Map.Entry<Host,HostLatencyTracker> e : latencies.entrySet()) {
            TimestampedAverage latency = e.getValue().getCurrentAverage();
            if (latency != null && latency.average >= 0 && latency.nbMeasure >= minMeasure && (now - latency.timestamp) <= retryPeriod) {
                newMin = Math.min(newMin, latency.average);
                minHost = e.getKey();
            }
        }
        if (newMin != Long.MAX_VALUE) {
            cachedMin = newMin;
            cachedMinHost = minHost;
        }
    }

    public long getMin() {
        return cachedMin;
    }
    public Host getMinHost() { return cachedMinHost; }

    public long min() {
        updateMin();
        return getMin();
    }

    public TimestampedAverage latencyOf(Host host) {
        HostLatencyTracker tracker = latencies.get(host);
        return tracker == null ? null : tracker.getCurrentAverage();
    }

    public Map<Host, TimestampedAverage> currentLatencies() {
        Map<Host, TimestampedAverage> map = new HashMap<Host, TimestampedAverage>(latencies.size());
        for (Map.Entry<Host, HostLatencyTracker> entry : latencies.entrySet())
            map.put(entry.getKey(), entry.getValue().getCurrentAverage());
        return map;
    }

    public void resetHost(Host host) {
        latencies.remove(host);
    }

    @Override
    public void onRegister(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void onUnregister(Cluster cluster) {
        // nothing to do
    }
}

