package ipa.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class Tracker implements LatencyTracker {

    private static final Set<Class<? extends DriverException>> EXCLUDED_EXCEPTIONS = ImmutableSet.of(
            UnavailableException.class, // this is done via the snitch and is usually very fast
            OverloadedException.class,
            BootstrappingException.class,
            UnpreparedException.class,
            QueryValidationException.class // query validation also happens at early stages in the coordinator
    );

    private final ConcurrentMap<Host, HostLatencyTracker> latencies = new ConcurrentHashMap<Host, HostLatencyTracker>();
    private volatile long cachedMin = -1L;

    private final long scale;
    private final long retryPeriod;
    private final int minMeasure;

    public Tracker(long scale, long retryPeriod, int minMeasure) {
        this.scale = scale;
        this.retryPeriod = retryPeriod;
        this.minMeasure = minMeasure;
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
        // query was successful: always consider
        if (exception == null) return true;
        // filter out "fast" errors
        if (EXCLUDED_EXCEPTIONS.contains(exception.getClass())) return false;
        return true;
    }

    public void updateMin() {
        long newMin = Long.MAX_VALUE;
        long now = System.nanoTime();
        for (HostLatencyTracker tracker : latencies.values()) {
            TimestampedAverage latency = tracker.getCurrentAverage();
            if (latency != null && latency.average >= 0 && latency.nbMeasure >= minMeasure && (now - latency.timestamp) <= retryPeriod)
                newMin = Math.min(newMin, latency.average);
        }
        if (newMin != Long.MAX_VALUE)
            cachedMin = newMin;
    }

    public long getMinAverage() {
        return cachedMin;
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

class TimestampedAverage {

    final long timestamp;
    final long average;
    final long nbMeasure;

    TimestampedAverage(long timestamp, long average, long nbMeasure) {
        this.timestamp = timestamp;
        this.average = average;
        this.nbMeasure = nbMeasure;
    }
}

class HostLatencyTracker {

    private final long thresholdToAccount;
    private final double scale;
    private final AtomicReference<TimestampedAverage> current = new AtomicReference<TimestampedAverage>();

    HostLatencyTracker(long scale, long thresholdToAccount) {
        this.scale = (double) scale; // We keep in double since that's how we'll use it.
        this.thresholdToAccount = thresholdToAccount;
    }

    public void add(long newLatencyNanos) {
        TimestampedAverage previous, next;
        do {
            previous = current.get();
            next = computeNextAverage(previous, newLatencyNanos);
        } while (next != null && !current.compareAndSet(previous, next));
    }

    private TimestampedAverage computeNextAverage(TimestampedAverage previous, long newLatencyNanos) {

        long currentTimestamp = System.nanoTime();

        long nbMeasure = previous == null ? 1 : previous.nbMeasure + 1;
        if (nbMeasure < thresholdToAccount)
            return new TimestampedAverage(currentTimestamp, -1L, nbMeasure);

        if (previous == null || previous.average < 0)
            return new TimestampedAverage(currentTimestamp, newLatencyNanos, nbMeasure);

        // Note: it's possible for the delay to be 0, in which case newLatencyNanos will basically be
        // discarded. It's fine: nanoTime is precise enough in practice that even if it happens, it
        // will be very rare, and discarding a latency every once in a while is not the end of the world.
        // We do test for negative value, even though in theory that should not happen, because it seems
        // that historically there has been bugs here (https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks)
        // so while this is almost surely not a problem anymore, there's no reason to break the computation
        // if this even happen.
        long delay = currentTimestamp - previous.timestamp;
        if (delay <= 0)
            return null;

        double scaledDelay = ((double) delay) / scale;
        // Note: We don't use log1p because we it's quite a bit slower and we don't care about the precision (and since we
        // refuse ridiculously big scales, scaledDelay can't be so low that scaledDelay+1 == 1.0 (due to rounding)).
        double prevWeight = Math.log(scaledDelay + 1) / scaledDelay;
        long newAverage = (long) ((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

        return new TimestampedAverage(currentTimestamp, newAverage, nbMeasure);
    }

    public TimestampedAverage getCurrentAverage() {
        return current.get();
    }
}
