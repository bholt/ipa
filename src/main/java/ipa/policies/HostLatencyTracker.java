package ipa.policies;

import java.util.concurrent.atomic.AtomicReference;

public class HostLatencyTracker {

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
