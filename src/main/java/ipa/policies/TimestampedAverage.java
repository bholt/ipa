package ipa.policies;

public class TimestampedAverage {

    public final long timestamp;
    public final long average;
    public final long nbMeasure;

    TimestampedAverage(long timestamp, long average, long nbMeasure) {
        this.timestamp = timestamp;
        this.average = average;
        this.nbMeasure = nbMeasure;
    }
}
