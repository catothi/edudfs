package edudfs.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Corresponds to org.apache.hadoop.util.SequentialNumber.
 * Base for INode IDs, block IDs and generation stamps.
 */
public abstract class SequentialNumber {

    private final AtomicLong currentValue;

    protected SequentialNumber(long initialValue) {
        currentValue = new AtomicLong(initialValue);
    }

    public long getCurrentValue() {
        return currentValue.get();
    }

    public void setCurrentValue(long value) {
        currentValue.set(value);
    }

    /** Returns the next value (current + 1). */
    public long nextValue() {
        return currentValue.incrementAndGet();
    }
}
