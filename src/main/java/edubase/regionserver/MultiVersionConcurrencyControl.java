package edubase.regionserver;

import java.io.InterruptedIOException;
import java.util.LinkedList;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl (MVCC).
 *
 * Every write gets a write number (= sequence ID of the edit, as in HBase 2). Its cells carry
 * this number. Readers remember the read point when they start and only see cells with
 * sequenceId &lt;= read point. The read point only advances once a write is COMPLETE (WAL synced
 * and all cells in the MemStore) - and only in order. This way a reader never sees half a row
 * (atomicity per row), without readers having to take a lock.
 *
 * In EduBase the writes of a region are serialized anyway (see HRegion), so the queue rarely holds
 * more than one entry; the mechanism is still the one of the original.
 */
public class MultiVersionConcurrencyControl {

    /** Corresponds to MultiVersionConcurrencyControl.WriteEntry. */
    public static final class WriteEntry {
        private final long writeNumber;
        private boolean completed;

        WriteEntry(long writeNumber) {
            this.writeNumber = writeNumber;
        }

        public long getWriteNumber() {
            return writeNumber;
        }
    }

    private long readPoint;
    private long writePoint;
    private final LinkedList<WriteEntry> writeQueue = new LinkedList<>();

    public MultiVersionConcurrencyControl(long startPoint) {
        this.readPoint = startPoint;
        this.writePoint = startPoint;
    }

    public synchronized WriteEntry begin() {
        WriteEntry e = new WriteEntry(++writePoint);
        writeQueue.add(e);
        return e;
    }

    /** Marks the write complete, advances the read point as far as possible and waits until it covers e. */
    public synchronized void completeAndWait(WriteEntry e) throws InterruptedIOException {
        e.completed = true;
        while (!writeQueue.isEmpty() && writeQueue.getFirst().completed) {
            readPoint = writeQueue.removeFirst().writeNumber;
        }
        notifyAll();
        while (readPoint < e.writeNumber) {
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
    }

    /** After opening a region: continue behind the highest sequence ID found in HFiles and WAL. */
    public synchronized void advanceTo(long seqId) {
        if (seqId > writePoint) {
            writePoint = seqId;
            readPoint = seqId;
        }
    }

    public synchronized long getReadPoint() {
        return readPoint;
    }

    public synchronized long getWritePoint() {
        return writePoint;
    }
}
