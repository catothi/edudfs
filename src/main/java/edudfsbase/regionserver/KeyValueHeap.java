package edudfsbase.regionserver;

import edudfsbase.Cell;
import edudfsbase.CellComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.KeyValueHeap: merges k sorted scanners
 * (MemStore + n HFiles) into ONE sorted stream - the k-way merge of an LSM tree.
 *
 * A priority queue holds the scanners, ordered by their current cell. {@code current} is the
 * scanner with the smallest cell; after next() it goes back into the queue if another scanner
 * now has a smaller cell. Since the CellComparator sorts the newest version and delete markers
 * first, the reader sees exactly the order in which it must decide.
 */
public class KeyValueHeap implements KeyValueScanner {

    private final PriorityQueue<KeyValueScanner> heap;
    private KeyValueScanner current;

    public KeyValueHeap(List<? extends KeyValueScanner> scanners) {
        heap = new PriorityQueue<>(Math.max(1, scanners.size()),
                (a, b) -> CellComparator.getInstance().compare(a.peek(), b.peek()));
        for (KeyValueScanner s : scanners) {
            if (s.peek() != null) {
                heap.add(s);
            } else {
                s.close();
            }
        }
        current = heap.poll();
    }

    @Override
    public Cell peek() {
        return current == null ? null : current.peek();
    }

    @Override
    public Cell next() throws IOException {
        if (current == null) {
            return null;
        }
        Cell kv = current.next();
        Cell kvNext = current.peek();
        if (kvNext == null) {
            current.close();
            current = heap.poll();
        } else {
            KeyValueScanner top = heap.peek();
            if (top != null && CellComparator.getInstance().compare(kvNext, top.peek()) > 0) {
                heap.add(current);
                current = heap.poll();
            }
        }
        return kv;
    }

    @Override
    public boolean seek(Cell key) throws IOException {
        return doSeek(key, false);
    }

    @Override
    public boolean reseek(Cell key) throws IOException {
        return doSeek(key, true);
    }

    private boolean doSeek(Cell key, boolean forward) throws IOException {
        List<KeyValueScanner> all = new ArrayList<>(heap);
        heap.clear();
        if (current != null) {
            all.add(current);
        }
        for (KeyValueScanner s : all) {
            boolean more = forward ? s.reseek(key) : s.seek(key);
            if (more && s.peek() != null) {
                heap.add(s);
            } else {
                s.close();
            }
        }
        current = heap.poll();
        return current != null;
    }

    /** EduDFSBase: source of the cell that peek() returns. */
    public String getCurrentSourceName() {
        return current == null ? null : current.getSourceName();
    }

    @Override
    public void close() {
        if (current != null) {
            current.close();
        }
        for (KeyValueScanner s : heap) {
            s.close();
        }
        heap.clear();
        current = null;
    }

    @Override
    public String getSourceName() {
        return "KeyValueHeap";
    }
}
