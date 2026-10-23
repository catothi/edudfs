package edubase.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * EduBase only (no counterpart in the original; comes closest to the TRACE log level of the
 * regionserver and to the read metrics): records the read path of a single Get/Scan -
 * which MemStore segments and HFiles were consulted, what the Bloom filter answered, which
 * block came from the BlockCache and which from HDFS.
 *
 * Switched on with {@code get 't', 'row', {TRACE => true}} in the shell.
 */
public class ReadTrace implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> lines = new ArrayList<>();

    public synchronized void add(String line) {
        lines.add(line);
    }

    public synchronized List<String> getLines() {
        return new ArrayList<>(lines);
    }
}
