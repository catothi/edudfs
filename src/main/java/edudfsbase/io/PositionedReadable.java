package edudfsbase.io;

import edudfs.client.DFSInputStream;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Corresponds to org.apache.hadoop.fs.PositionedReadable (+ FSDataInputStreamWrapper): a file
 * from which you can read at any offset. The HFile reader needs nothing else - it reads the
 * trailer at the end, the index and then single blocks at the offsets the index names.
 *
 * Implementations: a file in EduDFS (pread over DFSInputStream), a local file and a byte[]
 * (for tests and for {@code ./hbase hfile} on a local copy).
 */
public interface PositionedReadable extends Closeable {

    void readFully(long position, byte[] buf, int off, int len) throws IOException;

    long getLength();

    static PositionedReadable of(DFSInputStream in) {
        return new PositionedReadable() {
            @Override
            public void readFully(long position, byte[] buf, int off, int len) throws IOException {
                in.readFully(position, buf, off, len);
            }

            @Override
            public long getLength() {
                return in.getFileLength();
            }

            @Override
            public void close() {
                in.close();
            }
        };
    }

    static PositionedReadable of(byte[] data) {
        return new PositionedReadable() {
            @Override
            public void readFully(long position, byte[] buf, int off, int len) throws IOException {
                if (position + len > data.length) {
                    throw new EOFException("Read past end: " + (position + len) + " > " + data.length);
                }
                System.arraycopy(data, (int) position, buf, off, len);
            }

            @Override
            public long getLength() {
                return data.length;
            }

            @Override
            public void close() {
            }
        };
    }

    static PositionedReadable of(File f) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        return new PositionedReadable() {
            @Override
            public synchronized void readFully(long position, byte[] buf, int off, int len) throws IOException {
                raf.seek(position);
                raf.readFully(buf, off, len);
            }

            @Override
            public long getLength() {
                return f.length();
            }

            @Override
            public void close() throws IOException {
                raf.close();
            }
        };
    }
}
