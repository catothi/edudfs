package edudfs.server.datanode;

import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants.ReplicaState;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

/**
 * Corresponds to ReplicaInfo and the subclasses FinalizedReplica, ReplicaBeingWritten,
 * ReplicaWaitingToBeRecovered, ReplicaUnderRecovery - combined here using a state field.
 */
public class ReplicaInfo {

    private final long blockId;
    private long generationStamp;
    private volatile long numBytes;
    private ReplicaState state;
    private ReplicaState originalState;  // only in RUR
    private long recoveryId;              // only in RUR
    /** Volume (disk) on which the replica resides (original: ReplicaInfo.getVolume()). */
    private final FsVolumeImpl volume;
    /** Current directory: rbw/ or finalized/subdirX/subdirY/ of the volume. */
    private File dir;

    /** Current writer (BlockReceiver thread) - for RBW, so that a recovery can stop it. */
    private volatile Thread writer;
    private volatile Socket writerSocket;

    ReplicaInfo(long blockId, long generationStamp, long numBytes, ReplicaState state, FsVolumeImpl volume,
                File dir) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.numBytes = numBytes;
        this.state = state;
        this.volume = volume;
        this.dir = dir;
    }

    public long getBlockId() {
        return blockId;
    }

    public long getGenerationStamp() {
        return generationStamp;
    }

    void setGenerationStamp(long gs) {
        this.generationStamp = gs;
    }

    public long getNumBytes() {
        return numBytes;
    }

    void setNumBytes(long n) {
        this.numBytes = n;
    }

    public ReplicaState getState() {
        return state;
    }

    void setState(ReplicaState s) {
        this.state = s;
    }

    ReplicaState getOriginalState() {
        return originalState;
    }

    void setOriginalState(ReplicaState s) {
        this.originalState = s;
    }

    long getRecoveryId() {
        return recoveryId;
    }

    void setRecoveryId(long id) {
        this.recoveryId = id;
    }

    public FsVolumeImpl getVolume() {
        return volume;
    }

    File getDir() {
        return dir;
    }

    void setDir(File dir) {
        this.dir = dir;
    }

    void setWriter(Thread t, Socket s) {
        this.writer = t;
        this.writerSocket = s;
    }

    void clearWriter(Thread t) {
        if (writer == t) {
            writer = null;
            writerSocket = null;
        }
    }

    boolean hasWriter() {
        Thread t = writer;
        return t != null && t.isAlive();
    }

    /** Aborts the write: close the socket and wait for the thread to finish. */
    void stopWriter(long timeoutMs) {
        Socket s = writerSocket;
        Thread t = writer;
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(timeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Block toBlock() {
        return new Block(blockId, numBytes, generationStamp);
    }

    @Override
    public String toString() {
        return Block.BLOCK_FILE_PREFIX + blockId + "_" + generationStamp + "(" + state + ", len=" + numBytes + ")";
    }
}
