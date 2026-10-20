package edudfs.server.datanode;

import java.io.IOException;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy
 * (default of dfs.datanode.fsdataset.volume.choosing.policy).
 *
 * New replicas are distributed across the volumes in round-robin order; volumes with too little free
 * space for a whole block are skipped. The NameNode only chooses the
 * DataNode (and the StorageType) - WHICH disk is decided by the DataNode itself.
 * Alternative in the original: AvailableSpaceVolumeChoosingPolicy (prefers emptier disks).
 */
class RoundRobinVolumeChoosingPolicy {

    private int curVolume = 0;

    synchronized FsVolumeImpl chooseVolume(List<FsVolumeImpl> volumes, long blockSize) throws IOException {
        if (volumes.isEmpty()) {
            throw new IOException("No more available volumes");
        }
        if (curVolume >= volumes.size()) {
            curVolume = 0;  // the list has become shorter due to a volume failure
        }
        int start = curVolume;
        long maxAvailable = 0;
        while (true) {
            FsVolumeImpl v = volumes.get(curVolume);
            curVolume = (curVolume + 1) % volumes.size();
            long available = v.getAvailable();
            if (available >= blockSize) {
                return v;
            }
            maxAvailable = Math.max(maxAvailable, available);
            if (curVolume == start) {
                // Original: DiskOutOfSpaceException
                throw new IOException("Out of space: the volume with the most available space (=" + maxAvailable
                        + " B) is less than the block size (=" + blockSize + " B)");
            }
        }
    }
}
