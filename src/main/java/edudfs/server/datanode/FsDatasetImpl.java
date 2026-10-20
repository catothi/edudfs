package edudfs.server.datanode;

import edudfs.common.DataChecksum;
import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.Block;
import edudfs.server.common.HdfsServerConstants.ReplicaState;
import edudfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import edudfs.server.protocol.BlockReportReplica;
import edudfs.server.protocol.ReplicaRecoveryInfo;
import edudfs.server.protocol.StorageBlockReport;
import edudfs.server.protocol.StorageReport;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Corresponds to FsDatasetImpl (+ FsVolumeList, ReplicaMap): all replicas of a DataNode
 * on all of its volumes (disks).
 *
 * <ul>
 *   <li>Volumes: one per entry in dfs.datanode.data.dir, see {@link FsVolumeImpl}
 *       (which also describes the on-disk layout).</li>
 *   <li>volumeMap: ONE map blockId -&gt; replica across all volumes; each replica knows
 *       its volume.</li>
 *   <li>New replicas (createRbw) are placed on the volumes in round-robin order
 *       ({@link RoundRobinVolumeChoosingPolicy}).</li>
 *   <li>If a volume fails, its replicas disappear from the volumeMap; the DataNode
 *       keeps running as long as at most dfs.datanode.failed.volumes.tolerated volumes
 *       have failed.</li>
 * </ul>
 * The generation stamp is encoded in the name of the .meta file.
 */
public class FsDatasetImpl {

    private static final String LOG = "FsDataset";

    static final short META_VERSION = 1;
    /** short version + byte checksumType + int bytesPerChecksum. */
    static final int META_HEADER_SIZE = 7;

    private static final Pattern META_FILE = Pattern.compile("blk_(-?\\d+)_(\\d+)\\.meta");

    private final String dnName;
    private final int volFailuresTolerated;
    /** Healthy volumes (original: FsVolumeList). */
    private final List<FsVolumeImpl> volumes = new CopyOnWriteArrayList<>();
    /** Failed volumes - at startup or during operation (original: VolumeFailureInfo). */
    private final List<StorageLocation> failedLocations = new CopyOnWriteArrayList<>();
    private final RoundRobinVolumeChoosingPolicy volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
    /** blockId -> replica (in the original: ReplicaMap, per block pool). */
    private final Map<Long, ReplicaInfo> volumeMap = new HashMap<>();

    FsDatasetImpl(List<StorageLocation> locations, String datanodeUuid, int volFailuresTolerated, String dnName)
            throws IOException {
        this.dnName = dnName;
        this.volFailuresTolerated = volFailuresTolerated;
        if (volFailuresTolerated < 0 || volFailuresTolerated >= locations.size()) {
            throw new IOException("Invalid value configured for dfs.datanode.failed.volumes.tolerated - "
                    + volFailuresTolerated + ". Value configured is either less than 0 or >= to the number of"
                    + " configured volumes (" + locations.size() + ").");
        }
        for (StorageLocation loc : locations) {
            try {
                FsVolumeImpl v = new FsVolumeImpl(loc, datanodeUuid);
                volumes.add(v);
                // At startup: load existing replicas. RBW replicas lose their writer -> RWR.
                addToReplicasMap(v, v.getFinalizedRoot(), ReplicaState.FINALIZED);
                addToReplicasMap(v, v.getRbwDir(), ReplicaState.RWR);
                Log.info(LOG, dnName + ": added volume " + v);
            } catch (IOException e) {
                Log.warn(LOG, dnName + ": failed to add volume " + loc + ": " + e.getMessage());
                failedLocations.add(loc);
            }
        }
        if (!hasEnoughResource()) {
            throw new IOException("Too many failed volumes - current valid volumes: " + volumes.size()
                    + ", volumes configured: " + locations.size() + ", volumes failed: " + failedLocations.size()
                    + ", volume failures tolerated: " + volFailuresTolerated);
        }
    }

    /** Recursively loads all replicas of a directory (finalized/ has subdir levels). */
    private void addToReplicasMap(FsVolumeImpl v, File dir, ReplicaState state) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File f : files) {
            if (f.isDirectory()) {
                addToReplicasMap(v, f, state);
                continue;
            }
            Matcher m = META_FILE.matcher(f.getName());
            if (!m.matches()) {
                continue;
            }
            long id = Long.parseLong(m.group(1));
            long gs = Long.parseLong(m.group(2));
            File blockFile = new File(dir, Block.BLOCK_FILE_PREFIX + id);
            if (!blockFile.exists()) {
                continue;
            }
            ReplicaInfo old = volumeMap.get(id);
            if (old != null && old.getGenerationStamp() >= gs) {
                // Same block on two volumes (original: resolveDuplicateReplicas) - the newer GS wins
                continue;
            }
            volumeMap.put(id, new ReplicaInfo(id, gs, blockFile.length(), state, v, dir));
        }
    }

    static String metaFileName(long blockId, long gs) {
        return Block.BLOCK_FILE_PREFIX + blockId + "_" + gs + Block.METADATA_EXTENSION;
    }

    File getBlockFile(ReplicaInfo r) {
        return new File(r.getDir(), Block.BLOCK_FILE_PREFIX + r.getBlockId());
    }

    File getMetaFile(ReplicaInfo r) {
        return new File(r.getDir(), metaFileName(r.getBlockId(), r.getGenerationStamp()));
    }

    public synchronized ReplicaInfo getReplica(long blockId) {
        return volumeMap.get(blockId);
    }

    public List<FsVolumeImpl> getVolumes() {
        return List.copyOf(volumes);
    }

    // ============================================================== Writing
    /** Create a new replica in state RBW (start of a pipeline write). */
    synchronized ReplicaInfo createRbw(Block b) throws IOException {
        ReplicaInfo old = volumeMap.get(b.getBlockId());
        if (old != null) {
            if (old.getState() == ReplicaState.FINALIZED && old.getGenerationStamp() >= b.getGenerationStamp()
                    || old.hasWriter()) {
                throw new IOException("Block " + b + " already exists in state " + old.getState()
                        + " and thus cannot be created.");
            }
            deleteFiles(old);  // replace the stale replica
            volumeMap.remove(b.getBlockId());
        }
        FsVolumeImpl v = volumeChoosingPolicy.chooseVolume(volumes, b.getNumBytes());
        ReplicaInfo r = new ReplicaInfo(b.getBlockId(), b.getGenerationStamp(), 0, ReplicaState.RBW, v,
                v.getRbwDir());
        new FileOutputStream(getBlockFile(r)).close();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(getMetaFile(r)))) {
            out.writeShort(META_VERSION);
            out.writeByte(DataChecksum.CHECKSUM_CRC32);
            out.writeInt(HdfsConfig.BYTES_PER_CHECKSUM);
        }
        volumeMap.put(r.getBlockId(), r);
        return r;
    }

    /** RBW -> FINALIZED: move the files from rbw/ to finalized/subdirX/subdirY/ (same volume). */
    synchronized void finalizeBlock(ReplicaInfo r) throws IOException {
        if (volumeMap.get(r.getBlockId()) != r || r.getState() != ReplicaState.RBW) {
            throw new IOException("Cannot finalize " + r + " - replica was removed or is under recovery");
        }
        moveReplicaFiles(r, r.getVolume().getFinalizedDir(r.getBlockId()), r.getGenerationStamp());
        r.setState(ReplicaState.FINALIZED);
    }

    private void moveReplicaFiles(ReplicaInfo r, File newDir, long newGs) throws IOException {
        File oldBlock = getBlockFile(r);
        File oldMeta = getMetaFile(r);
        File newBlock = new File(newDir, Block.BLOCK_FILE_PREFIX + r.getBlockId());
        File newMeta = new File(newDir, metaFileName(r.getBlockId(), newGs));
        Files.move(oldBlock.toPath(), newBlock.toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.move(oldMeta.toPath(), newMeta.toPath(), StandardCopyOption.REPLACE_EXISTING);
        r.setDir(newDir);
        r.setGenerationStamp(newGs);
    }

    // ============================================================= Deletion
    /** Execute DNA_INVALIDATE. Returns the replicas actually deleted (for the IBR per storage). */
    List<ReplicaInfo> invalidate(Block[] blocks) {
        List<ReplicaInfo> toDelete = new ArrayList<>();
        synchronized (this) {
            for (Block b : blocks) {
                ReplicaInfo r = volumeMap.get(b.getBlockId());
                if (r == null) {
                    continue;
                }
                if (r.getGenerationStamp() != b.getGenerationStamp()) {
                    Log.info(LOG, dnName + ": not deleting " + r + ", NN asked to delete " + b + " (GS mismatch)");
                    continue;
                }
                volumeMap.remove(b.getBlockId());
                toDelete.add(r);
            }
        }
        for (ReplicaInfo r : toDelete) {
            r.stopWriter(2000);
            deleteFiles(r);
            Log.info(LOG, dnName + ": deleted " + r + " from " + r.getVolume().getStorageID());
        }
        return toDelete;
    }

    private void deleteFiles(ReplicaInfo r) {
        getBlockFile(r).delete();
        getMetaFile(r).delete();
    }

    // ======================================================= Volume failure
    /**
     * Corresponds to FsDatasetImpl.checkDataDir() + handleVolumeFailures(): checks all volumes,
     * removes broken ones together with their replicas and returns the newly failed volumes.
     * (Original: DatasetVolumeChecker checks asynchronously, triggered by IO errors or
     * periodically; here the BPServiceActor calls this on every heartbeat.)
     */
    List<FsVolumeImpl> checkDataDir() {
        List<FsVolumeImpl> failed = new ArrayList<>();
        for (FsVolumeImpl v : volumes) {
            if (!v.isHealthy()) {
                failed.add(v);
            }
        }
        if (failed.isEmpty()) {
            return failed;
        }
        synchronized (this) {
            for (FsVolumeImpl v : failed) {
                volumes.remove(v);
                failedLocations.add(v.getStorageLocation());
                int removed = 0;
                Iterator<ReplicaInfo> it = volumeMap.values().iterator();
                while (it.hasNext()) {
                    ReplicaInfo r = it.next();
                    if (r.getVolume() == v) {
                        it.remove();
                        removed++;
                    }
                }
                Log.warn(LOG, dnName + ": volume " + v + " FAILED - removed " + removed + " replica(s) from"
                        + " volumeMap, " + volumes.size() + " volume(s) left");
            }
        }
        return failed;
    }

    /** Like DataNode.hasEnoughResource / FsVolumeList: at most volFailuresTolerated have failed. */
    boolean hasEnoughResource() {
        return !volumes.isEmpty() && failedLocations.size() <= volFailuresTolerated;
    }

    int getNumFailedVolumes() {
        return failedLocations.size();
    }

    List<StorageLocation> getFailedStorageLocations() {
        return List.copyOf(failedLocations);
    }

    // ======================================================= Block report
    /** Full block report, one entry per (healthy) volume - including empty ones. */
    synchronized StorageBlockReport[] getBlockReport() {
        Map<FsVolumeImpl, List<BlockReportReplica>> perVolume = new LinkedHashMap<>();
        for (FsVolumeImpl v : volumes) {
            perVolume.put(v, new ArrayList<>());
        }
        for (ReplicaInfo info : volumeMap.values()) {
            List<BlockReportReplica> l = perVolume.get(info.getVolume());
            if (l != null) {
                l.add(new BlockReportReplica(info.toBlock(), info.getState()));
            }
        }
        List<StorageBlockReport> r = new ArrayList<>();
        for (Map.Entry<FsVolumeImpl, List<BlockReportReplica>> e : perVolume.entrySet()) {
            r.add(new StorageBlockReport(e.getKey().toDatanodeStorage(),
                    e.getValue().toArray(new BlockReportReplica[0])));
        }
        return r.toArray(new StorageBlockReport[0]);
    }

    /** Part of every heartbeat: capacity/usage per healthy volume (original: getStorageReports). */
    synchronized StorageReport[] getStorageReports() {
        List<StorageReport> r = new ArrayList<>();
        for (FsVolumeImpl v : volumes) {
            r.add(new StorageReport(v.toDatanodeStorage(), false, v.getCapacity(), getDfsUsed(v), v.getAvailable()));
        }
        return r.toArray(new StorageReport[0]);
    }

    private long getDfsUsed(FsVolumeImpl v) {
        long sum = 0;
        for (ReplicaInfo r : volumeMap.values()) {
            if (r.getVolume() == v) {
                sum += r.getNumBytes();
            }
        }
        return sum;
    }

    synchronized long getDfsUsed() {
        long sum = 0;
        for (ReplicaInfo r : volumeMap.values()) {
            sum += r.getNumBytes();
        }
        return sum;
    }

    public synchronized int getNumBlocks() {
        return volumeMap.size();
    }

    // ===================================================== Block recovery
    /** InterDatanodeProtocol.initReplicaRecovery: stop the writer, replica -> RUR. */
    ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rb) throws IOException {
        Block block = rb.getBlock();
        long recoveryId = rb.getNewGenerationStamp();
        ReplicaInfo r = getReplica(block.getBlockId());
        if (r != null && r.getState() == ReplicaState.RBW) {
            // Outside the lock: the writer may itself need the lock in order to terminate
            r.stopWriter(2000);
        }
        synchronized (this) {
            r = volumeMap.get(block.getBlockId());
            if (r == null) {
                return null;
            }
            if (r.getGenerationStamp() < block.getGenerationStamp()) {
                throw new IOException("replica.getGenerationStamp() < block.getGenerationStamp(), replica=" + r
                        + ", block=" + block);
            }
            if (r.getGenerationStamp() >= recoveryId) {
                throw new IOException("replica.getGenerationStamp() >= recoveryId = " + recoveryId + ", replica="
                        + r);
            }
            if (r.getState() == ReplicaState.RUR) {
                if (r.getRecoveryId() >= recoveryId) {
                    throw new IOException("rur.getRecoveryID() >= recoveryId = " + recoveryId + ", replica=" + r);
                }
            } else {
                r.setOriginalState(r.getState());
                r.setState(ReplicaState.RUR);
            }
            r.setRecoveryId(recoveryId);
            r.setNumBytes(getBlockFile(r).length());
            Log.info(LOG, dnName + ": initReplicaRecovery " + r + " (original state " + r.getOriginalState()
                    + "), recoveryId=" + recoveryId);
            return new ReplicaRecoveryInfo(r.getBlockId(), r.getNumBytes(), r.getGenerationStamp(),
                    r.getOriginalState());
        }
    }

    /** InterDatanodeProtocol.updateReplicaUnderRecovery: truncate, new GS, finalize. */
    synchronized ReplicaInfo updateReplicaUnderRecovery(Block oldBlock, long recoveryId, long newLength)
            throws IOException {
        ReplicaInfo r = volumeMap.get(oldBlock.getBlockId());
        if (r == null || r.getState() != ReplicaState.RUR) {
            throw new IOException("Replica " + r + " is not under recovery");
        }
        if (r.getRecoveryId() != recoveryId) {
            throw new IOException("Replica " + r + " has recoveryId " + r.getRecoveryId() + " != " + recoveryId);
        }
        if (newLength > r.getNumBytes()) {
            throw new IOException("newLength " + newLength + " > replica length " + r.getNumBytes());
        }
        if (newLength < r.getNumBytes()) {
            truncateBlock(getBlockFile(r), getMetaFile(r), newLength);
        }
        r.setNumBytes(newLength);
        moveReplicaFiles(r, r.getVolume().getFinalizedDir(r.getBlockId()), recoveryId);
        r.setState(ReplicaState.FINALIZED);
        r.setOriginalState(null);
        Log.info(LOG, dnName + ": updateReplicaUnderRecovery -> " + r);
        return r;
    }

    /** Like FsDatasetImpl.truncateBlock: truncate data + checksums, recompute the CRC of the last (partial) chunk. */
    static void truncateBlock(File blockFile, File metaFile, long newLen) throws IOException {
        int bpc = HdfsConfig.BYTES_PER_CHECKSUM;
        long n = (newLen + bpc - 1) / bpc;  // number of chunks
        long newMetaLen = META_HEADER_SIZE + n * DataChecksum.CHECKSUM_SIZE;
        int lastCrc = 0;
        try (RandomAccessFile bf = new RandomAccessFile(blockFile, "rw")) {
            bf.setLength(newLen);
            if (n > 0) {
                long lastChunkOffset = (n - 1) * bpc;
                byte[] b = new byte[(int) (newLen - lastChunkOffset)];
                bf.seek(lastChunkOffset);
                bf.readFully(b);
                lastCrc = DataChecksum.crc(b, 0, b.length);
            }
        }
        try (RandomAccessFile mf = new RandomAccessFile(metaFile, "rw")) {
            mf.setLength(newMetaLen);
            if (n > 0) {
                mf.seek(newMetaLen - DataChecksum.CHECKSUM_SIZE);
                mf.writeInt(lastCrc);
            }
        }
    }
}
