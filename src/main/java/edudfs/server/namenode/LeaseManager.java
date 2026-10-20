package edudfs.server.namenode;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;
import edudfs.server.common.HdfsServerConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.LeaseManager.
 *
 * HDFS allows exactly ONE writer per file. The writer holds a lease that it
 * must renew regularly (DFSClient: LeaseRenewer, every softLimit/2).
 * A lease belongs to a client ("holder") and covers ALL of its open files.
 *
 * <ul>
 *   <li><b>Soft limit</b> expired: another client may take over the lease
 *       (create/append then trigger a lease recovery).</li>
 *   <li><b>Hard limit</b> expired: the NameNode itself starts the lease recovery
 *       (monitor thread) so that the file does not stay open forever.</li>
 * </ul>
 *
 * Since HDFS-6757 the LeaseManager stores INode IDs instead of paths - renaming
 * an open file therefore requires no lease adjustment.
 */
public class LeaseManager {

    private static final String LOG = "LeaseManager";

    private final FSNamesystem fsnamesystem;

    /** holder -> Lease. */
    private final Map<String, Lease> leases = new TreeMap<>();
    /** Sorted by last renewal: the oldest leases come first. */
    private final NavigableSet<Lease> sortedLeases = new TreeSet<>(
            Comparator.comparingLong(Lease::getLastUpdate).thenComparing(Lease::getHolder));
    /** INode ID -> Lease. */
    private final Map<Long, Lease> leasesById = new HashMap<>();

    private volatile boolean shouldRunMonitor;
    private Thread lmthread;

    LeaseManager(FSNamesystem fsnamesystem) {
        this.fsnamesystem = fsnamesystem;
    }

    /** A lease: holder + time of the last renewal + open files (INode IDs). */
    public static class Lease {
        private final String holder;
        private long lastUpdate;
        private final Set<Long> files = new TreeSet<>();

        private Lease(String holder) {
            this.holder = holder;
            renew();
        }

        private void renew() {
            lastUpdate = Time.monotonicNow();
        }

        public String getHolder() {
            return holder;
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public boolean expiredHardLimit(long now) {
            return now - lastUpdate > HdfsConfig.LEASE_HARDLIMIT_MS;
        }

        public boolean expiredSoftLimit(long now) {
            return now - lastUpdate > HdfsConfig.LEASE_SOFTLIMIT_MS;
        }

        public Collection<Long> getFiles() {
            return Collections.unmodifiableSet(files);
        }

        @Override
        public String toString() {
            return "[Lease.  Holder: " + holder + ", pending creates: " + files.size() + "]";
        }
    }

    public synchronized Lease getLease(String holder) {
        return leases.get(holder);
    }

    public synchronized Lease getLease(INodeFile file) {
        return leasesById.get(file.getId());
    }

    public synchronized List<Lease> getLeases() {
        return new ArrayList<>(sortedLeases);
    }

    /** Registers a file for holder; if the lease already exists, it is renewed. */
    synchronized Lease addLease(String holder, long inodeId) {
        Lease lease = leases.get(holder);
        if (lease == null) {
            lease = new Lease(holder);
            leases.put(holder, lease);
            sortedLeases.add(lease);
        } else {
            renewLease(lease);
        }
        leasesById.put(inodeId, lease);
        lease.files.add(inodeId);
        return lease;
    }

    synchronized void removeLease(long inodeId) {
        Lease lease = leasesById.get(inodeId);
        if (lease != null) {
            removeLease(lease, inodeId);
        }
    }

    private synchronized void removeLease(Lease lease, long inodeId) {
        leasesById.remove(inodeId);
        lease.files.remove(inodeId);
        if (lease.files.isEmpty()) {
            leases.remove(lease.holder);
            sortedLeases.remove(lease);
        }
    }

    /** Transfers a file's lease to a new holder (recovery: -> HDFS_NameNode). */
    synchronized Lease reassignLease(Lease lease, INodeFile file, String newHolder) {
        if (lease != null) {
            removeLease(lease, file.getId());
        }
        file.getFileUnderConstructionFeature().setClientName(newHolder);
        return addLease(newHolder, file.getId());
    }

    /** RPC renewLease: renews the client's lease (for all of its files). */
    public synchronized void renewLease(String holder) {
        renewLease(leases.get(holder));
    }

    private synchronized void renewLease(Lease lease) {
        if (lease != null) {
            sortedLeases.remove(lease);  // sort key changes -> re-insert
            lease.renew();
            sortedLeases.add(lease);
        }
    }

    // ============================================================= Monitor
    /** Checks the oldest leases against the hard limit. Called under the FSNamesystem write lock. */
    synchronized void checkLeases() {
        long now = Time.monotonicNow();
        List<Lease> expired = new ArrayList<>();
        for (Lease l : sortedLeases) {
            if (!l.expiredHardLimit(now)) {
                break;  // sorted: all remaining ones are younger
            }
            expired.add(l);
        }
        for (Lease lease : expired) {
            for (Long id : new ArrayList<>(lease.files)) {
                INodeFile file = fsnamesystem.getBlockCollection(id);
                if (file == null || !file.isUnderConstruction()) {
                    removeLease(lease, id);
                    continue;
                }
                String path = file.getFullPathName();
                Log.info(LOG, lease + " has expired hard limit for " + path);
                try {
                    boolean closed = fsnamesystem.internalReleaseLease(lease, path, file,
                            HdfsServerConstants.NAMENODE_LEASE_HOLDER);
                    if (closed) {
                        Log.info(LOG, "Lease recovery for " + path + " is complete. File closed.");
                    }
                } catch (IOException e) {
                    // As in the original: the lease is removed if it cannot be released
                    Log.warn(LOG, "Cannot release the path " + path + " in the lease " + lease
                            + ". It will be removed: " + e.getMessage());
                    removeLease(lease, id);
                }
            }
        }
    }

    void startMonitor() {
        shouldRunMonitor = true;
        lmthread = new Thread(() -> {
            while (shouldRunMonitor) {
                try {
                    Thread.sleep(HdfsConfig.LEASE_RECHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                fsnamesystem.writeLock();
                try {
                    checkLeases();
                } finally {
                    fsnamesystem.writeUnlock();
                }
            }
        }, "LeaseManager.Monitor");
        lmthread.setDaemon(true);
        lmthread.start();
    }

    void stopMonitor() {
        shouldRunMonitor = false;
        if (lmthread != null) {
            lmthread.interrupt();
        }
    }
}
