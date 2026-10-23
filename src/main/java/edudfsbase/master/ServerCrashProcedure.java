package edudfsbase.master;

import edudfsbase.HConstants;
import edudfsbase.ServerName;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfsbase.wal.WALSplitter;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Corresponds to org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure: what the master does
 * when a RegionServer has died. Steps (states) as in the original:
 * <ol>
 *   <li>SERVER_CRASH_START: which regions were open on the server? -&gt; ABNORMALLY_CLOSED</li>
 *   <li>SERVER_CRASH_SPLIT_LOGS (SplitWALManager): rename WALs/&lt;server&gt; to WALs/&lt;server&gt;-splitting,
 *       per WAL file: recover the lease (closes the file, determines its length via block recovery),
 *       WALSplitter writes recovered.edits per region, the WAL goes to oldWALs</li>
 *   <li>SERVER_CRASH_ASSIGN: open the regions on other RegionServers - they replay
 *       recovered.edits and are complete again</li>
 *   <li>SERVER_CRASH_FINISH</li>
 * </ol>
 * The regions are unavailable from the crash until the end of step 3 (MTTR: detection by session
 * timeout + splitting + opening).
 */
public class ServerCrashProcedure implements Runnable {

    private static final String LOG = "ServerCrashProcedure";
    private static final AtomicLong PID = new AtomicLong(1);

    private final long pid = PID.getAndIncrement();
    private final HMaster master;
    private final ServerName serverName;
    private final List<String> steps = Collections.synchronizedList(new ArrayList<>());
    private volatile String state = "RUNNABLE";

    ServerCrashProcedure(HMaster master, ServerName serverName) {
        this.master = master;
        this.serverName = serverName;
    }

    private void step(String s) {
        steps.add(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " " + s);
        Log.info(LOG, "pid=" + pid + " " + s);
    }

    @Override
    public void run() {
        try {
            AssignmentManager am = master.getAssignmentManager();
            List<RegionState> regions = am.getRegionsOnServer(serverName);
            step("SERVER_CRASH_START server=" + serverName + ", " + regions.size() + " region(s) affected");
            for (RegionState rs : regions) {
                am.markAbnormallyClosed(rs);
            }
            step("SERVER_CRASH_SPLIT_LOGS");
            splitLogs(master.getFileSystem(), serverName, this::step);
            step("SERVER_CRASH_ASSIGN " + regions.size() + " region(s)");
            for (RegionState rs : regions) {
                am.markOffline(rs);
                if (am.assign(rs)) {
                    step("  " + rs.getRegion().getShortName() + " -> " + rs.getServerName());
                }
            }
            step("SERVER_CRASH_FINISH");
            state = "SUCCESS";
        } catch (IOException | RuntimeException e) {
            state = "FAILED: " + e.getMessage();
            Log.warn(LOG, "pid=" + pid + " failed: " + e);
        }
    }

    /**
     * Corresponds to SplitWALManager / MasterWalManager.splitLog: splits all WALs of a dead server.
     * Also used at master startup for WAL directories of servers that are no longer alive.
     */
    static void splitLogs(HFileSystem fs, ServerName sn, Consumer<String> log) throws IOException {
        String dir = CommonFSUtils.getWALDirectoryName(sn);
        String splitting = dir + HConstants.SPLITTING_EXT;
        if (fs.exists(dir)) {
            if (!fs.rename(dir, splitting)) {
                throw new IOException("Failed rename of " + dir + " to " + splitting);
            }
            log.accept("  renamed " + CommonFSUtils.getName(dir) + " -> " + CommonFSUtils.getName(splitting));
        }
        HdfsFileStatus[] files = fs.listStatus(splitting);
        for (HdfsFileStatus st : files) {
            String wal = st.getPath();
            if (st.isUnderConstruction()) {
                log.accept("  recoverLease " + CommonFSUtils.getName(wal)
                        + " (still open: the dead server never closed it)");
            }
            fs.recoverFileLease(wal, 60_000);
            WALSplitter.SplitResult r = WALSplitter.splitLogFile(fs, wal);
            log.accept("  split " + r.wal() + ": " + r.entries() + " entries -> recovered.edits of "
                    + r.editsPerRegion().size() + " region(s) " + r.editsPerRegion()
                    + (r.truncated() ? " (last entry was cut off by the crash and ignored)" : ""));
            fs.rename(wal, CommonFSUtils.getOldWALDir() + "/" + CommonFSUtils.getName(wal));
        }
        fs.delete(splitting, true);
    }

    public long getPid() {
        return pid;
    }

    public ServerName getServerName() {
        return serverName;
    }

    public String getState() {
        return state;
    }

    public List<String> getSteps() {
        synchronized (steps) {
            return new ArrayList<>(steps);
        }
    }
}
