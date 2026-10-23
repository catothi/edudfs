package edudfsbase.ipc;

import edudfsbase.RegionInfo;
import edudfsbase.client.TableDescriptor;
import edudfsbase.regionserver.compactions.CompactionRecord;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;

/**
 * Corresponds to AdminService (AdminProtos.proto): master or admin client -&gt; RegionServer.
 * Open/close regions (assignment), flush and compaction on request.
 *
 * Simplification: in the original, openRegion only queues the work; the RegionServer reports
 * the result to the master asynchronously (reportRegionStateTransition). Here the call blocks
 * until the region is open.
 */
public interface AdminService extends Remote {

    String NAME = "AdminService";

    /** Returns the sequence ID with which the region was opened. */
    long openRegion(RegionInfo region, TableDescriptor htd) throws IOException;

    void closeRegion(String encodedRegionName) throws IOException;

    /** Returns a short report of the HFiles written. */
    String flushRegion(String encodedRegionName) throws IOException;

    /** Synchronous here (original: queued); returns the protocols of the compactions. */
    List<CompactionRecord> compactRegion(String encodedRegionName, boolean major) throws IOException;

    List<RegionInfo> getOnlineRegions() throws IOException;

    /** EduDFSBase: the state of the RegionServer (regions, stores, MemStores, HFiles, WAL, BlockCache). */
    String metaSave() throws IOException;

    /** EduDFSBase: the last compactions of this RegionServer. */
    List<CompactionRecord> getCompactionHistory() throws IOException;
}
