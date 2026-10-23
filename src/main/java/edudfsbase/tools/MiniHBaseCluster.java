package edudfsbase.tools;

import edudfsbase.HBaseConfiguration;
import edudfsbase.master.HMaster;
import edudfsbase.regionserver.HRegionServer;
import edudfs.common.HdfsConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds (heavily simplified) to org.apache.hadoop.hbase.MiniHBaseCluster: HMaster + n
 * RegionServers in ONE JVM. They still talk to each other over RMI on localhost, and they store
 * everything in EduDFS - which must already be running (./hdfs minicluster).
 *
 * Ports: master 16000, RegionServer i: 16020 + i.
 */
public class MiniHBaseCluster implements Closeable {

    private HMaster master;
    private final List<HRegionServer> regionServers = new ArrayList<>();

    public MiniHBaseCluster(int numRegionServers) throws IOException {
        startMaster();
        for (int i = 0; i < numRegionServers; i++) {
            regionServers.add(null);
            startRegionServer(i);
        }
    }

    public void startMaster() throws IOException {
        master = new HMaster(HBaseConfiguration.MASTER_PORT);
        master.start();
    }

    public void stopMaster() {
        master.stop();
    }

    public HMaster getMaster() {
        return master;
    }

    public HRegionServer startRegionServer(int i) throws IOException {
        HRegionServer rs = new HRegionServer(HBaseConfiguration.REGIONSERVER_PORT + i);
        rs.start();
        regionServers.set(i, rs);
        return rs;
    }

    public HRegionServer getRegionServer(int i) {
        return regionServers.get(i);
    }

    public int getNumRegionServers() {
        return regionServers.size();
    }

    /** Crash: no flush, WAL stays open - the master has to recover the regions. */
    public void killRegionServer(int i) {
        regionServers.get(i).abort("killed by MiniHBaseCluster");
    }

    /** Clean shutdown of a RegionServer. */
    public void stopRegionServer(int i) {
        regionServers.get(i).stop("stopped by MiniHBaseCluster");
    }

    /** Waits until the master is initialized and all regions are open. */
    public void waitUntilAllRegionsAssigned(long timeoutMs) throws InterruptedException, IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!master.isInitialized() || master.getNumberOfOpenRegions() < master.getNumberOfRegions()) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Timeout waiting for regions to be assigned (" + master.getNumberOfOpenRegions()
                        + " of " + master.getNumberOfRegions() + " open)");
            }
            Thread.sleep(100);
        }
    }

    public void waitForActiveAndReadyMaster(long timeoutMs) throws InterruptedException, IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!master.isInitialized()) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Timeout waiting for the master to initialize");
            }
            Thread.sleep(100);
        }
    }

    /** Clean shutdown: RegionServers flush their MemStores, then the master stops. */
    @Override
    public void close() {
        for (HRegionServer rs : regionServers) {
            if (rs != null) {
                rs.stop("cluster shutdown");
            }
        }
        master.stop();
    }

    /** Starts master + n RegionServers and keeps them running (EduDFS must be running). */
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 2;
        MiniHBaseCluster cluster = new MiniHBaseCluster(n);
        cluster.waitForActiveAndReadyMaster(60_000);
        System.out.println("\nMiniHBaseCluster running: HMaster localhost:" + HBaseConfiguration.MASTER_PORT + ", "
                + n + " RegionServers (ports " + HBaseConfiguration.REGIONSERVER_PORT + ".."
                + (HBaseConfiguration.REGIONSERVER_PORT + n - 1) + "), hbase.rootdir=hdfs://" + HdfsConfig.NAMENODE_HOST
                + ":" + HdfsConfig.NAMENODE_RPC_PORT + HBaseConfiguration.ROOT_DIR
                + "\nShell: ./hbase shell      Stop with Ctrl+C (RegionServers flush their MemStores first)\n");
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));
        Thread.currentThread().join();
    }
}
