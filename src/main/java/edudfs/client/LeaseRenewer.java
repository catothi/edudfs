package edudfs.client;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.common.Time;

import java.io.IOException;

/**
 * Corresponds to org.apache.hadoop.hdfs.client.impl.LeaseRenewer.
 * Background thread that renews the client's lease every softLimit/2
 * as long as the client has files open for writing.
 */
class LeaseRenewer implements Runnable {

    private final DFSClient client;
    private final long renewalInterval = HdfsConfig.LEASE_SOFTLIMIT_MS / 2;
    private Thread daemon;
    private volatile boolean running;

    LeaseRenewer(DFSClient client) {
        this.client = client;
    }

    synchronized void start() {
        if (daemon == null) {
            running = true;
            daemon = new Thread(this, "LeaseRenewer:" + client.clientName);
            daemon.setDaemon(true);
            daemon.start();
        }
    }

    void stop() {
        Thread t;
        synchronized (this) {
            running = false;
            t = daemon;
            daemon = null;
        }
        if (t != null) {
            t.interrupt();
        }
    }

    @Override
    public void run() {
        long lastRenewed = Time.monotonicNow();
        while (running) {
            try {
                Thread.sleep(Math.min(renewalInterval, 1000));
                if (Time.monotonicNow() - lastRenewed >= renewalInterval) {
                    client.renewLease();
                    lastRenewed = Time.monotonicNow();
                }
            } catch (InterruptedException e) {
                return;
            } catch (IOException e) {
                Log.warn("LeaseRenewer", "Failed to renew lease for " + client.clientName + ": " + e.getMessage());
            }
        }
    }
}
