package edudfs.server.datanode;

import edudfs.common.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.DataXceiverServer:
 * accepts connections on the xferPort and starts a DataXceiver thread for each.
 */
class DataXceiverServer implements Runnable {

    private final DataNode datanode;
    private final ServerSocket serverSocket;
    private final Set<Socket> activeSockets = new HashSet<>();

    DataXceiverServer(DataNode datanode, int port) throws IOException {
        this.datanode = datanode;
        this.serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
    }

    @Override
    public void run() {
        while (datanode.isRunning()) {
            try {
                Socket s = serverSocket.accept();
                synchronized (activeSockets) {
                    activeSockets.add(s);
                }
                Thread t = new Thread(new DataXceiver(datanode, s), "DataXceiver@" + datanode);
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                if (datanode.isRunning()) {
                    Log.warn("DataXceiverServer", datanode + ": " + e.getMessage());
                }
            }
        }
    }

    void remove(Socket s) {
        synchronized (activeSockets) {
            activeSockets.remove(s);
        }
    }

    /** Closes the server connection and all open data connections (also simulates a crash). */
    void kill() {
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // ignore
        }
        List<Socket> sockets;
        synchronized (activeSockets) {
            sockets = new ArrayList<>(activeSockets);
        }
        for (Socket s : sockets) {
            try {
                s.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}
