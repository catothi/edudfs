package edudfs.server.namenode;

import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.ClientProtocol;
import edudfs.server.protocol.DatanodeProtocol;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.NameNode.
 * Starts FSNamesystem + RPC server. The namespace lives only in memory:
 * after a restart it is empty (no FSImage/EditLog in this simplification).
 */
public class NameNode {

    private final int port;
    private final FSNamesystem namesystem = new FSNamesystem();
    private NameNodeRpcServer rpcServer;
    private Registry registry;

    public NameNode(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        System.setProperty("java.rmi.server.hostname", HdfsConfig.NAMENODE_HOST);
        rpcServer = new NameNodeRpcServer(namesystem);
        Remote stub = UnicastRemoteObject.exportObject(rpcServer, 0);
        registry = LocateRegistry.createRegistry(port);
        registry.rebind(ClientProtocol.NAME, stub);
        registry.rebind(DatanodeProtocol.NAME, stub);
        namesystem.startActiveServices();
        Log.info("NameNode", "NameNode RPC up at " + HdfsConfig.NAMENODE_HOST + ":" + port);
    }

    public void stop() {
        namesystem.stopActiveServices();
        try {
            UnicastRemoteObject.unexportObject(rpcServer, true);
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (IOException ignored) {
            // already stopped
        }
    }

    public FSNamesystem getNamesystem() {
        return namesystem;
    }

    public static void main(String[] args) throws Exception {
        NameNode nn = new NameNode(HdfsConfig.NAMENODE_RPC_PORT);
        nn.start();
        Runtime.getRuntime().addShutdownHook(new Thread(nn::stop));
        Thread.currentThread().join();
    }
}
