package edudfs.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.DatanodeID.
 * Identity of a DataNode: a stable UUID (stored in the storage directory)
 * plus the address for data transfer (xferPort) and RPC between DataNodes (ipcPort).
 */
public class DatanodeID implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String datanodeUuid;
    private String hostName;
    private int xferPort;
    private int ipcPort;

    public DatanodeID(String datanodeUuid, String hostName, int xferPort, int ipcPort) {
        this.datanodeUuid = datanodeUuid;
        this.hostName = hostName;
        this.xferPort = xferPort;
        this.ipcPort = ipcPort;
    }

    public DatanodeID(DatanodeID from) {
        this(from.datanodeUuid, from.hostName, from.xferPort, from.ipcPort);
    }

    public String getDatanodeUuid() {
        return datanodeUuid;
    }

    public String getHostName() {
        return hostName;
    }

    public int getXferPort() {
        return xferPort;
    }

    public int getIpcPort() {
        return ipcPort;
    }

    public String getXferAddr() {
        return hostName + ":" + xferPort;
    }

    /** On re-registration the address may change; the UUID stays the same. */
    public void updateRegInfo(DatanodeID other) {
        this.hostName = other.hostName;
        this.xferPort = other.xferPort;
        this.ipcPort = other.ipcPort;
    }

    @Override
    public String toString() {
        return getXferAddr();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof DatanodeID && datanodeUuid.equals(((DatanodeID) o).datanodeUuid));
    }

    @Override
    public int hashCode() {
        return datanodeUuid.hashCode();
    }
}
