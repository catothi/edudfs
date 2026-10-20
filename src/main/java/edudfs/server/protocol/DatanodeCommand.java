package edudfs.server.protocol;

import java.io.Serializable;

/** Corresponds to org.apache.hadoop.hdfs.server.protocol.DatanodeCommand. */
public class DatanodeCommand implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int action;

    public DatanodeCommand(int action) {
        this.action = action;
    }

    /** One of the DNA_* codes from {@link DatanodeProtocol}. */
    public int getAction() {
        return action;
    }
}
