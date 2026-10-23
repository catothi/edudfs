package edubase;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hbase.ServerName: host,port,startcode,
 * e.g. {@code localhost,16020,1761213640123}.
 *
 * The startcode (start time in ms) distinguishes a restarted RegionServer from its predecessor
 * on the same host and port. That matters: the WALs of the dead predecessor must be split,
 * those of the new one must not.
 */
public final class ServerName implements Serializable, Comparable<ServerName> {

    private static final long serialVersionUID = 1L;

    private final String hostname;
    private final int port;
    private final long startcode;

    public ServerName(String hostname, int port, long startcode) {
        this.hostname = hostname;
        this.port = port;
        this.startcode = startcode;
    }

    public static ServerName valueOf(String s) {
        String[] p = s.split(",");
        return new ServerName(p[0], Integer.parseInt(p[1]), Long.parseLong(p[2]));
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public long getStartcode() {
        return startcode;
    }

    public String getAddress() {
        return hostname + ":" + port;
    }

    @Override
    public String toString() {
        return hostname + "," + port + "," + startcode;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ServerName s && s.toString().equals(toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public int compareTo(ServerName o) {
        return toString().compareTo(o.toString());
    }
}
