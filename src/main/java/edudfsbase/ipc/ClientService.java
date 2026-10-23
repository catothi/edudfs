package edudfsbase.ipc;

import edudfsbase.client.Get;
import edudfsbase.client.Mutation;
import edudfsbase.client.Result;
import edudfsbase.client.Scan;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Corresponds to ClientService (ClientProtos.proto): client -&gt; RegionServer, the data operations.
 * The region is addressed by its encoded name. If the RegionServer does not serve it (any more),
 * it throws NotServingRegionException - the client then asks the master again.
 *
 * Original: HBase RPC + protobuf; here Java RMI (as ClientProtocol in EduDFS).
 */
public interface ClientService extends Remote {

    String NAME = "ClientService";

    Result get(String encodedRegionName, Get get) throws IOException;

    void mutate(String encodedRegionName, Mutation mutation) throws IOException;

    /** Opens a scanner on the RegionServer; it keeps its position between the next() calls. */
    long openScanner(String encodedRegionName, Scan scan) throws IOException;

    /** Up to nbRows rows. */
    ScanResponse next(long scannerId, int nbRows) throws IOException;

    void closeScanner(long scannerId) throws IOException;
}
