package edubase.ipc;

import edubase.client.Result;

import java.io.Serializable;
import java.util.List;

/** Corresponds to ScanResponse (ClientProtos.proto): a batch of rows and whether the region has more. */
public record ScanResponse(List<Result> results, boolean moreResultsInRegion) implements Serializable {
}
