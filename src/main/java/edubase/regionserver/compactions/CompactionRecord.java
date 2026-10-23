package edubase.regionserver.compactions;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * EduBase only: the protocol of a finished compaction - which files went in, which came out,
 * and how many cells were dropped for which reason. Shown by the shell after
 * {@code compact}/{@code major_compact} and with {@code compaction_history}.
 * (The original only logs this: "Completed compaction of 3 file(s) in cf of t,,1761...".)
 */
public class CompactionRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** An input or output file. */
    public record FileSummary(String name, long size, long entries, String createdBy) implements Serializable {
    }

    private final String region;
    private final String family;
    private final String reason;
    boolean major;
    boolean allFiles;
    long startTime;
    long durationMs;
    final List<FileSummary> inputs = new ArrayList<>();
    FileSummary output;
    long droppedVersions;
    long droppedDeletedCells;
    long droppedDeleteMarkers;
    long droppedExpired;
    int remainingFiles;

    public CompactionRecord(String region, String family, String reason) {
        this.region = region;
        this.family = family;
        this.reason = reason;
    }

    public String getRegion() {
        return region;
    }

    public String getFamily() {
        return family;
    }

    public boolean isMajor() {
        return major;
    }

    public List<FileSummary> getInputs() {
        return inputs;
    }

    public FileSummary getOutput() {
        return output;
    }

    public long getDroppedVersions() {
        return droppedVersions;
    }

    public long getDroppedDeletedCells() {
        return droppedDeletedCells;
    }

    public long getDroppedDeleteMarkers() {
        return droppedDeleteMarkers;
    }

    public long getDroppedExpired() {
        return droppedExpired;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s %s compaction of %s, family %s (%s)%n",
                new SimpleDateFormat("HH:mm:ss").format(new Date(startTime)), major ? "MAJOR" : "minor", region,
                family, reason));
        sb.append(String.format("  %-34s %8s %8s  %s%n", "input HFile", "bytes", "cells", "created by"));
        long inCells = 0;
        for (FileSummary f : inputs) {
            sb.append(String.format("  %-34s %8d %8d  %s%n", f.name(), f.size(), f.entries(), f.createdBy()));
            inCells += f.entries();
        }
        sb.append(String.format("  -> %-31s %8d %8d%n", output.name(), output.size(), output.entries()));
        sb.append(String.format("  cells: %d in, %d out; dropped: %d old versions, %d deleted cells, "
                        + "%d delete markers, %d expired (TTL)%n", inCells, output.entries(), droppedVersions,
                droppedDeletedCells, droppedDeleteMarkers, droppedExpired));
        sb.append(String.format("  delete markers %s (%s), %d HFile(s) in the store afterwards, took %d ms%n",
                allFiles ? "DROPPED" : "KEPT", allFiles ? "all files of the store took part"
                        : "not all files took part - an older file may still hold covered cells",
                remainingFiles, durationMs));
        return sb.toString();
    }
}
