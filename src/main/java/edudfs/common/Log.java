package edudfs.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Minimalist logging (instead of log4j / slf4j). */
public final class Log {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /** With -Dedudfs.quiet=true, INFO messages are suppressed. */
    public static volatile boolean quiet = Boolean.getBoolean("edudfs.quiet");

    private Log() {
    }

    public static synchronized void info(String component, String msg) {
        if (!quiet) {
            System.out.println(LocalTime.now().format(FMT) + " [" + component + "] " + msg);
        }
    }

    public static synchronized void warn(String component, String msg) {
        System.out.println(LocalTime.now().format(FMT) + " [" + component + "] WARN " + msg);
    }
}
