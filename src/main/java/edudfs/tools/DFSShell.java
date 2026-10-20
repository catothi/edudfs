package edudfs.tools;

import edudfs.client.DFSClient;
import edudfs.client.DFSInputStream;
import edudfs.client.DFSOutputStream;
import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.HdfsFileStatus;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Corresponds (in excerpts) to 'hdfs dfs' (FsShell) plus some admin/debug commands.
 */
public class DFSShell {

    private static final String USAGE = String.join("\n",
            "Usage: DFSShell <command> [args]",
            "  -ls [-R] <path>                list directory",
            "  -mkdir <path>                  create directory/directories",
            "  -put [-f] [-repl n] [-bs bytes] <localSrc> <dst>   local -> HDFS (-f: overwrite)",
            "  -copyFromLocal ...             same as -put",
            "  -get <src> <localDst>          HDFS -> local",
            "  -copyToLocal <src> <localDst>  same as -get",
            "  -appendToFile <localSrc>... <dst>  append local file(s) ('-' = stdin)",
            "  -cat <src>",
            "  -touchz <path>                 create empty file",
            "  -rm [-r] [-skipTrash] <path>   delete; '/*' (in quotes) = everything under /",
            "  -mv <src> <dst>",
            "  -recoverLease <path>           force lease recovery (hdfs debug recoverLease)",
            "  -metasave                      show the NameNode's in-memory structures",
            "  -report                        show DataNodes (hdfs dfsadmin -report)",
            "  -writeAndCrash <path> <bytes>  writes data and then simulates a client crash");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println(USAGE);
            System.exit(1);
        }
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        DFSClient client = new DFSClient();
        int rc = 0;
        try {
            rc = run(client, args);
        } catch (IOException e) {
            System.err.println(args[0].substring(1) + ": " + e.getMessage());
            rc = 1;
        } finally {
            if (!args[0].equals("-writeAndCrash")) {
                client.close();
            }
        }
        System.exit(rc);
    }

    static int run(DFSClient client, String[] a) throws IOException {
        switch (a[0]) {
            case "-ls": {
                boolean recursive = a.length > 2 && a[1].equals("-R");
                ls(client, a[a.length - 1], recursive);
                return 0;
            }
            case "-mkdir":
                client.mkdirs(a[1]);
                return 0;
            case "-put":
            case "-copyFromLocal": {
                short repl = HdfsConfig.DEFAULT_REPLICATION;
                long bs = HdfsConfig.DEFAULT_BLOCK_SIZE;
                boolean overwrite = false;
                int i = 1;
                while (a[i].startsWith("-")) {
                    if (a[i].equals("-f")) {
                        overwrite = true;
                        i++;
                        continue;
                    }
                    if (a[i].equals("-repl")) {
                        repl = Short.parseShort(a[i + 1]);
                    } else if (a[i].equals("-bs")) {
                        bs = Long.parseLong(a[i + 1]);
                    }
                    i += 2;
                }
                try (InputStream in = new FileInputStream(a[i]);
                     DFSOutputStream out = client.create(a[i + 1], overwrite, repl, bs)) {
                    in.transferTo(out);
                }
                return 0;
            }
            case "-appendToFile": {
                String dst = a[a.length - 1];
                try (DFSOutputStream out = client.append(dst)) {
                    for (int i = 1; i < a.length - 1; i++) {
                        if (a[i].equals("-")) {
                            System.in.transferTo(out);
                        } else {
                            try (InputStream in = new FileInputStream(a[i])) {
                                in.transferTo(out);
                            }
                        }
                    }
                }
                return 0;
            }
            case "-get":
            case "-copyToLocal":
                try (DFSInputStream in = client.open(a[1]); OutputStream out = new FileOutputStream(a[2])) {
                    in.transferTo(out);
                }
                return 0;
            case "-cat":
                try (DFSInputStream in = client.open(a[1])) {
                    in.transferTo(System.out);
                    System.out.flush();
                }
                return 0;
            case "-touchz":
                client.create(a[1], false).close();
                return 0;
            case "-rm": {
                boolean recursive = false;
                for (int i = 1; i < a.length - 1; i++) {
                    if (a[i].equals("-r") || a[i].equals("-R")) {
                        recursive = true;
                    }
                    // -skipTrash: accepted, there is no trash here
                }
                int rc = 0;
                for (String path : expandGlob(client, a[a.length - 1])) {
                    if (!client.delete(path, recursive)) {
                        System.err.println("rm: `" + path + "': No such file or directory");
                        rc = 1;
                    } else {
                        System.out.println("Deleted " + path);
                    }
                }
                return rc;
            }
            case "-mv":
                if (!client.rename(a[1], a[2])) {
                    System.err.println("mv: rename " + a[1] + " -> " + a[2] + " failed");
                    return 1;
                }
                return 0;
            case "-recoverLease": {
                boolean closed = client.recoverLease(a[1]);
                System.out.println(closed ? "recoverLease SUCCEEDED on " + a[1]
                        : "recoverLease returned false (block recovery in progress) - try again later");
                return 0;
            }
            case "-metasave":
                System.out.print(client.metaSave());
                return 0;
            case "-report":
                for (DatanodeInfo dn : client.datanodeReport()) {
                    System.out.printf("%-18s %-5s blocks=%-4d used=%d bytes  remaining=%d MB  uuid=%s%n",
                            dn.getXferAddr(), dn.isAlive() ? "LIVE" : "DEAD", dn.getNumBlocks(), dn.getDfsUsed(),
                            dn.getRemaining() >> 20, dn.getDatanodeUuid());
                }
                return 0;
            case "-writeAndCrash": {
                DFSOutputStream out = client.create(a[1], false);
                byte[] data = new byte[Integer.parseInt(a[2])];
                new Random().nextBytes(data);
                out.write(data);
                try {
                    Thread.sleep(1000);  // give the pipeline time to send the full packets
                } catch (InterruptedException ignored) {
                    // doesn't matter
                }
                client.abort();
                System.out.println("Client " + client.getClientName() + " 'crashed'. The file stays open until"
                        + " the lease expires (soft " + HdfsConfig.LEASE_SOFTLIMIT_MS / 1000 + " s / hard "
                        + HdfsConfig.LEASE_HARDLIMIT_MS / 1000 + " s). Observe with -metasave.");
                return 0;
            }
            default:
                System.err.println(USAGE);
                return 1;
        }
    }

    /**
     * Minimal replacement for FsShell globbing: only '*' in the last path component,
     * e.g. "/*" or "/user/me/*.txt". Without '*' the path is returned unchanged.
     */
    private static List<String> expandGlob(DFSClient client, String path) throws IOException {
        int slash = path.lastIndexOf('/');
        String pattern = path.substring(slash + 1);
        if (!pattern.contains("*")) {
            return List.of(path);
        }
        String parent = slash <= 0 ? "/" : path.substring(0, slash);
        String regex = ("\\Q" + pattern + "\\E").replace("*", "\\E.*\\Q");
        List<String> result = new ArrayList<>();
        for (HdfsFileStatus st : client.listPaths(parent)) {
            String name = st.getPath().substring(st.getPath().lastIndexOf('/') + 1);
            if (name.matches(regex)) {
                result.add(st.getPath());
            }
        }
        return result;
    }

    private static void ls(DFSClient client, String path, boolean recursive) throws IOException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        for (HdfsFileStatus st : client.listPaths(path)) {
            System.out.printf("%s %3s %10d %s %s%s%n", st.isDir() ? "d" : "-",
                    st.isDir() ? "-" : String.valueOf(st.getReplication()), st.getLen(),
                    fmt.format(new Date(st.getModificationTime())), st.getPath(),
                    st.isUnderConstruction() ? "   (under construction)" : "");
            if (recursive && st.isDir()) {
                ls(client, st.getPath(), true);
            }
        }
    }
}
