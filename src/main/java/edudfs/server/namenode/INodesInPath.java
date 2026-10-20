package edudfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.INodesInPath.
 *
 * Result of path resolution: for "/a/b/c" the components ["", "a", "b", "c"]
 * and the corresponding INodes [root, a, b, c]. Non-existent components are null.
 */
public class INodesInPath {

    private final String[] components;
    private final INode[] inodes;

    private INodesInPath(String[] components, INode[] inodes) {
        this.components = components;
        this.inodes = inodes;
    }

    static String[] getPathComponents(String path) throws IOException {
        if (path == null || !path.startsWith("/")) {
            throw new IOException("Path must be absolute: " + path);
        }
        List<String> parts = new ArrayList<>();
        parts.add("");  // root
        for (String s : path.split("/")) {
            if (s.isEmpty()) {
                continue;
            }
            if (s.equals(".") || s.equals("..") || s.contains(":")) {
                throw new IOException("Invalid path name " + path);
            }
            parts.add(s);
        }
        return parts.toArray(new String[0]);
    }

    static INodesInPath resolve(INodeDirectory root, String[] components) {
        INode[] inodes = new INode[components.length];
        inodes[0] = root;
        INode cur = root;
        for (int i = 1; i < components.length; i++) {
            cur = (cur != null && cur.isDirectory()) ? cur.asDirectory().getChild(components[i]) : null;
            inodes[i] = cur;
        }
        return new INodesInPath(components, inodes);
    }

    public int length() {
        return inodes.length;
    }

    /** A negative index counts from the end (-1 = last). */
    public INode getINode(int i) {
        return inodes[i >= 0 ? i : inodes.length + i];
    }

    public INode getLastINode() {
        return inodes[inodes.length - 1];
    }

    public String getLastLocalName() {
        return components[components.length - 1];
    }

    public String getPath() {
        return pathOf(components.length);
    }

    public String getParentPath() {
        return pathOf(components.length - 1);
    }

    private String pathOf(int n) {
        if (n <= 1) {
            return "/";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < n; i++) {
            sb.append('/').append(components[i]);
        }
        return sb.toString();
    }
}
