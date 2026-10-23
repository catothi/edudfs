package edubase;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hbase.TableName: namespace + table name ("ns:t").
 * Without a namespace the table lives in the namespace "default", in HDFS under
 * /hbase/data/default/&lt;table&gt;. EduBase supports only the namespace "default".
 */
public final class TableName implements Serializable, Comparable<TableName> {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_NAMESPACE_NAME = "default";
    /** The catalog table. In EduBase it is not a real table (see HMaster). */
    public static final TableName META_TABLE_NAME = new TableName("hbase", "meta");

    private final String namespace;
    private final String qualifier;

    private TableName(String namespace, String qualifier) {
        this.namespace = namespace;
        this.qualifier = qualifier;
    }

    public static TableName valueOf(String name) {
        int i = name.indexOf(':');
        if (i < 0) {
            return new TableName(DEFAULT_NAMESPACE_NAME, check(name));
        }
        String ns = name.substring(0, i);
        if (ns.equals("hbase") && name.substring(i + 1).equals("meta")) {
            return META_TABLE_NAME;
        }
        if (!ns.equals(DEFAULT_NAMESPACE_NAME)) {
            throw new IllegalArgumentException("EduBase supports only the namespace 'default': " + name);
        }
        return new TableName(ns, check(name.substring(i + 1)));
    }

    private static String check(String q) {
        if (!q.matches("[a-zA-Z0-9_][a-zA-Z0-9_.-]*")) {
            throw new IllegalArgumentException("Illegal table name: '" + q + "' (allowed: [a-zA-Z0-9_.-])");
        }
        return q;
    }

    public String getNamespaceAsString() {
        return namespace;
    }

    public String getQualifierAsString() {
        return qualifier;
    }

    public String getNameAsString() {
        return namespace.equals(DEFAULT_NAMESPACE_NAME) ? qualifier : namespace + ":" + qualifier;
    }

    @Override
    public String toString() {
        return getNameAsString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TableName t && t.namespace.equals(namespace) && t.qualifier.equals(qualifier);
    }

    @Override
    public int hashCode() {
        return namespace.hashCode() * 31 + qualifier.hashCode();
    }

    @Override
    public int compareTo(TableName o) {
        return getNameAsString().compareTo(o.getNameAsString());
    }
}
