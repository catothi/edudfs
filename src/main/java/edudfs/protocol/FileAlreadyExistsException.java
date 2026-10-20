package edudfs.protocol;

import java.io.IOException;

/** The file already exists. (Name as in the original HDFS.) */
public class FileAlreadyExistsException extends IOException {
    private static final long serialVersionUID = 1L;

    public FileAlreadyExistsException(String msg) {
        super(msg);
    }
}
