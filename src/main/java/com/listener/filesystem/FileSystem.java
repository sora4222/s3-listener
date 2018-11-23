package com.listener.filesystem;

import java.util.Set;

public interface FileSystem {
    /**
     * Lists the objects
     *
     * @return A set of all the locations in string format
     */
    Set<String> list();

    String getIdentifier();
}
