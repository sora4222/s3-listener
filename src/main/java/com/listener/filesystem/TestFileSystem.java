package com.listener.filesystem;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TestFileSystem implements FileSystem {

    private Logger logger = LoggerFactory.getLogger(TestFileSystem.class.getName());

    private Set<String> listSet;

    /**
     * A test file system.
     * The hash set used is the {@link LinkedHashSet} as this
     * set retains the order in which the objects are put in
     * this gives the tester an idea of when these will be
     * effected.
     */
    public TestFileSystem() {
        // Linked hashset is used to retain the order in which the
        // use
        listSet = new LinkedHashSet<>();
    }

    public TestFileSystem(List<String> list) {
        listSet = new HashSet<>(list);
    }

    /**
     * Adds a file string to the set of files that this
     * TestFileSystem will list to the user.
     *
     * @param file The string location for the file this
     *             should be almost a URI.
     */
    public void addFile(String file) {
        logger.trace("Adding file {}, which is contained:",
                file, listSet.contains(file));
        listSet.add(file);
    }

    /**
     * Returns the {@link HashSet} of the file locations in this file
     * system
     *
     * @return A set of all the locations in string format
     */
    @Override
    public Set<String> list() {
        logger.trace("Listing set");
        return listSet;
    }

    @Override
    public String getIdentifier() {
        return "Test file system";
    }

}
