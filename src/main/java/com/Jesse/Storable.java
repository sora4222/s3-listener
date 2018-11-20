package com.Jesse;

import com.sun.xml.internal.ws.Closeable;
import java.io.FilterWriter
import java.io.Flushable;

public interface Storable extends Closeable, Flushable, AutoCloseable {
    /**
     * Checks whether the key is in the storable.
     * @param key the key as a string
     * @return returns as a boolean whether the key is within the storable
     */
    public boolean keyAlreadyRead(String key);

    /**
     * Attempts to put the key in the storable
     * @param key the key as a string
     * @return returns a boolean whether the key was written successfully or not
     */
    public boolean putKey(String key);
    public Iterable<String> keysIterable();
    public void flush();
}
