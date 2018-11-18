package com.Jesse;

import com.sun.xml.internal.ws.Closeable;
import java.io.FilterWriter
import java.io.Flushable;

public interface Storable extends Closeable, Flushable, AutoCloseable {
    public void read(String key);
    public Iterable<String> keysIterable();
    public void flush();
}
