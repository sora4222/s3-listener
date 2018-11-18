package com.Jesse;

import javax.xml.ws.WebServiceException;
import java.io.IOException;
import java.nio.CharBuffer;

public class FileStorage implements Storable {

    @Override
    public void read(String key) {

    }

    @Override
    public Iterable<String> keysIterable() {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws WebServiceException {

    }
}
