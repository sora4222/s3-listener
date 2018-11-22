package com.listener.storable;


import java.io.Closeable;

public interface Storable extends Closeable {
    /**
     * Checks whether the key is in the storable.
     * @param key the key as a string
     * @return returns as a boolean whether the key is within the storable
     */
    public boolean keyAlreadyRead(String key);

    //TODO: put in another keyAlreadyRead that accepts and gives back a batch.

    /**
     * Attempts to put the key in the storable
     * @param key the key as a string
     * @return returns a boolean whether the key was written successfully or not
     */
    public boolean putKey(String key);
}
