package store;

import java.io.IOException;

/**
 * KV interface defining basic operations for a key-value store.
 */

public interface KV {
    /**
     * Stores a value associated with the given key.
     * @param key the key to store the value under
     * @param value the value to store
     * 
     * @throws IOException if an I/O error occurs
     */
    void put(String key, byte[] value) throws IOException;

  /**
     * Retrieves the value associated with the given key.
     * @param key the key whose associated value is to be returned
     * 
     * @return the value associated with the key, or null if not found
     * @throws IOException if an I/O error occurs
     */
    byte[] get(String key) throws IOException;

    /**
     * Deletes the value associated with the given key.
     * @param key the key whose associated value is to be deleted
     * 
     * @throws IOException if an I/O error occurs
     */
    void delete(String key) throws IOException;
}