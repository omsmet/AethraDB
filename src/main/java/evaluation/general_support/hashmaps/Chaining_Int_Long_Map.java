package evaluation.general_support.hashmaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * TODO: update once tested
 * This map provides a simple hash-map implementation for mapping primitive integer keys to primitive
 * long integer values. We assume keys are non-negative to keep the implementation simple and efficient.
 */
public class Chaining_Int_Long_Map {

    /**
     * The initial capacity with which maps of this type are created.
     * Needs to be power of two, since we need the hash-table length to be a power of 2.
     */
    private static final int INITIAL_CAPACITY = 4;

    /**
     * The initial length that collision chains get in the hash table. Needs to be a multiple of two.
     */
    private static final int INITIAL_COLLISION_CHAIN_LENGTH = 4;

    /**
     * Variable indicating the current number of records in the map.
     */
    public int numberOfRecords;

    /**
     * Array containing the key of each element in the map (in insertion order).
     */
    public int[] keys;

    /**
     * Array containing the values of each element in the map (in insertion order).
     */
    private long[] values;

    /**
     * The hash table maps a hash value to an array of (key, index) pairs, where 'index' is the index
     * for 'key' in the keys and values array. Collisions are chained, which is why  the entries in
     * the table are integer are arrays whose length is a multiple of two where even indices store
     * the keys and the odd indices store the indices of the pairs.
     */
    private int[][] hashTable;

    /**
     * Creates an empty {@link Chaining_Int_Long_Map} instance.
     */
    public Chaining_Int_Long_Map() {
        this(INITIAL_CAPACITY);
    }

    /**
     * Creates an empty {@link Chaining_Int_Long_Map} instance.
     * @param capacity The capacity to create the map with. (Needs to be a power of 2)
     */
    public Chaining_Int_Long_Map(int capacity) {
        if (capacity % 2 != 0)
            throw new IllegalArgumentException("Simple_Int_Long_Map requires its capacity to be a power of 2");

        this.numberOfRecords = 0;

        this.keys = new int[capacity];
        Arrays.fill(this.keys, -1); // Mark unused entries

        this.values = new long[capacity];

        // Hash-table since needs to be a power of two for efficient hashing
        this.hashTable = new int[capacity][];
        int[] minusOneArray = new int[INITIAL_COLLISION_CHAIN_LENGTH];
        Arrays.fill(minusOneArray, -1);
        Arrays.fill(this.hashTable, minusOneArray);
    }

    /**
     * Method for computing the hash of an integer key from its pre-hash.
     * @param preHash The pre-hash value for which to compute the hash value.
     * @return The value {@code preHash mod hashLength}.
     */
    private int hash(long preHash) {
        // Hash-table length is a power of two --> Allows optimisation of modulo operator into binary and
        return (int) (preHash & (this.hashTable.length - 1));
    }

    /**
     * Method to associate a given value to a key. Replaces the old value of a key if the map
     * already contained a value for key.
     * @param key The key to which to associate a value.
     * @param preHash The pre-hash value of the key.
     * @param value The value to associate to the key.
     */
    public void put(int key, long preHash, long value) {
        checkKeyNonNegative(key);

        // Check if the map already contains the key
        int index = find(key, preHash);
        if (index != -1) { // Need to update an existing index
            values[index] = value;
        }

        // Need to put a new key into the map
        int newIndex = this.numberOfRecords++;
        if (this.keys.length == newIndex) {
            // We need to grow the keys, values and next arrays first
            growArrays();
            assert this.keys.length > newIndex;
        }

        // Store the new key-value association
        keys[newIndex] = key;
        values[newIndex] = value;

        // Need to store the hash-table entry for the key to nextIndex
        // We rehash on collision only if the hash-table is approximately 3/4 full.
        boolean rehashOnCollision = this.numberOfRecords > (3 * this.hashTable.length) / 4;
        putHashEntry(key, preHash, newIndex, rehashOnCollision);
    }

    /**
     * Method to put a certain key into the {@code hashTable} and have it point to a given index
     * in the {@code keys} and {@code values} arrays (or chain it via the {@code next} array.
     * @param key The key to insert into the {@code hashTable}.
     * @param preHash The pre-hash value of the key to insert into the {@code hashTable}.
     * @param index The index of the {@code keys} and {@code values} arrays to associate in the {@code hashTable}.
     * @param rehashOnCollision Whether to rebuild the hash-table into a larger table on a collision.
     */
    private void putHashEntry(int key, long preHash, int index, boolean rehashOnCollision) {
        int hashTableIndex = hash(preHash);
        int[] initialIndex = hashTable[hashTableIndex];

        if (initialIndex == null) {
            // Hash-table entry is still free, so create a new chain
            int[] newChain = new int[INITIAL_COLLISION_CHAIN_LENGTH];
            Arrays.fill(newChain, -1);
            newChain[0] = key;
            newChain[1] = index;
            this.hashTable[hashTableIndex] = newChain;
            return;
        } else if (rehashOnCollision) {
            // We have a hash-collision and thus need to rebuild
            rehash();
            return;
        }

        // We have a collision but won't rebuild --> add to the collision chain
        int[] collisionChain = this.hashTable[hashTableIndex];
        int collisionChainLength = collisionChain.length;
        for (int i = 0; i < collisionChainLength; i += 2) {
            // Look for an empty entry
            if (collisionChain[i] == -1) {
                // Store once you found one
                collisionChain[i] = key;
                collisionChain[i + 1] = index;
                this.hashTable[hashTableIndex] = collisionChain;
                return;
            }
        }

        // The current collision chain is full, so we need to extend it
        int[] newChain = new int[collisionChainLength * 2];
        Arrays.fill(newChain, -1);
        System.arraycopy(collisionChain, 0, newChain, 0, collisionChainLength);
        newChain[collisionChainLength] = key;
        newChain[collisionChainLength + 1] = index;
        this.hashTable[hashTableIndex] = newChain;
    }

    /**
     * Method to increment the value associated to a given key by a given value if the key is already
     * in the map, or to associate the given value to the key otherwise.
     * @param key The key to perform the operation for.
     * @param preHash The pre-hash value of the key to perform the operation for.
     * @param value The value to add/associate to the key.
     */
    public void addToKeyOrPutIfNotExist(int key, long preHash, long value) {
        checkKeyNonNegative(key);

        int index = find(key, preHash);
        if (index == -1)
            put(key, preHash, value);
        else
            values[index] += value;
    }

    /**
     * Method to obtain the value associated to a given key.
     * @param key The key to find the value for.
     * @param preHash The pre-hash value of the key to find the value for.
     * @return The value associated to the key (if the map contains the key).
     * @throws NoSuchElementException if the map does not contain the provided key.
     */
    public long get(int key, long preHash) throws NoSuchElementException {
        checkKeyNonNegative(key);
        int index = find(key, preHash);

        if (index != -1)
            return values[index];

        throw new NoSuchElementException("Simple_Int_Long_Map does not contain key " + key);
    }

    /**
     * Method to check whether the map contains a given key.
     * @param key The key to check.
     * @param preHash The pre-hash value belonging to the key to check.
     * @return {@code true} iff the map contains the given key.
     */
    public boolean contains(int key, long preHash) {
        checkKeyNonNegative(key);
        return find(key, preHash) != -1;
    }

    /**
     * Finds the index into the {@code keys} and {@code values} arrays for a given key if the key
     * exists in the map, or returns -1 otherwise.
     * @param key A non-negative key to find the index for.
     * @param preHash The pre-hash value of the key to find the index for.
     * @return The index into {@code keys} and {@code values} if the key exists in the map, -1 otherwise.
     */
    private int find(int key, long preHash) {
        int hashTableIndex = hash(preHash);
        int[] collisionChain = hashTable[hashTableIndex];

        if (collisionChain == null) // No hash-table entry implies the key is certainly not in the map.
            return -1;

        for (int i = 0; i < collisionChain.length; i += 2) {
            int keyAtIndex = collisionChain[i];
            if (keyAtIndex == key)
                return collisionChain[i + 1];
            else if (keyAtIndex == - 1)
                return -1;
        }

        // Should never arrive here but return statement is required
        return -1;
    }

    /**
     * Method to grow the {@code keys}, {@code values} and {@code next} arrays to a larger size to
     * support more elements.
     */
    private void growArrays() {
        int currentSize = this.keys.length;
        int newSize = currentSize + Math.max(INITIAL_CAPACITY, currentSize >> 1);
        if (newSize > Integer.MAX_VALUE - 1)
            throw new UnsupportedOperationException("Simple_Int_Long_Map has grown too large");

        int[] newKeys = new int[newSize];
        System.arraycopy(this.keys, 0, newKeys, 0, currentSize);
        Arrays.fill(newKeys, currentSize, newSize, -1); // Mark unused entries
        this.keys = newKeys;

        long[] newValues = new long[newSize];
        System.arraycopy(this.values, 0, newValues, 0, currentSize);
        this.values = newValues;
    }

    /**
     * Method which constructs a completely new hashTable for the current {@code keys} and {@code values}.
     */
    private void rehash() {
        // Compute the new hash-table size as the smallest power of 2 which is greater than this.numberOfRecords
        // This satisfies the requirement that the hash-table size is a power of two
        int size = this.hashTable.length;
        while (size <= this.numberOfRecords)
            size <<= 1;

        // Add some additional size to prevent collisions (multiply by two to keep the hash-table size a power of two)
        size <<= 1;

        // Create the new hashTable and reset the next array
        this.hashTable = new int[size][];
        int[] minusOneArray = new int[INITIAL_COLLISION_CHAIN_LENGTH];
        Arrays.fill(minusOneArray, -1);
        Arrays.fill(this.hashTable, minusOneArray);

        // And insert all key-value associations again
        for (int i = 0; i < this.numberOfRecords; i++) {
            long preHashValue = Int_Hash_Function.preHash(this.keys[i]);
            putHashEntry(this.keys[i], preHashValue, i, false);
        }
    }

    /**
     * Method which simply checks whether the supplied key is non-negative.
     * @param key The key to check.
     * @throws IllegalArgumentException if the supplied key is negative.
     */
    private void checkKeyNonNegative(int key) throws IllegalArgumentException {
        if (key < 0)
            throw new IllegalArgumentException("Simple_Int_Long_Map does not support negative keys");
    }

}
