package evaluation.general_support.hashmaps;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * This map provides a simple hash-map implementation for mapping primitive integer keys to primitive
 * long integer values. We assume keys are non-negative to keep the implementation simple and efficient.
 */
public class Simple_Int_Long_Map {

    /**
     * The initial capacity with which maps of this type are created.
     * Needs to be power of two, since we need the hash-table length to be a power of 2.
     */
    private static final int INITIAL_CAPACITY = 4;

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
     * The table mapping a hash value to an index in the {@code keys} and {@code values} arrays.
     * The length of this table needs to be a power of 2 for efficient hashing.
     */
    private int[] hashTable;

    /**
     * Array of "next indices" to resolve collisions in the {@code hashTable}.
     * The idea is as follows:
     *  - For a given key, hash(key) produces an index into hashTable
     *  - The value of hashTable[hash(key)] produces an index into the keys/values.
     *  - If for a given key, key[hashTable[hash(key)]] does not equal key, then
     *      next[hashTable[hash(key)]] contains the next index to "probe" in the hash chain to find
     *      the actual key entry (and so on)
     * Thus, when the map is re-hashed, we actually need to produce a new hashTable and a new next
     * array.
     */
    private int[] next;

    /**
     * Creates an empty {@link Simple_Int_Long_Map} instance.
     */
    public Simple_Int_Long_Map() {
        this(INITIAL_CAPACITY);
    }

    /**
     * Creates an empty {@link Simple_Int_Long_Map} instance.
     * @param capacity The capacity to create the map with. (Needs to be a power of 2)
     */
    public Simple_Int_Long_Map(int capacity) {
        if (capacity % 2 != 0)
            throw new IllegalArgumentException("Simple_Int_Long_Map requires its capacity to be a power of 2");

        this.numberOfRecords = 0;

        this.keys = new int[capacity];
        Arrays.fill(this.keys, -1); // Mark unused entries

        this.values = new long[capacity];

        this.hashTable = new int[capacity]; // Hash-table since needs to be a power of two for efficient hashing
        Arrays.fill(this.hashTable, -1); // Mark unused entries

        this.next = new int[capacity];
        Arrays.fill(this.next, -1); // Mark unused entries
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
        int initialIndex = hashTable[hashTableIndex];

        if (initialIndex == -1) { // Hash-table entry is still free, so simply store
            this.hashTable[hashTableIndex] = index;
            return;
        } else if (rehashOnCollision) { // We have a hash-collision and thus need to rebuild
            rehash();
            return;
        }

        // We have a collision but won't rebuild --> follow next pointers until we get an available pointer
        int currentIndex = initialIndex;
        while (keys[currentIndex] != key && next[currentIndex] != -1) {
            currentIndex = next[currentIndex];
        }

        // CurrentIndex has property that next[currentIndex] == -1, so we update it to point to index
        // so the collision list is correct
        next[currentIndex] = index;
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
        int initialIndex = hashTable[hashTableIndex];

        if (initialIndex == -1)  // No hash-table entry implies the key is certainly not in the map.
            return -1;

        int currentIndex = initialIndex;
        while (keys[currentIndex] != key) {
            int potentialNextIndex = next[currentIndex];
            if (potentialNextIndex == -1)   // No next element to search, so the map does not contain the key
                return -1;
            else
                currentIndex = potentialNextIndex;
        }

        return currentIndex;
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

        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1); // Mark unused entries
        this.next = newNext;
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
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1); // Mark unused entries

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
