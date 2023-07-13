package benchmarks.aggregation_query_hard_coded.VectorisedSupport;

import evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class KeyValueMap_2096808863 {
    public int numberOfRecords;
    public int[] keys;
    public long[] values_ord_0;
    public long[] values_ord_1;
    public long[] values_ord_2;
    private int[] hashTable;
    private int[] next;

    public KeyValueMap_2096808863() {
        this(4);
    }
    public KeyValueMap_2096808863(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys = new int[capacity];
        Arrays.fill(this.keys, -1);
        this.values_ord_0 = new long[capacity];
        this.values_ord_1 = new long[capacity];
        this.values_ord_2 = new long[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
        this.next = new int[capacity];
        Arrays.fill(this.next, -1);
    }

    public void incrementForKey(int key, long preHash, long record_ord_0, long record_ord_1, long record_ord_2) {
        if ((key < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects non-negative keys");
        }
        int index = this.find(key, preHash);
        boolean newEntry = false;
        if ((index == -1)) {
            newEntry = true;
            index = this.numberOfRecords++;
            if ((this.keys.length == index)) {
                this.growArrays();
            }
            this.keys[index] = key;
        }
        this.values_ord_0[index] += record_ord_0;
        this.values_ord_1[index] += record_ord_1;
        this.values_ord_2[index] += record_ord_2;
        if (newEntry) {
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(key, preHash, index, rehashOnCollision);
        }
    }
    private int find(int key, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return -1;
        }
        int currentIndex = initialIndex;
        while (this.keys[currentIndex] != key) {
            int potentialNextIndex = this.next[currentIndex];
            if ((potentialNextIndex == -1)) {
                return -1;
            } else {
                currentIndex = potentialNextIndex;
            }
        }
        return currentIndex;
    }
    private void growArrays() {
        int currentSize = this.keys.length;
        int newSize = (currentSize << 1);
        if ((newSize > (Integer.MAX_VALUE - 1))) {
            throw new java.lang.UnsupportedOperationException("Map has grown too large");
        }
        int[] newKeys = new int[newSize];
        System.arraycopy(this.keys, 0, newKeys, 0, currentSize);
        Arrays.fill(newKeys, currentSize, newSize, -1);
        this.keys = newKeys;
        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1);
        this.next = newNext;
        long[] new_values_ord_0 = new long[newSize];
        System.arraycopy(this.values_ord_0, 0, new_values_ord_0, 0, currentSize);
        this.values_ord_0 = new_values_ord_0;
        long[] new_values_ord_1 = new long[newSize];
        System.arraycopy(this.values_ord_1, 0, new_values_ord_1, 0, currentSize);
        this.values_ord_1 = new_values_ord_1;
        long[] new_values_ord_2 = new long[newSize];
        System.arraycopy(this.values_ord_2, 0, new_values_ord_2, 0, currentSize);
        this.values_ord_2 = new_values_ord_2;
    }
    private void putHashEntry(int key, long preHash, int index, boolean rehashOnCollision) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            this.hashTable[htIndex] = index;
            return;
        }
        if (rehashOnCollision) {
            this.rehash();
            return;
        }
        int currentIndex = initialIndex;
        while (this.keys[currentIndex] != key && this.next[currentIndex] != -1) {
            currentIndex = this.next[currentIndex];
        }
        this.next[currentIndex] = index;
    }
    private void rehash() {
        int size = this.hashTable.length;
        while (size <= this.numberOfRecords) {
            size = (size << 1);
        }
        size = (size << 1);
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
        for (int i = 0; i < this.numberOfRecords; i++) {
            int key = this.keys[i];
            long preHash = Int_Hash_Function.preHash(key);
            this.putHashEntry(key, preHash, i, false);
        }
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.keys, -1);
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
        Arrays.fill(this.values_ord_0, 0);
        Arrays.fill(this.values_ord_1, 0);
        Arrays.fill(this.values_ord_2, 0);
    }
}
