package AethraDB.benchmarks.tpch.q3_no_sort_hard_coded;

import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class AggregationMap {
    public int numberOfRecords;
    public int[] keys_ord_0;
    public int[] keys_ord_1;
    public int[] keys_ord_2;
    public double[] values_ord_0;
    private int[] hashTable;
    private int[] next;

    public AggregationMap() {
        this(4);
    }
    public AggregationMap(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys_ord_0 = new int[capacity];
        Arrays.fill(this.keys_ord_0, -1);
        this.keys_ord_1 = new int[capacity];
        Arrays.fill(this.keys_ord_1, -1);
        this.keys_ord_2 = new int[capacity];
        Arrays.fill(this.keys_ord_2, -1);
        this.values_ord_0 = new double[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
        this.next = new int[capacity];
        Arrays.fill(this.next, -1);
    }

    public void incrementForKey(int key_ord_0, int key_ord_1, int key_ord_2, long preHash, double value_ord_0) {
        if ((key_ord_0 < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects the first key ordinal to be non-negative");
        }
        int index = this.find(key_ord_0, key_ord_1, key_ord_2, preHash);
        boolean newEntry = false;
        if ((index == -1)) {
            newEntry = true;
            index = this.numberOfRecords++;
            if ((this.keys_ord_0.length == index)) {
                this.growArrays();
            }
            this.keys_ord_0[index] = key_ord_0;
            this.keys_ord_1[index] = key_ord_1;
            this.keys_ord_2[index] = key_ord_2;
        }
        this.values_ord_0[index] += value_ord_0;
        if (newEntry) {
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(key_ord_0, key_ord_1, key_ord_2, preHash, index, rehashOnCollision);
        }
    }
    private int find(int key_ord_0, int key_ord_1, int key_ord_2, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return -1;
        }
        int currentIndex = initialIndex;
        while ((((this.keys_ord_0[currentIndex] != key_ord_0) || (this.keys_ord_1[currentIndex] != key_ord_1)) || (this.keys_ord_2[currentIndex] != key_ord_2))) {
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
        int currentSize = this.keys_ord_0.length;
        int newSize = (currentSize << 1);
        if ((newSize > (Integer.MAX_VALUE - 1))) {
            throw new java.lang.UnsupportedOperationException("Map has grown too large");
        }
        int[] keys_ord_0_new = new int[newSize];
        System.arraycopy(this.keys_ord_0, 0, keys_ord_0_new, 0, currentSize);
        Arrays.fill(keys_ord_0_new, currentSize, newSize, -1);
        this.keys_ord_0 = keys_ord_0_new;
        int[] keys_ord_1_new = new int[newSize];
        System.arraycopy(this.keys_ord_1, 0, keys_ord_1_new, 0, currentSize);
        Arrays.fill(keys_ord_1_new, currentSize, newSize, -1);
        this.keys_ord_1 = keys_ord_1_new;
        int[] keys_ord_2_new = new int[newSize];
        System.arraycopy(this.keys_ord_2, 0, keys_ord_2_new, 0, currentSize);
        Arrays.fill(keys_ord_2_new, currentSize, newSize, -1);
        this.keys_ord_2 = keys_ord_2_new;
        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1);
        this.next = newNext;
        double[] new_values_ord_0 = new double[newSize];
        System.arraycopy(this.values_ord_0, 0, new_values_ord_0, 0, currentSize);
        this.values_ord_0 = new_values_ord_0;
    }
    private void putHashEntry(int key_ord_0, int key_ord_1, int key_ord_2, long preHash, int index, boolean rehashOnCollision) {
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
        while (((((this.keys_ord_0[currentIndex] != key_ord_0) || (this.keys_ord_1[currentIndex] != key_ord_1)) || (this.keys_ord_2[currentIndex] != key_ord_2)) && (this.next[currentIndex] != -1))) {
            currentIndex = this.next[currentIndex];
        }
        this.next[currentIndex] = index;
    }
    private void rehash() {
        int size = this.hashTable.length;
        while ((size <= this.numberOfRecords)) {
            size = (size << 1);
        }
        size = (size << 1);
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
        for (int i = 0; i < this.numberOfRecords; i++) {
            int key_ord_0 = this.keys_ord_0[i];
            int key_ord_1 = this.keys_ord_1[i];
            int key_ord_2 = this.keys_ord_2[i];
            long preHash = Int_Hash_Function.preHash(key_ord_0);
            preHash ^= Int_Hash_Function.preHash(key_ord_1);
            preHash ^= Int_Hash_Function.preHash(key_ord_2);
            this.putHashEntry(key_ord_0, key_ord_1, key_ord_2, preHash, i, false);
        }
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.keys_ord_0, -1);
        Arrays.fill(this.keys_ord_1, -1);
        Arrays.fill(this.keys_ord_2, -1);
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
        Arrays.fill(this.values_ord_0, 0.0);
    }
}
