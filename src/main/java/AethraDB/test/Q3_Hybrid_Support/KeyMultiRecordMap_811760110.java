package AethraDB.test.Q3_Hybrid_Support;

import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class KeyMultiRecordMap_811760110 {
    private int numberOfRecords;
    private int[] keys;
    public int[] keysRecordCount;
    private int[] hashTable;
    private int[] next;

    public KeyMultiRecordMap_811760110() {
        this(262144);
    }
    public KeyMultiRecordMap_811760110(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys = new int[capacity];
        Arrays.fill(this.keys, -1);
        this.keysRecordCount = new int[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
        this.next = new int[capacity];
        Arrays.fill(this.next, -1);
    }

    public void associate(int key, long preHash) {
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
        this.keysRecordCount[index]++;
        if (newEntry) {
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(preHash, index, rehashOnCollision);
        }
    }
    private int find(int key, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return -1;
        }
        int currentIndex = initialIndex;
        while ((this.keys[currentIndex] != key)) {
            currentIndex = this.next[currentIndex];
            if ((currentIndex == -1)) {
                return -1;
            }
        }
        return currentIndex;
    }
    private void growArrays() {
        int currentSize = this.keys.length;
        int newSize = (currentSize * 8);
        if ((newSize > (Integer.MAX_VALUE - 1))) {
            throw new java.lang.UnsupportedOperationException("Map has grown too large");
        }
        int[] newKeys = new int[newSize];
        System.arraycopy(this.keys, 0, newKeys, 0, currentSize);
        Arrays.fill(newKeys, currentSize, newSize, -1);
        this.keys = newKeys;
        int[] newKeysRecordCount = new int[newSize];
        System.arraycopy(this.keysRecordCount, 0, newKeysRecordCount, 0, currentSize);
        this.keysRecordCount = newKeysRecordCount;
        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1);
        this.next = newNext;
    }
    private void putHashEntry(long preHash, int index, boolean rehashOnCollision) {
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
        while ((this.next[currentIndex] != -1)) {
            currentIndex = this.next[currentIndex];
        }
        this.next[currentIndex] = index;
    }
    private void rehash() {
        int size = (this.hashTable.length * 8);
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        for (int recordIndex = 0; recordIndex < numberOfRecords; recordIndex++) {
            this.next[recordIndex] = -1;
        }
        for (int i = 0; i < this.numberOfRecords; i++) {
            int key = this.keys[i];
            long preHash = Int_Hash_Function.preHash(key);
            this.putHashEntry(preHash, i, false);
        }
    }
    public int getIndex(int key, long preHash) {
        if ((key < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects non-negative keys");
        }
        return this.find(key, preHash);
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.keys, -1);
        Arrays.fill(this.keysRecordCount, 0);
        Arrays.fill(this.next, -1);
        Arrays.fill(this.hashTable, -1);
    }
}
