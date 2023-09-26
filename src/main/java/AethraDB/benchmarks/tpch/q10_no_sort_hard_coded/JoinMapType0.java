package AethraDB.benchmarks.tpch.q10_no_sort_hard_coded;

import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class JoinMapType0 {
    private int numberOfRecords;
    private int[] keys;
    public int[] keysRecordCount;
    public byte[][][] values_record_ord_0;
    public byte[][][] values_record_ord_1;
    public byte[][][] values_record_ord_2;
    public byte[][][] values_record_ord_3;
    public double[][] values_record_ord_4;
    public byte[][][] values_record_ord_5;
    private int[] hashTable;
    private int[] next;

    public JoinMapType0() {
        this(4);
    }
    public JoinMapType0(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys = new int[capacity];
        Arrays.fill(this.keys, -1);
        this.keysRecordCount = new int[capacity];
        this.values_record_ord_0 = new byte[capacity][8][];
        this.values_record_ord_1 = new byte[capacity][8][];
        this.values_record_ord_2 = new byte[capacity][8][];
        this.values_record_ord_3 = new byte[capacity][8][];
        this.values_record_ord_4 = new double[capacity][8];
        this.values_record_ord_5 = new byte[capacity][8][];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
        this.next = new int[capacity];
        Arrays.fill(this.next, -1);
    }

    public void associate(int key, long preHash, byte[] record_ord_0, byte[] record_ord_1, byte[] record_ord_2, byte[] record_ord_3, double record_ord_4, byte[] record_ord_5) {
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
        int insertionIndex = this.keysRecordCount[index];
        if (!((insertionIndex < this.values_record_ord_0[index].length))) {
            int currentValueArraysSize = this.values_record_ord_0[index].length;
            int newValueArraysSize = (2 * currentValueArraysSize);
            byte[][] temp_values_record_ord_0 = new byte[newValueArraysSize][];
            System.arraycopy(this.values_record_ord_0[index], 0, temp_values_record_ord_0, 0, currentValueArraysSize);
            this.values_record_ord_0[index] = temp_values_record_ord_0;
            byte[][] temp_values_record_ord_1 = new byte[newValueArraysSize][];
            System.arraycopy(this.values_record_ord_1[index], 0, temp_values_record_ord_1, 0, currentValueArraysSize);
            this.values_record_ord_1[index] = temp_values_record_ord_1;
            byte[][] temp_values_record_ord_2 = new byte[newValueArraysSize][];
            System.arraycopy(this.values_record_ord_2[index], 0, temp_values_record_ord_2, 0, currentValueArraysSize);
            this.values_record_ord_2[index] = temp_values_record_ord_2;
            byte[][] temp_values_record_ord_3 = new byte[newValueArraysSize][];
            System.arraycopy(this.values_record_ord_3[index], 0, temp_values_record_ord_3, 0, currentValueArraysSize);
            this.values_record_ord_3[index] = temp_values_record_ord_3;
            double[] temp_values_record_ord_4 = new double[newValueArraysSize];
            System.arraycopy(this.values_record_ord_4[index], 0, temp_values_record_ord_4, 0, currentValueArraysSize);
            this.values_record_ord_4[index] = temp_values_record_ord_4;
            byte[][] temp_values_record_ord_5 = new byte[newValueArraysSize][];
            System.arraycopy(this.values_record_ord_5[index], 0, temp_values_record_ord_5, 0, currentValueArraysSize);
            this.values_record_ord_5[index] = temp_values_record_ord_5;
        }
        int record_ord_0_length = record_ord_0.length;
        byte[] record_ord_0_copy = new byte[record_ord_0_length];
        System.arraycopy(record_ord_0, 0, record_ord_0_copy, 0, record_ord_0_length);
        this.values_record_ord_0[index][insertionIndex] = record_ord_0_copy;
        int record_ord_1_length = record_ord_1.length;
        byte[] record_ord_1_copy = new byte[record_ord_1_length];
        System.arraycopy(record_ord_1, 0, record_ord_1_copy, 0, record_ord_1_length);
        this.values_record_ord_1[index][insertionIndex] = record_ord_1_copy;
        int record_ord_2_length = record_ord_2.length;
        byte[] record_ord_2_copy = new byte[record_ord_2_length];
        System.arraycopy(record_ord_2, 0, record_ord_2_copy, 0, record_ord_2_length);
        this.values_record_ord_2[index][insertionIndex] = record_ord_2_copy;
        int record_ord_3_length = record_ord_3.length;
        byte[] record_ord_3_copy = new byte[record_ord_3_length];
        System.arraycopy(record_ord_3, 0, record_ord_3_copy, 0, record_ord_3_length);
        this.values_record_ord_3[index][insertionIndex] = record_ord_3_copy;
        this.values_record_ord_4[index][insertionIndex] = record_ord_4;
        int record_ord_5_length = record_ord_5.length;
        byte[] record_ord_5_copy = new byte[record_ord_5_length];
        System.arraycopy(record_ord_5, 0, record_ord_5_copy, 0, record_ord_5_length);
        this.values_record_ord_5[index][insertionIndex] = record_ord_5_copy;
        this.keysRecordCount[index]++;
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
        while ((this.keys[currentIndex] != key)) {
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
        int[] newKeysRecordCount = new int[newSize];
        System.arraycopy(this.keysRecordCount, 0, newKeysRecordCount, 0, currentSize);
        this.keysRecordCount = newKeysRecordCount;
        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1);
        this.next = newNext;
        byte[][][] new_values_record_ord_0 = new byte[newSize][8][];
        System.arraycopy(this.values_record_ord_0, 0, new_values_record_ord_0, 0, currentSize);
        this.values_record_ord_0 = new_values_record_ord_0;
        byte[][][] new_values_record_ord_1 = new byte[newSize][8][];
        System.arraycopy(this.values_record_ord_1, 0, new_values_record_ord_1, 0, currentSize);
        this.values_record_ord_1 = new_values_record_ord_1;
        byte[][][] new_values_record_ord_2 = new byte[newSize][8][];
        System.arraycopy(this.values_record_ord_2, 0, new_values_record_ord_2, 0, currentSize);
        this.values_record_ord_2 = new_values_record_ord_2;
        byte[][][] new_values_record_ord_3 = new byte[newSize][8][];
        System.arraycopy(this.values_record_ord_3, 0, new_values_record_ord_3, 0, currentSize);
        this.values_record_ord_3 = new_values_record_ord_3;
        double[][] new_values_record_ord_4 = new double[newSize][8];
        System.arraycopy(this.values_record_ord_4, 0, new_values_record_ord_4, 0, currentSize);
        this.values_record_ord_4 = new_values_record_ord_4;
        byte[][][] new_values_record_ord_5 = new byte[newSize][8][];
        System.arraycopy(this.values_record_ord_5, 0, new_values_record_ord_5, 0, currentSize);
        this.values_record_ord_5 = new_values_record_ord_5;
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
        while (((this.keys[currentIndex] != key) && (this.next[currentIndex] != -1))) {
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
            int key = this.keys[i];
            long preHash = Int_Hash_Function.preHash(key);
            this.putHashEntry(key, preHash, i, false);
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
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
    }
}
