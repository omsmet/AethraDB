package AethraDB.benchmarks.tpch.q1_no_sort_hard_coded;

import AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function;

import java.util.Arrays;

public final class AggregationMap_Class {

    public static final class MapRC {

        public double ord_0;
        public double ord_1;
        public double ord_2;
        public double ord_3;
        public int ord_4;
        public double ord_5;

        public MapRC(double ord_incr_0, double ord_incr_1, double ord_incr_2, double ord_incr_3, int ord_incr_4, double ord_incr_5) {
            this.ord_0 = ord_incr_0;
            this.ord_1 = ord_incr_1;
            this.ord_2 = ord_incr_2;
            this.ord_3 = ord_incr_3;
            this.ord_4 = ord_incr_4;
            this.ord_5 = ord_incr_5;
        }

        public void increment(double ord_incr_0, double ord_incr_1, double ord_incr_2, double ord_incr_3, int ord_incr_4, double ord_incr_5) {
            this.ord_0 += ord_incr_0;
            this.ord_1 += ord_incr_1;
            this.ord_2 += ord_incr_2;
            this.ord_3 += ord_incr_3;
            this.ord_4 += ord_incr_4;
            this.ord_5 += ord_incr_5;
        }

    }

    public int numberOfRecords;
    public byte[][] keys_ord_0;
    public byte[][] keys_ord_1;
    public MapRC[] values;
    private int[] hashTable;
    private int[] next;

    public AggregationMap_Class() {
        this(4);
    }
    public AggregationMap_Class(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys_ord_0 = new byte[capacity][];
        Arrays.fill(this.keys_ord_0, null);
        this.keys_ord_1 = new byte[capacity][];
        Arrays.fill(this.keys_ord_1, null);
        this.values = new MapRC[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
        this.next = new int[capacity];
        Arrays.fill(this.next, -1);
    }

    public void incrementForKey(byte[] key_ord_0, byte[] key_ord_1, long preHash, double value_ord_0, double value_ord_1, double value_ord_2, double value_ord_3, int value_ord_4, double value_ord_5) {
        if ((key_ord_0 == null)) {
            throw new IllegalArgumentException("The map expects the first key ordinal to be non-null");
        }
        int index = this.find(key_ord_0, key_ord_1, preHash);
        if ((index == -1)) {
            index = this.numberOfRecords++;
            if ((this.keys_ord_0.length == index)) {
                this.growArrays();
            }
            int key_ord_0_length = key_ord_0.length;
            byte[] key_ord_0_copy = new byte[key_ord_0_length];
            System.arraycopy(key_ord_0, 0, key_ord_0_copy, 0, key_ord_0_length);
            this.keys_ord_0[index] = key_ord_0_copy;
            int key_ord_1_length = key_ord_1.length;
            byte[] key_ord_1_copy = new byte[key_ord_1_length];
            System.arraycopy(key_ord_1, 0, key_ord_1_copy, 0, key_ord_1_length);
            this.keys_ord_1[index] = key_ord_1_copy;
            this.values[index] = new MapRC(value_ord_0, value_ord_1, value_ord_2, value_ord_3, value_ord_4, value_ord_5);
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(key_ord_0, key_ord_1, preHash, index, rehashOnCollision);
            return;
        }
        this.values[index].increment(value_ord_0, value_ord_1, value_ord_2, value_ord_3, value_ord_4, value_ord_5);
    }
    private int find(byte[] key_ord_0, byte[] key_ord_1, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return -1;
        }
        int currentIndex = initialIndex;
        while ((!(Arrays.equals(this.keys_ord_0[currentIndex], key_ord_0)) || !(Arrays.equals(this.keys_ord_1[currentIndex], key_ord_1)))) {
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
            throw new UnsupportedOperationException("Map has grown too large");
        }
        byte[][] keys_ord_0_new = new byte[newSize][];
        System.arraycopy(this.keys_ord_0, 0, keys_ord_0_new, 0, currentSize);
        Arrays.fill(keys_ord_0_new, currentSize, newSize, null);
        this.keys_ord_0 = keys_ord_0_new;
        byte[][] keys_ord_1_new = new byte[newSize][];
        System.arraycopy(this.keys_ord_1, 0, keys_ord_1_new, 0, currentSize);
        Arrays.fill(keys_ord_1_new, currentSize, newSize, null);
        this.keys_ord_1 = keys_ord_1_new;
        int[] newNext = new int[newSize];
        System.arraycopy(this.next, 0, newNext, 0, currentSize);
        Arrays.fill(newNext, currentSize, newSize, -1);
        this.next = newNext;
        MapRC[] new_values = new MapRC[newSize];
        System.arraycopy(this.values, 0, new_values, 0, currentSize);
        this.values = new_values;
    }
    private void putHashEntry(byte[] key_ord_0, byte[] key_ord_1, long preHash, int index, boolean rehashOnCollision) {
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
        while (((!(Arrays.equals(this.keys_ord_0[currentIndex], key_ord_0)) || !(Arrays.equals(this.keys_ord_1[currentIndex], key_ord_1))) && (this.next[currentIndex] != -1))) {
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
            byte[] key_ord_0 = this.keys_ord_0[i];
            byte[] key_ord_1 = this.keys_ord_1[i];
            long preHash = Char_Arr_Hash_Function.preHash(key_ord_0);
            preHash ^= Char_Arr_Hash_Function.preHash(key_ord_1);
            this.putHashEntry(key_ord_0, key_ord_1, preHash, i, false);
        }
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.keys_ord_0, null);
        Arrays.fill(this.keys_ord_1, null);
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.next, -1);
        Arrays.fill(this.values, null);
    }
}
