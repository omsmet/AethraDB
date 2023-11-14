package AethraDB.benchmarks.tpch.q1_no_sort_hard_coded;

import AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function;

import java.util.Arrays;

public final class AggregationMap {
    public static final class RecordType {
        public byte[] key_ord_0;
        public byte[] key_ord_1;
        public double value_ord_0;
        public double value_ord_1;
        public double value_ord_2;
        public double value_ord_3;
        public int value_ord_4;
        public double value_ord_5;
        public RecordType next;

        public RecordType(byte[] key_ord_0, byte[] key_ord_1, double value_ord_0, double value_ord_1, double value_ord_2, double value_ord_3, int value_ord_4, double value_ord_5) {
            this.key_ord_0 = key_ord_0;
            this.key_ord_1 = key_ord_1;
            this.value_ord_0 = value_ord_0;
            this.value_ord_1 = value_ord_1;
            this.value_ord_2 = value_ord_2;
            this.value_ord_3 = value_ord_3;
            this.value_ord_4 = value_ord_4;
            this.value_ord_5 = value_ord_5;
            this.next = null;
        }

        public void increment(double value_ord_0, double value_ord_1, double value_ord_2, double value_ord_3, int value_ord_4, double value_ord_5) {
            this.value_ord_0 += value_ord_0;
            this.value_ord_1 += value_ord_1;
            this.value_ord_2 += value_ord_2;
            this.value_ord_3 += value_ord_3;
            this.value_ord_4 += value_ord_4;
            this.value_ord_5 += value_ord_5;
        }
    }

    public int numberOfRecords;
    public RecordType[] records;
    private int[] hashTable;

    public AggregationMap() {
        this(32768);
    }
    public AggregationMap(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.records = new RecordType[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
    }

    public void incrementForKey(byte[] key_ord_0, byte[] key_ord_1, long preHash, double value_ord_0, double value_ord_1, double value_ord_2, double value_ord_3, int value_ord_4, double value_ord_5) {
        if ((key_ord_0 == null)) {
            throw new java.lang.IllegalArgumentException("The map expects the first key ordinal to be non-null");
        }
        RecordType record = this.find(key_ord_0, key_ord_1, preHash);
        if ((record == null)) {
            int newIndex = this.numberOfRecords++;
            if ((this.records.length == newIndex)) {
                this.growArrays();
            }
            int key_ord_0_length = key_ord_0.length;
            byte[] key_ord_0_copy = new byte[key_ord_0_length];
            System.arraycopy(key_ord_0, 0, key_ord_0_copy, 0, key_ord_0_length);
            int key_ord_1_length = key_ord_1.length;
            byte[] key_ord_1_copy = new byte[key_ord_1_length];
            System.arraycopy(key_ord_1, 0, key_ord_1_copy, 0, key_ord_1_length);
            record = new RecordType(key_ord_0_copy, key_ord_1_copy, value_ord_0, value_ord_1, value_ord_2, value_ord_3, value_ord_4, value_ord_5);
            this.records[newIndex] = record;
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(record, preHash, newIndex, rehashOnCollision);
            return;
        }
        record.increment(value_ord_0, value_ord_1, value_ord_2, value_ord_3, value_ord_4, value_ord_5);
    }
    private RecordType find(byte[] key_ord_0, byte[] key_ord_1, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return null;
        }
        RecordType currentRecord = this.records[initialIndex];
        while ((!((((currentRecord.key_ord_0.length == 1) && (key_ord_0.length == 1)) && (currentRecord.key_ord_0[0] == key_ord_0[0]))) || !((((currentRecord.key_ord_1.length == 1) && (key_ord_1.length == 1)) && (currentRecord.key_ord_1[0] == key_ord_1[0]))))) {
            currentRecord = currentRecord.next;
            if ((currentRecord == null)) {
                return null;
            }
        }
        return currentRecord;
    }
    private void growArrays() {
        int currentSize = this.records.length;
        int newSize = (currentSize * 16);
        if ((newSize > (Integer.MAX_VALUE - 1))) {
            throw new java.lang.UnsupportedOperationException("Map has grown too large");
        }
        RecordType[] newrecords = new RecordType[newSize];
        System.arraycopy(this.records, 0, newrecords, 0, currentSize);
        this.records = newrecords;
    }
    private void putHashEntry(RecordType record, long preHash, int index, boolean rehashOnCollision) {
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
        RecordType currentRecord = this.records[initialIndex];
        while ((currentRecord.next != null)) {
            currentRecord = currentRecord.next;
        }
        currentRecord.next = record;
    }
    private void rehash() {
        int size = (this.hashTable.length * 16);
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        for (int recordIndex = 0; recordIndex < numberOfRecords; recordIndex++) {
            this.records[recordIndex].next = null;
        }
        for (int i = 0; i < this.numberOfRecords; i++) {
            RecordType currentRecord = this.records[i];
            byte[] key_ord_0 = currentRecord.key_ord_0;
            byte[] key_ord_1 = currentRecord.key_ord_1;
            long preHash = Char_Arr_Hash_Function.preHash(key_ord_0);
            preHash ^= Char_Arr_Hash_Function.preHash(key_ord_1);
            this.putHashEntry(currentRecord, preHash, i, false);
        }
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.records, null);
    }
}
