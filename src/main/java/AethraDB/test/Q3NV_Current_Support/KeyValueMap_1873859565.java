package AethraDB.test.Q3NV_Current_Support;

import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class KeyValueMap_1873859565 {
    public static final class RecordType {
        public int key_ord_0;
        public int key_ord_1;
        public int key_ord_2;
        public double value_ord_0;
        public RecordType next;

        public RecordType(int key_ord_0, int key_ord_1, int key_ord_2, double value_ord_0) {
            this.key_ord_0 = key_ord_0;
            this.key_ord_1 = key_ord_1;
            this.key_ord_2 = key_ord_2;
            this.value_ord_0 = value_ord_0;
            this.next = null;
        }

        public void increment(double value_ord_0) {
            this.value_ord_0 += value_ord_0;
        }
    }

    public int numberOfRecords;
    public RecordType[] records;
    private int[] hashTable;

    public KeyValueMap_1873859565() {
        this(32768);
    }
    public KeyValueMap_1873859565(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.records = new RecordType[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
    }

    public void incrementForKey(int key_ord_0, int key_ord_1, int key_ord_2, long preHash, double value_ord_0) {
        if ((key_ord_0 < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects the first key ordinal to be non-negative");
        }
        RecordType record = this.find(key_ord_0, key_ord_1, key_ord_2, preHash);
        if ((record == null)) {
            int newIndex = this.numberOfRecords++;
            if ((this.records.length == newIndex)) {
                this.growArrays();
            }
            record = new RecordType(key_ord_0, key_ord_1, key_ord_2, value_ord_0);
            this.records[newIndex] = record;
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(record, preHash, newIndex, rehashOnCollision);
            return;
        }
        record.increment(value_ord_0);
    }
    private RecordType find(int key_ord_0, int key_ord_1, int key_ord_2, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return null;
        }
        RecordType currentRecord = this.records[initialIndex];
        while ((((currentRecord.key_ord_0 != key_ord_0) || (currentRecord.key_ord_1 != key_ord_1)) || (currentRecord.key_ord_2 != key_ord_2))) {
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
            int key_ord_0 = currentRecord.key_ord_0;
            int key_ord_1 = currentRecord.key_ord_1;
            int key_ord_2 = currentRecord.key_ord_2;
            long preHash = Int_Hash_Function.preHash(key_ord_0);
            preHash ^= Int_Hash_Function.preHash(key_ord_1);
            preHash ^= Int_Hash_Function.preHash(key_ord_2);
            this.putHashEntry(currentRecord, preHash, i, false);
        }
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.hashTable, -1);
        Arrays.fill(this.records, null);
    }
}
