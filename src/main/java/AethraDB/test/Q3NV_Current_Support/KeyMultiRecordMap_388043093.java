package AethraDB.test.Q3NV_Current_Support;

import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;

import java.util.Arrays;

public final class KeyMultiRecordMap_388043093 {
    public static final class ValueRecordType {
        public final int value_ord_0;
        public final int value_ord_1;

        public ValueRecordType(int value_ord_0, int value_ord_1) {
            this.value_ord_0 = value_ord_0;
            this.value_ord_1 = value_ord_1;
        }

    }

    public static final class KeyRecordType {
        public final int key;
        public int count;
        public KeyRecordType next;
        public ValueRecordType[] records;

        public KeyRecordType(int key) {
            this.key = key;
            this.count = 0;
            this.next = null;
            this.records = new ValueRecordType[1];
        }

    }

    private int numberOfRecords;
    public KeyRecordType[] keys;
    private int[] hashTable;

    public KeyMultiRecordMap_388043093() {
        this(262144);
    }
    public KeyMultiRecordMap_388043093(int capacity) {
        if (!(((capacity > 1) && ((capacity & (capacity - 1)) == 0)))) {
            throw new java.lang.IllegalArgumentException("The map capacity is required to be a power of two");
        }
        this.numberOfRecords = 0;
        this.keys = new KeyRecordType[capacity];
        this.hashTable = new int[capacity];
        Arrays.fill(this.hashTable, -1);
    }

    public void associate(int key, long preHash, int record_ord_0, int record_ord_1) {
        if ((key < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects non-negative keys");
        }
        KeyRecordType keyRecord = this.find(key, preHash);
        boolean newEntry = false;
        int newEntryIndex = -1;
        if ((keyRecord == null)) {
            newEntry = true;
            newEntryIndex = this.numberOfRecords++;
            if ((this.keys.length == newEntryIndex)) {
                this.growArrays();
            }
            keyRecord = new KeyRecordType(key);
            this.keys[newEntryIndex] = keyRecord;
        }
        int insertionIndex = keyRecord.count;
        if (!((insertionIndex < keyRecord.records.length))) {
            int currentValueArraySize = keyRecord.records.length;
            int newValueArraySize = (8 * currentValueArraySize);
            ValueRecordType[] temp_records = new ValueRecordType[newValueArraySize];
            System.arraycopy(keyRecord.records, 0, temp_records, 0, currentValueArraySize);
            keyRecord.records = temp_records;
        }
        keyRecord.records[insertionIndex] = new ValueRecordType(record_ord_0, record_ord_1);
        keyRecord.count++;
        if (newEntry) {
            boolean rehashOnCollision = (this.numberOfRecords > ((3 * this.hashTable.length) / 4));
            this.putHashEntry(keyRecord, preHash, newEntryIndex, rehashOnCollision);
        }
    }
    private KeyRecordType find(int key, long preHash) {
        int htIndex = ((int) (preHash & (this.hashTable.length - 1)));
        int initialIndex = this.hashTable[htIndex];
        if ((initialIndex == -1)) {
            return null;
        }
        KeyRecordType currentKeyRecord = this.keys[initialIndex];
        while ((currentKeyRecord.key != key)) {
            currentKeyRecord = currentKeyRecord.next;
            if ((currentKeyRecord == null)) {
                return null;
            }
        }
        return currentKeyRecord;
    }
    private void growArrays() {
        int currentSize = this.keys.length;
        int newSize = (currentSize * 8);
        if ((newSize > (Integer.MAX_VALUE - 1))) {
            throw new java.lang.UnsupportedOperationException("Map has grown too large");
        }
        KeyRecordType[] newKeys = new KeyRecordType[newSize];
        System.arraycopy(this.keys, 0, newKeys, 0, currentSize);
        this.keys = newKeys;
    }
    private void putHashEntry(KeyRecordType keyRecord, long preHash, int index, boolean rehashOnCollision) {
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
        KeyRecordType currentKeyRecord = this.keys[initialIndex];
        while ((currentKeyRecord.next != null)) {
            currentKeyRecord = currentKeyRecord.next;
        }
        currentKeyRecord.next = keyRecord;
    }
    private void rehash() {
        int size = (this.hashTable.length * 8);
        this.hashTable = new int[size];
        Arrays.fill(this.hashTable, -1);
        for (int recordIndex = 0; recordIndex < numberOfRecords; recordIndex++) {
            this.keys[recordIndex].next = null;
        }
        for (int i = 0; i < this.numberOfRecords; i++) {
            KeyRecordType keyRecord = this.keys[i];
            long preHash = Int_Hash_Function.preHash(keyRecord.key);
            this.putHashEntry(keyRecord, preHash, i, false);
        }
    }
    public KeyRecordType getIndex(int key, long preHash) {
        if ((key < 0)) {
            throw new java.lang.IllegalArgumentException("The map expects non-negative keys");
        }
        return this.find(key, preHash);
    }
    public void reset() {
        this.numberOfRecords = 0;
        Arrays.fill(this.keys, null);
        Arrays.fill(this.hashTable, -1);
    }
}
