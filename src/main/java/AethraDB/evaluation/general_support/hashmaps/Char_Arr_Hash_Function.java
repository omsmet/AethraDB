package AethraDB.evaluation.general_support.hashmaps;

import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * The standard hash function used for computing the hash value of a character array column.
 */
public final class Char_Arr_Hash_Function {

    /**
     * Buffer array for reading from a fixed-size binary vector.
     */
    private static byte[] readBuffer = null;

    /**
     * Prevent instantiation of this class.
     */
    private Char_Arr_Hash_Function() {

    }

    /**
     * Method to compute the pre-hash of a character array key.
     * @param key The key to compute the pre-hash value for.
     * @return The pre-hash value.
     */
    public static long preHash(byte[] key) {
        long hash = 0L;
        for (int i = 0; i < key.length; i++)
            hash = (hash * 31) ^ key[i];

        return (hash < 0) ? (-hash) :  hash;
    }

    /**
     * Method to compute the pre-hash of a character array in an arrow vector without any copies.
     * @param vector The arrow vector containing the character array.
     * @param keyIndex The index of the character array to compute the pre-hash value for.
     * @return The pre-hash value.
     */
    public static long preHash(FixedSizeBinaryVector vector, long keyIndex) {
        int vectorWidth = vector.getByteWidth();
        long memoryAddressToRead = vector.getDataBufferAddress() + keyIndex * vectorWidth;

        // Upgrade the readBuffer if necessary
        if (readBuffer == null || readBuffer.length < vectorWidth) {
            readBuffer = new byte[vectorWidth];
        }

        // Read the value
        MemoryUtil.UNSAFE.copyMemory(null, memoryAddressToRead, readBuffer, MemoryUtil.BYTE_ARRAY_BASE_OFFSET, vectorWidth);

        // Hash the part of the readBuffer that is valid
        long hash = 0L;
        for (int i = 0; i < vectorWidth; i++) {
            hash = (hash * 31) ^ readBuffer[i];
        }

        return (hash < 0) ? (-hash) : hash;
    }

}
