package AethraDB.evaluation.general_support;

import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * This class contains methods which can be used to optimise certain access patterns to Arrow vectors.
 */
public class ArrowOptimisations {

    /**
     * Prevent instantiating this class.
     */
    private ArrowOptimisations() {

    }

    /**
     * Method to obtain the value of an element in a {@link FixedSizeBinaryVector} without performing
     * unnecessary allocations by using a byte array cache.
     * @param vector The vector to obtain the element value from.
     * @param elementIndex The index of the element whose value should be retrieved.
     * @param byteCacheTarget The byte array cache to perform the read with.
     * @return The reference to the byte array cache which contains the read value.
     */
    public static byte[] getFixedSizeBinaryValue(FixedSizeBinaryVector vector, long elementIndex, byte[] byteCacheTarget) {
        int vectorWidth = vector.getByteWidth();
        long memoryAddressToRead = vector.getDataBufferAddress() + elementIndex * vectorWidth;

        // Below statement initialises the byteCacheTarget on the first call to this method for the given column
        if (byteCacheTarget == null) {
            byteCacheTarget = new byte[vectorWidth];
        }

        MemoryUtil.UNSAFE.copyMemory(null, memoryAddressToRead, byteCacheTarget, MemoryUtil.BYTE_ARRAY_BASE_OFFSET, vectorWidth);
        return byteCacheTarget;
    }

    /**
     * Method to obtain the value of an element in a {@link VarCharVector} without performing
     * unnecessary allocations by using a byte array cache system.
     * @param vector The vector to obtain the element value from.
     * @param elementIndex The index of the element whose value should be retrieved.
     * @param byteCacheTargets The array of byte array caches to perform the read with.
     * @return The reference to the byte array cache which contains the read value.
     */
    public static byte[] getVarCharBinaryValue(VarCharVector vector, int elementIndex, byte[][] byteCacheTargets) {
        int requiredWidth = vector.getValueLength(elementIndex);

        if (byteCacheTargets[requiredWidth] == null) {
            byteCacheTargets[requiredWidth] = new byte[requiredWidth];
        }

        int startOffset = vector.getStartOffset(elementIndex);
        long memoryAddressToRead = vector.getDataBufferAddress() + startOffset;
        MemoryUtil.UNSAFE.copyMemory(null, memoryAddressToRead, byteCacheTargets[requiredWidth], MemoryUtil.BYTE_ARRAY_BASE_OFFSET, requiredWidth);
        return byteCacheTargets[requiredWidth];
    }

}
