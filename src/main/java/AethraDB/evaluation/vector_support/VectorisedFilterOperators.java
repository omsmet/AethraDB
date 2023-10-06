package AethraDB.evaluation.vector_support;

import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.IntVector;

/**
 * Class containing vectorised primitives for filter operations.
 */
public class VectorisedFilterOperators extends VectorisedOperators {

    /**
     * Prevent instantiating this class.
     */
    private VectorisedFilterOperators() {
        super();
    }

    /* TO PREVENT IMPLEMENTATION OVERHEAD, THE BELOW METHODS DO NOT HAVE JAVADOC, AS THEY SHOULD BE SELF EXPLANATORY */

    public static int gt(IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) > condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int ge(IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) >= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int lt(IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int le(IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) <= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(
            IntVector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) > condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int ge(
            IntVector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) >= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int lt(
            IntVector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) < condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int le(
            IntVector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) <= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) > condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int ge(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) >= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int lt(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int le(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) <= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int between_ge_lt(org.apache.arrow.vector.DateDayVector vector, int lower, int upper, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            int value = vector.get(i);
            if (value >= lower && value < upper)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector, int[] validIndices, int validIndicesCount) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) > condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int ge(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector, int[] validIndices, int validIndicesCount) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) >= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int lt(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector, int[] validIndices, int validIndicesCount) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) < condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int le(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector, int[] validIndices, int validIndicesCount) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) <= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(org.apache.arrow.vector.Float8Vector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) > condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int ge(org.apache.arrow.vector.Float8Vector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) >= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int lt(org.apache.arrow.vector.Float8Vector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int le(org.apache.arrow.vector.Float8Vector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) <= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) > condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int ge(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) >= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int lt(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) < condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int le(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) <= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(org.apache.arrow.vector.Float8Vector vector, double condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) > condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int ge(org.apache.arrow.vector.Float8Vector vector, double condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) >= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int lt(org.apache.arrow.vector.Float8Vector vector, double condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int le(org.apache.arrow.vector.Float8Vector vector, double condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) <= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) > condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int ge(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) >= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int lt(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) < condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    public static int le(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            if (vector.get(validIndex) <= condition)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int between_ge_le(
            org.apache.arrow.vector.Float8Vector vector,
            double lower,
            double upper,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount
    ) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < validIndicesCount; i++) {
            int validIndex = validIndices[i];
            double value = vector.get(validIndex);
            if (value >= lower && value <= upper)
                selectionVector[selectionVectorIndex++] = validIndex;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int eq(org.apache.arrow.vector.FixedSizeBinaryVector vector, byte[] condition, int[] selectionVector) {
        long vectorWidth = vector.getByteWidth();
        long vectorMemoryBaseAddress = vector.getDataBufferAddress();
        assert vectorWidth == condition.length;

        int selectionVectorIndex = 0;

        outerLoop: for (int i = 0; i < vector.getValueCount(); i++) {
            long elementMemoryBaseAddress = vectorMemoryBaseAddress + i * vectorWidth;

            for (int j = 0; j < vectorWidth; j++) {
                if (MemoryUtil.UNSAFE.getByte(elementMemoryBaseAddress + j) != condition[j])
                    continue outerLoop;
            }

            selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int eq(org.apache.arrow.vector.FixedSizeBinaryVector vector, byte[] condition, boolean[] validityMask) {
        long vectorWidth = vector.getByteWidth();
        long vectorMemoryBaseAddress = vector.getDataBufferAddress();
        assert vectorWidth == condition.length;

        int vectorLength = vector.getValueCount();

        outerLoop: for (int i = 0; i < vectorLength; i++) {
            long elementMemoryBaseAddress = vectorMemoryBaseAddress + i * vectorWidth;

            for (int j = 0; j < vectorWidth; j++) {
                if (MemoryUtil.UNSAFE.getByte(elementMemoryBaseAddress + j) != condition[j]) {
                    validityMask[i] = false;
                    continue outerLoop;
                }
            }

            validityMask[i] = true;
        }

        return vectorLength;
    }

}
