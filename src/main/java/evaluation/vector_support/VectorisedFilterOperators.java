package evaluation.vector_support;

import evaluation.general_support.ArrowOptimisations;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Class containing vectorised primitives for filter operations.
 */
public class VectorisedFilterOperators extends VectorisedOperators {

    /**
     * The {@link VectorSpecies<Double>} to use for the SIMD-ed primitives in this class.
     */
    private static final VectorSpecies<Double> DOUBLE_SPECIES_PREFERRED =
            jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;

    /**
     * The {@link VectorSpecies<Integer>} to use for the SIMD-ed primitives in this class.
     */
    private static final VectorSpecies<Integer> INT_SPECIES_PREFERRED =
            jdk.incubator.vector.IntVector.SPECIES_PREFERRED;

    /**
     * Prevent instantiating this class.
     */
    private VectorisedFilterOperators() {
        super();
    }

    /* TO PREVENT IMPLEMENTATION OVERHEAD, THE BELOW METHODS DO NOT HAVE JAVADOC, AS THEY SHOULD BE SELF EXPLANATORY */

    public static int gt(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) > condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int ge(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) >= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int lt(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int le(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) <= condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gt(
            org.apache.arrow.vector.IntVector vector,
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
            org.apache.arrow.vector.IntVector vector,
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
            org.apache.arrow.vector.IntVector vector,
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
            org.apache.arrow.vector.IntVector vector,
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

    public static int gtSIMD(org.apache.arrow.vector.IntVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask);
    }

    public static int geSIMD(org.apache.arrow.vector.IntVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask);
    }

    public static int ltSIMD(org.apache.arrow.vector.IntVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask);
    }

    public static int leSIMD(org.apache.arrow.vector.IntVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * IntVector.TYPE_WIDTH;
        MemorySegment vectorSegment =
                MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += INT_SPECIES_PREFERRED.length()) {
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var validityMaskVector = simdVector.compare(comparisonOperator, condition);
            validityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) > condition;
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) >= condition;
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gtSIMD(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int geSIMD(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int ltSIMD(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int leSIMD(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * IntVector.TYPE_WIDTH;
        MemorySegment vectorSegment = MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += INT_SPECIES_PREFERRED.length()) {
            // Initialise the SIMD vector and mask indicating the entries that are valid
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            VectorMask<Integer> validityMaskVector = VectorMask.fromArray(
                    INT_SPECIES_PREFERRED,
                    validIndicesMask,
                    currentIndex
            );

            // Compute the valid entries which match the condition as a vector mask
            var resultValidityMaskVector = simdVector.compare(
                    comparisonOperator,
                    condition,
                    validityMaskVector);
            resultValidityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) > condition);
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) >= condition);
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) <= condition);
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
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

    public static int gtSIMD(org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask);
    }

    public static int geSIMD(org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask);
    }

    public static int ltSIMD(org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask);
    }

    public static int leSIMD(org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.DateDayVector vector,
            int condition,
            boolean[] validityMask
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * IntVector.TYPE_WIDTH;
        MemorySegment vectorSegment =
                MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += INT_SPECIES_PREFERRED.length()) {
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var validityMaskVector = simdVector.compare(comparisonOperator, condition);
            validityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) > condition;
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) >= condition;
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
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

    public static int gtSIMD(
            org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask, boolean[] validIndicesMask, int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int geSIMD(
            org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask, boolean[] validIndicesMask, int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int ltSIMD(
            org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask, boolean[] validIndicesMask, int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int leSIMD(
            org.apache.arrow.vector.DateDayVector vector, int condition, boolean[] validityMask, boolean[] validIndicesMask, int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.DateDayVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * IntVector.TYPE_WIDTH;
        MemorySegment vectorSegment =
                MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += INT_SPECIES_PREFERRED.length()) {
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            VectorMask<Integer> validityMaskVector = VectorMask.fromArray(
                    INT_SPECIES_PREFERRED,
                    validIndicesMask,
                    currentIndex
            );

            // Compute the valid entries which match the condition as a vector mask
            var resultValidityMaskVector = simdVector.compare(
                    comparisonOperator,
                    condition,
                    validityMaskVector);
            resultValidityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) > condition);
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) >= condition);
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) <= condition);
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
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

    public static int gtSIMD(org.apache.arrow.vector.Float8Vector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask);
    }

    public static int geSIMD(org.apache.arrow.vector.Float8Vector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask);
    }

    public static int ltSIMD(org.apache.arrow.vector.Float8Vector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask);
    }

    public static int leSIMD(org.apache.arrow.vector.Float8Vector vector, int condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH;
        MemorySegment vectorSegment =
                MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var simdVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var validityMaskVector = simdVector.compare(comparisonOperator, condition);
            validityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) > condition;
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) >= condition;
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gtSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int geSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int ltSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int leSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.Float8Vector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH;
        MemorySegment vectorSegment = MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            // Initialise the SIMD vector and mask indicating the entries that are valid
            var simdVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            VectorMask<Double> validityMaskVector = VectorMask.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    validIndicesMask,
                    currentIndex
            );

            // Compute the valid entries which match the condition as a vector mask
            var resultValidityMaskVector = simdVector.compare(
                    comparisonOperator,
                    condition,
                    validityMaskVector);
            resultValidityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) > condition);
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) >= condition);
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) <= condition);
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
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

    public static int gtSIMD(org.apache.arrow.vector.Float8Vector vector, double condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask);
    }

    public static int geSIMD(org.apache.arrow.vector.Float8Vector vector, double condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask);
    }

    public static int ltSIMD(org.apache.arrow.vector.Float8Vector vector, double condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask);
    }

    public static int leSIMD(org.apache.arrow.vector.Float8Vector vector, double condition, boolean[] validityMask) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH;
        MemorySegment vectorSegment =
                MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var simdVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var validityMaskVector = simdVector.compare(comparisonOperator, condition);
            validityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) > condition;
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) >= condition;
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int gtSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int geSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.GE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int ltSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LT, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int leSIMD(
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength) {
        return compareSIMD(VectorOperators.LE, vector, condition, validityMask, validIndicesMask, validIndicesMaskLength);
    }

    public static int compareSIMD(
            VectorOperators.Comparison comparisonOperator,
            org.apache.arrow.vector.Float8Vector vector,
            double condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength
    ) {
        // Initialise the memory segment
        int vectorLength = vector.getValueCount();
        long bufferSize = (long) vectorLength * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH;
        MemorySegment vectorSegment = MemorySegment.ofAddress(vector.getDataBufferAddress(), bufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for (; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            // Initialise the SIMD vector and mask indicating the entries that are valid
            var simdVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    vectorSegment,
                    (long) currentIndex * org.apache.arrow.vector.Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            VectorMask<Double> validityMaskVector = VectorMask.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    validIndicesMask,
                    currentIndex
            );

            // Compute the valid entries which match the condition as a vector mask
            var resultValidityMaskVector = simdVector.compare(
                    comparisonOperator,
                    condition,
                    validityMaskVector);
            resultValidityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        if (comparisonOperator == VectorOperators.GT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) > condition);
            }

        } else if (comparisonOperator == VectorOperators.GE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) >= condition);
            }

        } else if (comparisonOperator == VectorOperators.LT) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
            }

        } else if (comparisonOperator == VectorOperators.LE) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) <= condition);
            }

        } else {
            throw new UnsupportedOperationException(
                    "VectorisedFilterOperators.compareSIMD does not support the provided comparison operator: " + comparisonOperator);
        }

        return vectorLength;
    }

    /* --------------------------------------------------------------------------------------------------- */

    public static int eq(org.apache.arrow.vector.FixedSizeBinaryVector vector, byte[] condition, int[] selectionVector) {
        assert vector.getByteWidth() == condition.length;

        byte[] byte_array_cache = getByteArrayCache(vector.getByteWidth());
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (Arrays.equals(ArrowOptimisations.getFixedSizeBinaryValue(vector, i, byte_array_cache), condition))
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    public static int eq(org.apache.arrow.vector.FixedSizeBinaryVector vector, byte[] condition, boolean[] validityMask) {
        assert vector.getByteWidth() == condition.length;

        byte[] byte_array_cache = getByteArrayCache(vector.getByteWidth());
        int vectorLength = vector.getValueCount();

        for (int i = 0; i < vectorLength; i++) {
            validityMask[i] = Arrays.equals(ArrowOptimisations.getFixedSizeBinaryValue(vector, i, byte_array_cache), condition);
        }

        return vectorLength;
    }

    public static int eqSIMD(org.apache.arrow.vector.FixedSizeBinaryVector vector, byte[] condition, boolean[] validityMask) {
        // TODO: not manually SIMDed for now, since we assume the compiler already SIMDs the Arrays.equals call based on the source code
        return eq(vector, condition, validityMask);
    }

}
