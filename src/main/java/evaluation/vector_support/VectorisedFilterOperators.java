package evaluation.vector_support;

import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * Class containing vectorised primitives for filter operations.
 */
public class VectorisedFilterOperators extends VectorisedOperators {

    /**
     * The {@link VectorSpecies} to use for the SIMD-ed primitives in this class.
     */
    private static final VectorSpecies<Integer> INT_SPECIES_PREFERRED =
            jdk.incubator.vector.IntVector.SPECIES_PREFERRED;

    /**
     * Prevent instantiating this class.
     */
    private VectorisedFilterOperators() {
        super();
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector}
     * with a given less-than/less-than-equal condition.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param selectionVector The vector into which to produce the indices of valid records.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid selection of {@code selectionVector}/number of records
     * matching the condition.
     */
    public static int lessThanEqual(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector, boolean allowEqual) {
        int selectionVectorIndex = 0;

        if (allowEqual) {
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.get(i) <= condition)
                    selectionVector[selectionVectorIndex++] = i;
            }
        } else {
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.get(i) < condition)
                    selectionVector[selectionVectorIndex++] = i;
            }
        }

        return selectionVectorIndex;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector}
     * with a given less-than/less-than-equal condition where only some indices of the vector are valid.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param selectionVector The vector into which to produce the indices of valid records.
     * @param validIndices The array containing the indices of {@code vector} that are valid to start from.
     * @param validIndicesCount The number of valid indices in {@code validIndices}.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid selection of {@code selectionVector}/number of valid records
     * matching the condition.
     */
    public static int lessThanEqual(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            int[] selectionVector,
            int[] validIndices,
            int validIndicesCount,
            boolean allowEqual
    ) {
        int selectionVectorIndex = 0;

        if (allowEqual) {
            for (int i = 0; i < validIndicesCount; i++) {
                int validIndex = validIndices[i];
                if (vector.get(validIndex) <= condition)
                    selectionVector[selectionVectorIndex++] = validIndex;
            }
        } else {
            for (int i = 0; i < validIndicesCount; i++) {
                int validIndex = validIndices[i];
                if (vector.get(validIndex) < condition)
                    selectionVector[selectionVectorIndex++] = validIndex;
            }
        }

        return selectionVectorIndex;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector} with a
     * given less-than/less-than-equal condition using SIMD-ed code.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param validityMask The mask into which to produce the boolean indicating the validity of
     *                     each record in the vector.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid section of {@code validityMask}.
     */
    public static int lessThanEqualSIMD(
        org.apache.arrow.vector.IntVector vector,
        int condition,
        boolean[] validityMask,
        boolean allowEqual
    ) {
        // The operator to use
        VectorOperators.Comparison comparisonOperator =
                allowEqual ? VectorOperators.LE : VectorOperators.LT;

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
        if (allowEqual) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }
        } else {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }
        }

        return vectorLength;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector} with a
     * given less-than/less-than-equal condition using SIMD-ed code where only some indices of the
     * vector are valid.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param validityMask The mask into which to produce the boolean indicating the validity of
     *                     each record in the vector.
     * @param validIndicesMask The mask indicating which indices of {@code vector} are valid.
     * @param validIndicesMaskLength The length of the valid portion of {@code validIndicesMask}.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid section of {@code validityMask}.
     */
    public static int lessThanEqualSIMD(
            org.apache.arrow.vector.IntVector vector,
            int condition,
            boolean[] validityMask,
            boolean[] validIndicesMask,
            int validIndicesMaskLength,
            boolean allowEqual
    ) {
        // The operator to use
        VectorOperators.Comparison comparisonOperator =
                allowEqual ? VectorOperators.LE : VectorOperators.LT;

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
        if (allowEqual) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) <= condition);
            }
        } else {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] =
                        validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
            }
        }

        return vectorLength;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.DateDayVector}
     * with a given less-than/less-than-equal condition.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param selectionVector The vector into which to produce the indices of valid records.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid selection of {@code selectionVector}/number of records
     * matching the condition.
     */
    public static int lessThanEqual(org.apache.arrow.vector.DateDayVector vector, int condition, int[] selectionVector, boolean allowEqual) {
        int selectionVectorIndex = 0;

        if (allowEqual) {
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.get(i) <= condition)
                    selectionVector[selectionVectorIndex++] = i;
            }
        } else {
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.get(i) < condition)
                    selectionVector[selectionVectorIndex++] = i;
            }
        }

        return selectionVectorIndex;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.DateDayVector} with a
     * given less-than/less-than-equal condition using SIMD-ed code.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param validityMask The mask into which to produce the boolean indicating the validity of
     *                     each record in the vector.
     * @param allowEqual {@code true} if a less-than-equal comparison is to be performed.
     * @return The length of the valid section of {@code validityMask}.
     */
    public static int lessThanEqualSIMD(
            org.apache.arrow.vector.DateDayVector vector,
            int condition,
            boolean[] validityMask,
            boolean allowEqual
    ) {
        // The operator to use
        VectorOperators.Comparison comparisonOperator =
                allowEqual ? VectorOperators.LE : VectorOperators.LT;

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
        if (allowEqual) {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) <= condition;
            }
        } else {
            for (; currentIndex < vectorLength; currentIndex++) {
                validityMask[currentIndex] = vector.get(currentIndex) < condition;
            }
        }

        return vectorLength;
    }

}
