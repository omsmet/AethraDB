package evaluation.vector_support;

import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static evaluation.codegen.infrastructure.context.OptimisationContext.MAX_LEN_INT_VECTOR_SPECIES;

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

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector}
     * with a given less-than condition.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param selectionVector The vector into which to produce the indices of valid records.
     * @return The length of the valid selection of {@code selectionVector}/number of records
     * matching the condition.
     */
    public static int lessThan(org.apache.arrow.vector.IntVector vector, int condition, int[] selectionVector) {
        int selectionVectorIndex = 0;

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.get(i) < condition)
                selectionVector[selectionVectorIndex++] = i;
        }

        return selectionVectorIndex;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector}
     * with a given less-than condition where only some indices of the vector are valid.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param selectionVector The vector into which to produce the indices of valid records.
     * @param validIndices The array containing the indices of {@code vector} that are valid to start from.
     * @param validIndicesCount The number of valid indices in {@code validIndices}.
     * @return The length of the valid selection of {@code selectionVector}/number of valid records
     * matching the condition.
     */
    public static int lessThan(
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

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector} with a
     * given less-than condition using SIMD-ed code.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param validityMask The mask into which to produce the boolean indicating the validity of
     *                     each record in the vector.
     * @return The length of the valid section of {@code validityMask}.
     */
    public static int lessThanSIMD(
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
        for (; currentIndex < MAX_LEN_INT_VECTOR_SPECIES.loopBound(vectorLength); currentIndex += MAX_LEN_INT_VECTOR_SPECIES.length()) {
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    MAX_LEN_INT_VECTOR_SPECIES,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            var validityMaskVector = simdVector.lt(condition);
            validityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            validityMask[currentIndex] = vector.get(currentIndex) < condition;
        }

        return vectorLength;
    }

    /**
     * Primitive for performing a selection over {@link org.apache.arrow.vector.IntVector} with a
     * given less-than condition using SIMD-ed code where only some indices of the vector are valid.
     * @param vector The vector to perform the selection over.
     * @param condition Specifies the maximum value of a valid record in the vector.
     * @param validityMask The mask into which to produce the boolean indicating the validity of
     *                     each record in the vector.
     * @param validIndicesMask The mask indicating which indices of {@code vector} are valid.
     * @param validIndicesMaskLength The length of the valid portion of {@code validIndicesMask}.
     * @return The length of the valid section of {@code validityMask}.
     */
    public static int lessThanSIMD(
            org.apache.arrow.vector.IntVector vector,
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
        for (; currentIndex < MAX_LEN_INT_VECTOR_SPECIES.loopBound(vectorLength); currentIndex += MAX_LEN_INT_VECTOR_SPECIES.length()) {
            // Initialise the SIMD vector and mask indicating the entries that are valid
            var simdVector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    MAX_LEN_INT_VECTOR_SPECIES,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);
            VectorMask<Integer> validityMaskVector = VectorMask.fromArray(
                    MAX_LEN_INT_VECTOR_SPECIES,
                    validIndicesMask,
                    currentIndex
            );

            // Compute the valid entries which match the condition as a vector mask
            var resultValidityMaskVector = simdVector.compare(
                    VectorOperators.LT,
                    condition,
                    validityMaskVector);
            resultValidityMaskVector.intoArray(validityMask, currentIndex);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            validityMask[currentIndex] =
                    validIndicesMask[currentIndex] && (vector.get(currentIndex) < condition);
        }

        return vectorLength;
    }

}
