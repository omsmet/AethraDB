package evaluation.vector_support;

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

}
