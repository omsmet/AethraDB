package evaluation.vector_support;

/**
 * Class containing vectorised primitives for aggregation operators.
 */
public class VectorisedAggregationOperators extends VectorisedOperators {

    /**
     * Prevent instantiating this class
     */
    private VectorisedAggregationOperators() {
        super();
    }

    /**
     * Method counting the number of valid entries in a validity mask.
     * @param validityMask The mask to count the number of valid entries in.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @return The number of valid entries in {@code validityMask}.
     */
    public static int count(boolean[] validityMask, int validityMaskLength) {
        int validCount = 0;
        for (int i = 0; i < validityMaskLength; i++)
            if (validityMask[i])
                validCount++;
        return validCount;
    }

}
