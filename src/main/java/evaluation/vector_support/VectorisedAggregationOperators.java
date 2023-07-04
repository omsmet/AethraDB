package evaluation.vector_support;

import evaluation.general_support.hashmaps.Simple_Int_Long_Map;

/**
 * Class containing vectorised primitives for aggregation operators.
 */
public class VectorisedAggregationOperators extends VectorisedOperators {

    /**
     * Prevent instantiating this class.
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

    /**
     * Method to maintain a SUM Group-By aggregate.
     * @param keyVector The vector containing the group-by key.
     * @param preHashKeyVector The vector containing the pre-hash value of each key.
     * @param valueVector The vector containing the values to sum.
     * @param sumMap The hashmap to maintain.
     */
    public static void maintainSum(
            org.apache.arrow.vector.IntVector keyVector,
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector valueVector,
            Simple_Int_Long_Map sumMap)
    {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            sumMap.addToKeyOrPutIfNotExist(keyVector.get(i), preHashKeyVector[i], valueVector.get(i));
    }

    /**
     * Method to construct a key vector from a {@link Simple_Int_Long_Map.Simple_Int_Long_Map_Iterator}.
     * @param keyVector The key vector to construct.
     * @param keyIterator The iterator to construct the vector from.
     * @return The length of the valid portion of {@code keyVector}.
     */
    public static int constructKeyVector(
            int[] keyVector,
            Simple_Int_Long_Map.Simple_Int_Long_Map_Iterator keyIterator)
    {
        int currentIndex = 0;
        while (keyIterator.hasNext() && currentIndex < VECTOR_LENGTH) {
            keyVector[currentIndex++] = keyIterator.next();
        }
        return currentIndex;
    }

    /**
     * Method to construct a value vector from a {@link Simple_Int_Long_Map}.
     * @param valueVector The value vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param intLongMap The map from which to obtain the values to add to the value vector.
     */
    public static void constructValueVector(
            long[] valueVector,
            int[] keyVector,
            long[] preHashKeyVector,
            int keyVectorLength,
            Simple_Int_Long_Map intLongMap)
    {
        for (int i = 0; i < keyVectorLength; i++)
            valueVector[i] = intLongMap.get(keyVector[i], preHashKeyVector[i]);
    }

}
