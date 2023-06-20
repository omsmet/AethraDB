package evaluation.vector_support;

import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import evaluation.general_support.hashmaps.Simple_Int_Long_Map;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static evaluation.codegen.infrastructure.context.OptimisationContext.MAX_LEN_INT_VECTOR_SPECIES;
import static evaluation.codegen.infrastructure.context.OptimisationContext.MAX_LEN_LONG_VECTOR_SPECIES;

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
     * Method to construct a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid poriton of {@code keyVector}.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector.get(i));
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid poriton of {@code keyVector}.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            int[] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector[i]);
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
