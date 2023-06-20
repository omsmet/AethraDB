package evaluation.vector_support;

import evaluation.general_support.hashmaps.Int_Hash_Function;
import evaluation.general_support.hashmaps.Simple_Int_Long_Map;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

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
     * @param keyVector The key vector for which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector.get(i));
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector using SIMD acceleration.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector from which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector
    ) {
        // Compute the required vector species for the processing
        VectorSpecies<Long> LONG_SPECIES = jdk.incubator.vector.LongVector.SPECIES_PREFERRED;
        int vectorElementLength = LONG_SPECIES.length();
        VectorSpecies<Integer> INT_SPECIES;

        if (vectorElementLength == jdk.incubator.vector.IntVector.SPECIES_64.length())
            INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_64;
        else if (vectorElementLength == jdk.incubator.vector.IntVector.SPECIES_128.length())
            INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_128;
        else if (vectorElementLength == jdk.incubator.vector.IntVector.SPECIES_256.length())
            INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_256;
        else if (vectorElementLength == jdk.incubator.vector.IntVector.SPECIES_512.length())
            INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_512;
        else
            throw new IllegalArgumentException("Cannot determine Integer VectorSpecies for provided length");

        // Initialise the memory segment
        int vectorLength = keyVector.getValueCount();
        long bufferSize = (long) vectorLength * IntVector.TYPE_WIDTH;
        MemorySegment vectorSegment = MemorySegment.ofAddress(keyVector.getDataBufferAddress(), bufferSize);

        // Perform vectorised pre-hashing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES.loopBound(vectorLength); currentIndex += INT_SPECIES.length()) {
            // First initialise the current portion of the key vector as a SIMD int vector
            var int_key_simd_vector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN
            );

            // Cast the key vector to a SIMD long vector
            var long_key_simd_vector = (jdk.incubator.vector.LongVector) int_key_simd_vector.castShape(LONG_SPECIES, 0);

            // Now perform the actual pre-hash computation
            // a * key
            var long_key_mul_a_simd_vector = long_key_simd_vector.mul(Int_Hash_Function.hashConstantA);
            // a * key + b
            var long_key_mul_a_plus_b_simd_vector = long_key_mul_a_simd_vector.add(Int_Hash_Function.hashConstantB);

            // Store the partial computation of the pre-hash value (still need to take it mod p)
            long_key_mul_a_plus_b_simd_vector.intoArray(preHashKeyVector, currentIndex);
        }

        // Execute the mod p part for the vectorised indices
        for (int i = 0; i < currentIndex; i++)
            preHashKeyVector[i] %= Int_Hash_Function.hashConstantP;

        // Perform the tail processing
        for (; currentIndex < vectorLength; currentIndex++)
            preHashKeyVector[currentIndex] = Int_Hash_Function.preHash(keyVector.get(currentIndex));
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
