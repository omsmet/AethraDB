package evaluation.vector_support;

import evaluation.general_support.hashmaps.Int_Hash_Function;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * Class containing vectorised primitives for hashing operators.
 */
public class VectorisedHashOperators extends VectorisedOperators  {

    /**
     * Vector for temporarily storing longs if required by some method contained in this class.
     */
    private static long[] tempVector = new long[VECTOR_LENGTH];

    /**
     * Prevent instantiating this class.
     */
    private VectorisedHashOperators() {
        super();
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector.get(i));
        } else {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                preHashKeyVector[i] ^= Int_Hash_Function.preHash(keyVector.get(i));
        }
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector using SIMD acceleration.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector from which to construct the pre-hash vector.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector,
            boolean extend
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
            if (!extend)
                long_key_mul_a_plus_b_simd_vector.intoArray(preHashKeyVector, currentIndex);
            else
                long_key_mul_a_plus_b_simd_vector.intoArray(tempVector, currentIndex);
        }

        // Execute the mod p part for the vectorised indices
        if (!extend) {
            for (int i = 0; i < currentIndex; i++)
                preHashKeyVector[i] %= Int_Hash_Function.hashConstantP;
        } else {
            for (int i = 0; i < currentIndex; i++)
                preHashKeyVector[i] ^= (tempVector[i] % Int_Hash_Function.hashConstantP);
        }

        // Perform the tail processing
        if (!extend) {
            for (; currentIndex < vectorLength; currentIndex++)
                preHashKeyVector[currentIndex] = Int_Hash_Function.preHash(keyVector.get(currentIndex));
        } else {
            for (; currentIndex < vectorLength; currentIndex++)
                preHashKeyVector[currentIndex] ^= Int_Hash_Function.preHash(keyVector.get(currentIndex));
        }
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid poriton of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            int[] keyVector,
            int keyVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector[i]);
        } else {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] ^= Int_Hash_Function.preHash(keyVector[i]);
        }
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector using SIMD accelleration.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            int[] keyVector,
            int keyVectorLength,
            boolean extend
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

        // Perform vectorised pre-hashing
        int currentIndex = 0;
        for (; currentIndex < INT_SPECIES.loopBound(keyVectorLength); currentIndex += INT_SPECIES.length()) {
            // First initialise the current portion of the key vector as a SIMD int vector
            var int_key_simd_vector = jdk.incubator.vector.IntVector.fromArray(
                    INT_SPECIES,
                    keyVector,
                    currentIndex
            );

            // Cast the key vector to a SIMD long vector
            var long_key_simd_vector = (jdk.incubator.vector.LongVector) int_key_simd_vector.castShape(LONG_SPECIES, 0);

            // Now perform the actual pre-hash computation
            // a * key
            var long_key_mul_a_simd_vector = long_key_simd_vector.mul(Int_Hash_Function.hashConstantA);
            // a * key + b
            var long_key_mul_a_plus_b_simd_vector = long_key_mul_a_simd_vector.add(Int_Hash_Function.hashConstantB);

            // Store the partial computation of the pre-hash value (still need to take it mod p)
            if (!extend) {
                long_key_mul_a_plus_b_simd_vector.intoArray(preHashKeyVector, currentIndex);
            } else {
                long_key_mul_a_plus_b_simd_vector.intoArray(tempVector, currentIndex);
            }
        }

        // Execute the mod p part for the vectorised indices
        if (!extend) {
            for (int i = 0; i < currentIndex; i++)
                preHashKeyVector[i] %= Int_Hash_Function.hashConstantP;
        } else {
            for (int i = 0; i < currentIndex; i++)
                preHashKeyVector[i] ^= (tempVector[i] % Int_Hash_Function.hashConstantP);
        }

        // Perform the tail processing
        if (!extend) {
            for (; currentIndex < keyVectorLength; currentIndex++)
                preHashKeyVector[currentIndex] = Int_Hash_Function.preHash(keyVector[currentIndex]);
        } else {
            for (; currentIndex < keyVectorLength; currentIndex++)
                preHashKeyVector[currentIndex] ^= Int_Hash_Function.preHash(keyVector[currentIndex]);
        }
    }

}
