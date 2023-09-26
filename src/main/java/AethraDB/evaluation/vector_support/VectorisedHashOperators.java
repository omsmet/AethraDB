package AethraDB.evaluation.vector_support;

import AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function;
import AethraDB.evaluation.general_support.hashmaps.Double_Hash_Function;
import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;
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
     * Method to construct a pre-hash vector for an integer key vector, but only at the indices
     * that are indicated by a selection vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector,
            int[] selectionVector,
            int selectionVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] = Int_Hash_Function.preHash(keyVector.get(recordIndex));
            }
        } else {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] ^= Int_Hash_Function.preHash(keyVector.get(recordIndex));
            }
        }
    }

    /**
     * Method to construct a pre-hash vector for an integer key vector using SIMD acceleration.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector from which to construct the pre-hash vector.
     * @param validityMask The boolean mask indicating which entries of {@code keyVector} are valid.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            org.apache.arrow.vector.IntVector keyVector,
            boolean[] validityMask,
            int validityMaskLength,
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
            // Initialise the application mask from the validity vector
            var application_mask = jdk.incubator.vector.VectorMask.fromArray(
                    INT_SPECIES, validityMask, currentIndex);

            // First initialise the current portion of the key vector as a SIMD int vector
            var int_key_simd_vector = jdk.incubator.vector.IntVector.fromMemorySegment(
                    INT_SPECIES,
                    vectorSegment,
                    (long) currentIndex * IntVector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN,
                    application_mask
            );

            // Cast the key vector to a SIMD long vector
            var long_application_mask = application_mask.cast(LONG_SPECIES);
            var long_key_simd_vector = (jdk.incubator.vector.LongVector) int_key_simd_vector.castShape(LONG_SPECIES, 0);

            // Now perform the actual pre-hash computation
            // a * key
            var long_key_mul_a_simd_vector = long_key_simd_vector.mul(Int_Hash_Function.hashConstantA, long_application_mask);
            // a * key + b
            var long_key_mul_a_plus_b_simd_vector = long_key_mul_a_simd_vector.add(Int_Hash_Function.hashConstantB, long_application_mask);

            // Store the partial computation of the pre-hash value (still need to take it mod p)
            if (!extend)
                long_key_mul_a_plus_b_simd_vector.intoArray(preHashKeyVector, currentIndex, long_application_mask);
            else
                long_key_mul_a_plus_b_simd_vector.intoArray(tempVector, currentIndex, long_application_mask);
        }

        // Execute the mod p part for the vectorised indices
        if (!extend) {
            for (int i = 0; i < currentIndex; i++)
                if (validityMask[i])
                    preHashKeyVector[i] %= Int_Hash_Function.hashConstantP;
        } else {
            for (int i = 0; i < currentIndex; i++)
                if (validityMask[i])
                    preHashKeyVector[i] ^= (tempVector[i] % Int_Hash_Function.hashConstantP);
        }

        // Perform the tail processing
        if (!extend) {
            for (; currentIndex < vectorLength; currentIndex++)
                if (validityMask[currentIndex])
                    preHashKeyVector[currentIndex] = Int_Hash_Function.preHash(keyVector.get(currentIndex));
        } else {
            for (; currentIndex < vectorLength; currentIndex++)
                if (validityMask[currentIndex])
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
     * Method to construct a pre-hash vector for an integer key vector using SIMD acceleration.
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

    /**
     * Method to construct a pre-hash vector for a double key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            double[] keyVector,
            int keyVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] = Double_Hash_Function.preHash(keyVector[i]);
        } else {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] ^= Double_Hash_Function.preHash(keyVector[i]);
        }
    }

    /**
     * Method to construct a pre-hash vector for a double key vector using SIMD acceleration.
     * TODO: currently not SIMD.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            double[] keyVector,
            int keyVectorLength,
            boolean extend
    ) {
        constructPreHashKeyVector(preHashKeyVector, keyVector, keyVectorLength, extend);
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector, i);
        } else {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector, i);
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector[i]);
        } else {
            for (int i = 0; i < keyVectorLength; i++)
                preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector[i]);
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector using SIMD acceleration.
     * TODO: not currently using SIMD
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            boolean extend
    ) {
        constructPreHashKeyVector(preHashKeyVector, keyVector, keyVectorLength, extend);
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector,
            int[] selectionVector,
            int selectionVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] = Char_Arr_Hash_Function.preHash(keyVector, recordIndex);
            }
        } else {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] ^= Char_Arr_Hash_Function.preHash(keyVector, recordIndex);
            }
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            int[] selectionVector,
            int selectionVectorLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] = Char_Arr_Hash_Function.preHash(keyVector[recordIndex]);
            }
        } else {
            for (int i = 0; i < selectionVectorLength; i++) {
                int recordIndex = selectionVector[i];
                preHashKeyVector[recordIndex] ^= Char_Arr_Hash_Function.preHash(keyVector[recordIndex]);
            }
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector using SIMD acceleration.
     * TODO: not currently using SIMD
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            int[] selectionVector,
            int selectionVectorLength,
            boolean extend
    ) {
        constructPreHashKeyVector(preHashKeyVector, keyVector, keyVectorLength, selectionVector, selectionVectorLength, extend);
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param validityMask The boolean mask indicating which entries of {@code keyVector} are valid.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector,
            boolean[] validityMask,
            int validityMaskLength,
            boolean extend
    ) {
        byte[] byte_array_cache = getByteArrayCache(keyVector.getByteWidth());

        if (!extend) {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                if (validityMask[i])
                    preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector, i);
        } else {
            for (int i = 0; i < keyVector.getValueCount(); i++)
                if (validityMask[i])
                    preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector, i);
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param validityMask The boolean mask indicating which entries of {@code keyVector} are valid.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVector(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            boolean[] validityMask,
            int validityMaskLength,
            boolean extend
    ) {
        if (!extend) {
            for (int i = 0; i < keyVectorLength; i++)
                if (validityMask[i])
                    preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector[i]);
        } else {
            for (int i = 0; i < keyVectorLength; i++)
                if (validityMask[i])
                    preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector[i]);
        }
    }

    /**
     * Method to construct a pre-hash vector for a fixed size binary key vector using SIMD acceleration.
     * TODO: not currently using SIMD
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param validityMask The boolean mask indicating which entries of {@code keyVector} are valid.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param extend Whether the operation is extending an existing vector or not.
     */
    public static void constructPreHashKeyVectorSIMD(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            boolean[] validityMask,
            int validityMaskLength,
            boolean extend
    ) {
        constructPreHashKeyVector(preHashKeyVector, keyVector, keyVectorLength, validityMask, validityMaskLength, extend);
    }

}
