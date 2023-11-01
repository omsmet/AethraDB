package AethraDB.evaluation.vector_support;

import AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function;
import AethraDB.evaluation.general_support.hashmaps.Double_Hash_Function;
import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;
import org.apache.arrow.vector.IntVector;

/**
 * Class containing vectorised primitives for hashing operators.
 */
public class VectorisedHashOperators extends VectorisedOperators  {

    /**
     * Prevent instantiating this class.
     */
    private VectorisedHashOperators() {
        super();
    }

    /**
     * Method to initially construct a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            IntVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector.get(i));
    }

    /**
     * Method to extend a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            IntVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] ^= Int_Hash_Function.preHash(keyVector.get(i));
    }

    /**
     * Method to initialise a pre-hash vector for an integer key vector, but only at the indices
     * that are indicated by a selection vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            IntVector keyVector,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] = Int_Hash_Function.preHash(keyVector.get(recordIndex));
        }
    }

    /**
     * Method to extend a pre-hash vector for an integer key vector, but only at the indices
     * that are indicated by a selection vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            IntVector keyVector,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] ^= Int_Hash_Function.preHash(keyVector.get(recordIndex));
        }
    }

    /**
     * Method to initialise a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid poriton of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            int[] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] = Int_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to extend a pre-hash vector for an integer key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid poriton of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            int[] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] ^= Int_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to initialise a pre-hash vector for a double key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            double[] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] = Double_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to extend a pre-hash vector for a double key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            double[] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] ^= Double_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to initialise a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector, i);
    }

    /**
     * Method to extend a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector
    ) {
        for (int i = 0; i < keyVector.getValueCount(); i++)
            preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector, i);
    }

    /**
     * Method to initialise a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] = Char_Arr_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to extend a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength
    ) {
        for (int i = 0; i < keyVectorLength; i++)
            preHashKeyVector[i] ^= Char_Arr_Hash_Function.preHash(keyVector[i]);
    }

    /**
     * Method to initialise a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] = Char_Arr_Hash_Function.preHash(keyVector, recordIndex);
        }
    }

    /**
     * Method to extend a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the pre-hash vector.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            org.apache.arrow.vector.FixedSizeBinaryVector keyVector,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] ^= Char_Arr_Hash_Function.preHash(keyVector, recordIndex);
        }
    }

    /**
     * Method to initialise a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorInit(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] = Char_Arr_Hash_Function.preHash(keyVector[recordIndex]);
        }
    }

    /**
     * Method to extend a pre-hash vector for a fixed size binary key vector.
     * @param preHashKeyVector The pre-hash vector to construct.
     * @param keyVector The key vector for which to construct the value vector.
     * @param keyVectorLength The length of the valid portion of {@code keyVector}.
     * @param selectionVector The vector indicating the valid indices of {@code keyVector}.
     * @param selectionVectorLength The length of the valid portion of (@code selectionVector}.
     */
    public static void constructPreHashKeyVectorExtend(
            long[] preHashKeyVector,
            byte[][] keyVector,
            int keyVectorLength,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        for (int i = 0; i < selectionVectorLength; i++) {
            int recordIndex = selectionVector[i];
            preHashKeyVector[recordIndex] ^= Char_Arr_Hash_Function.preHash(keyVector[recordIndex]);
        }
    }

}
