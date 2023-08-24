package evaluation.general_support.hashmaps;

/**
 * The standard hash function used for computing the hash value of a character array column.
 */
public final class Char_Arr_Hash_Function {

    /**
     * Prevent instantiation of this class.
     */
    private Char_Arr_Hash_Function() {

    }

    /**
     * Method to compute the pre-hash of an character array key.
     * @param key The key to compute teh pre-hash value for.
     * @return The pre-hash value.
     */
    public static long preHash(byte[] key) {
        long hash = 0L;
        for (int i = 0; i < key.length; i++)
            hash = (hash * 31) ^ key[i];

        return (hash < 0) ? (-hash) :  hash;
    }

}
