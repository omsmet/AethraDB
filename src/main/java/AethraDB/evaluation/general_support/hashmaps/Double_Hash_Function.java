package AethraDB.evaluation.general_support.hashmaps;

/**
 * The standard hash function used for computing the hash value of a double column.
 */
public final class Double_Hash_Function {

    /**
     * Prevent instantiation of this class.
     */
    private Double_Hash_Function() {

    }

    /**
     * Method to compute the pre-hash of a double key.
     * @param key The key to compute the pre-hash value for.
     * @return The pre-hash value.
     */
    public static long preHash(double key) {
        // TODO: consider using something more efficient
        return Double.hashCode(key);
    }

}
