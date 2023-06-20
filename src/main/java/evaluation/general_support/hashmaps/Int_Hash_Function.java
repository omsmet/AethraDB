package evaluation.general_support.hashmaps;

/**
 * The standard hash function used for computing the hash value of an integer column.
 * The hash function is a fixed universal hash function (CLRS page 267) based on the maximum integer
 * value, as this is the maximum number of elements which can occur in an array.
 *
 * It uses the following constants:
 *  - p = 4 294 967 459 > Integer.MAX_VALUE
 *  - a = 3 044 339 450 (random number between 1 and p - 1)
 *  - b = 4 157 137 050 (random number between 0 and p - 1)
 */
public final class Int_Hash_Function {

    /**
     * Prevent instantiation of this class.
     */
    private Int_Hash_Function() {

    }

    /**
     * Hash constant p.
     */
    public static final long hashConstantP = 4_294_967_459L;

    /**
     * Hash constant a.
     */
    public static final long hashConstantA = 3_044_339_450L;

    /**
     * Hash constant b.
     */
    public static final long hashConstantB = 4_157_137_050L;

    /**
     * Method to compute the pre-hash of an integer key. That is, compute the value
     * {@code (a * key + b) mod p} (which is thus not truncated to the appropriate hash length.
     * @param key The key to compute teh pre-hash value for.
     * @return The pre-hash value {@code (a * key + b) mod p}.
     */
    public static long preHash(int key) {
        return (hashConstantA * key + hashConstantB) % hashConstantP;
    }

}
