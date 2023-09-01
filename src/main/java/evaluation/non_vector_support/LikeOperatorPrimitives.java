package evaluation.non_vector_support;

import sun.misc.Unsafe;

/**
 * Class containing some helper primitives for implementing the LIKE operator.
 */
public class LikeOperatorPrimitives {

    /**
     * Prevent instantiating this class.
     */
    private LikeOperatorPrimitives() {

    }

    /**
     * Method which returns whether a needle is contained in a specific section of the hay stack.
     * @param hayStack The byte array to be searched in.
     * @param needle The element to find in the {@code hayStack}.
     * @return {@code true} iff needle is contained in hayStack.
     */
    public static boolean contains(byte[] hayStack, byte[] needle) {
        int needleLength = needle.length;

        pml: for (int i = 0; i <= hayStack.length - needleLength; i++) {
            // Look for potential matches
            if (hayStack[i] == needle[0]) {
                for (int j = 1; j < needleLength; j++) {
                    if (hayStack[i + j] != needle[j]) {
                        continue pml;
                    }
                }

                // Found a match
                return true;
            }
        }

        // Found no match
        return false;

    }

}
