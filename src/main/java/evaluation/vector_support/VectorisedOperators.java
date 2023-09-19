package evaluation.vector_support;

/**
 * Class containing general definitions for the vectorised processing support "library".
 */
public class VectorisedOperators {

    /**
     * Variable storing byte array caches of appropriate sizes.
     */
    static private byte[][] byteArrayCaches = new byte[1][0];

    /**
     * Length of vectors in the system.
     * Current value is determined as the maximum vector length of used datasets so vectors always fit.
     * TODO: maybe make less magic.
     */
    public static final int VECTOR_LENGTH = 16384;

    /**
     * Prevent this class from being instantiated.
     */
    protected VectorisedOperators() {

    }

    /**
     * Method to obtain a byte array cache of a given width.
     * @param cacheWidth The width that the byte array cache should have.
     * @return A reference to a byte array cache of the appropriate size.
     */
    static protected byte[] getByteArrayCache(int cacheWidth) {
        if (cacheWidth >= byteArrayCaches.length) {
            int newCachesArrayLength = Integer.highestOneBit(cacheWidth) << 1;
            byte[][] newByteArrayCaches = new byte[newCachesArrayLength][];
            System.arraycopy(byteArrayCaches, 0, newByteArrayCaches, 0, byteArrayCaches.length);
            byteArrayCaches = newByteArrayCaches;
        }

        if (byteArrayCaches[cacheWidth] == null) {
            byteArrayCaches[cacheWidth] = new byte[cacheWidth];
        }

        return byteArrayCaches[cacheWidth];
    }

}
