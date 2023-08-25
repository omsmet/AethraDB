package evaluation.vector_support;

public class VectorisedPrintOperators extends VectorisedOperators {

    /**
     * Primitive for printing a {@link org.apache.arrow.vector.IntVector} to the standard output.
     * @param vector The vector to output.
     */
    public static void print(org.apache.arrow.vector.IntVector vector) {
        System.out.print("[");
        int lastVectorIndex = vector.getValueCount() - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(vector.get(i) + ", ");
        System.out.println(vector.get(lastVectorIndex) + "]");
    }

    /**
     * Primitive for printing a {@link org.apache.arrow.vector.FixedSizeBinaryVector} to the standard output.
     * @param vector The vector to output.
     */
    public static void print(org.apache.arrow.vector.FixedSizeBinaryVector vector) {
        System.out.print("[");
        int lastVectorIndex = vector.getValueCount() - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(new String(vector.get(i)) + ", ");
        System.out.println(new String(vector.get(lastVectorIndex)) + "]");
    }

    /**
     * Primitive for printing a {@link org.apache.arrow.vector.IntVector} to the standard output
     * when only some entries are valid as specified by a selection vector.
     * @param vector The vector to output.
     * @param selectionVector The selection vector indicating the valid entries.
     * @param selectionVectorLength The length of the selection vector.
     */
    public static void print(
            org.apache.arrow.vector.IntVector vector,
            int[] selectionVector,
            int selectionVectorLength
    ) {
        System.out.print("[");
        int lastSelectionVectorIndex = selectionVectorLength  -1;
        for (int i = 0; i < lastSelectionVectorIndex; i++) {
            int vectorIndex = selectionVector[i];
            System.out.print(vector.get(vectorIndex) + ", ");
        }
        System.out.println(vector.get(selectionVector[lastSelectionVectorIndex]) + "]");
    }

    /**
     * Primitive for printing a {@link org.apache.arrow.vector.IntVector} to the standard output
     * when only some entries are valid as specified by a validity mask.
     * @param vector The vector to output.
     * @param validityMask The validity mask indicating the valid entries.
     * @param validityMaskLength The length of the valid portion of the validity mask.
     */
    public static void print(
            org.apache.arrow.vector.IntVector vector,
            boolean[] validityMask,
            int validityMaskLength
    ) {
        System.out.print("[");
        boolean entryHasBeenPrinted = false;

        for (int i = 0; i < validityMaskLength; i++) {
            if (!validityMask[i])
                continue;

            if (entryHasBeenPrinted)
                System.out.print(", " + vector.get(i));
            else {
                System.out.print(vector.get(i));
                entryHasBeenPrinted = true;
            }
        }

        System.out.println("]");
    }

    /**
     * Primitive for printing a primitive array representing a vector to the standard output.
     * @param vector The vector to output.
     * @param vectorLength The length of the valid portion of {@code vector} to print.
     */
    public static void print(double[] vector, int vectorLength) {
        System.out.print("[");
        int lastVectorIndex = vectorLength - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(String.format("%.2f",vector[i]) + ", ");
        System.out.println(String.format("%.2f",vector[lastVectorIndex]) + "]");
    }

    /**
     * Primitive for printing a primitive array representing a vector to the standard output.
     * @param vector The vector to output.
     * @param vectorLength The length of the valid portion of {@code vector} to print.
     */
    public static void print(int[] vector, int vectorLength) {
        System.out.print("[");
        int lastVectorIndex = vectorLength - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(vector[i] + ", ");
        System.out.println(vector[lastVectorIndex] + "]");
    }

    /**
     * Primitive for printing a primitive array representing a vector to the standard output.
     * @param vector The vector to output.
     * @param vectorLength The length of the valid portion of {@code vector} to print.
     */
    public static void print(long[] vector, int vectorLength) {
        System.out.print("[");
        int lastVectorIndex = vectorLength - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(vector[i] + ", ");
        System.out.println(vector[lastVectorIndex] + "]");
    }

    /**
     * Primitive for printing a nested byte array representing a vector to the standard output.
     * @param vector The vector to output.
     * @param vectorLength The length of the valid portion of {@code vector} to print.
     */
    public static void print(byte[][] vector, int vectorLength) {
        System.out.print("[");
        int lastVectorIndex = vectorLength - 1;
        for (int i = 0; i < lastVectorIndex; i++)
            System.out.print(new String(vector[i]) + ", ");
        System.out.println(new String(vector[lastVectorIndex]) + "]");
    }

}
