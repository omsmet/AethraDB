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

}
