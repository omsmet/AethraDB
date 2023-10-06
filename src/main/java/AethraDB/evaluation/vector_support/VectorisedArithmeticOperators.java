package AethraDB.evaluation.vector_support;

import org.apache.arrow.vector.Float8Vector;

/**
 * Class containing vectorised primitives for arithmetic operators.
 */
public class VectorisedArithmeticOperators extends VectorisedOperators {

    /**
     * Method to multiply two double vectors.
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(Float8Vector lhsArrowVector, Float8Vector rhsArrowVector, double[] result) {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrowVector.getValueCount();

        for (int i = 0; i < vectorLength; i++) {
            result[i] = lhsArrowVector.get(i) * rhsArrowVector.get(i);
        }

        return vectorLength;
    }

    /**
     * Method to multiply two double vectors, but only at the indices indicated by a given selection vector.
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param selectionVector The vector indicating which indices the operation should be performed at.
     * @param selectionVectorLength The length of the valid portion of {@code selectionVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(
            Float8Vector lhsArrowVector,
            Float8Vector rhsArrowVector,
            int[] selectionVector,
            int selectionVectorLength,
            double[] result) {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrowVector.getValueCount();

        for (int i = 0; i < selectionVectorLength; i++) {
            int selectedIndex = selectionVector[i];
            result[selectedIndex] = lhsArrowVector.get(selectedIndex) * rhsArrowVector.get(selectedIndex);
        }

        return vectorLength;
    }

    /**
     * Method to multiply two double vectors.
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(Float8Vector lhsArrowVector, double[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrayVectorLength;

        for (int i = 0; i < vectorLength; i++) {
            result[i] = lhsArrowVector.get(i) * rhsArrayVector[i];
        }

        return vectorLength;
    }

    /**
     * Method to multiply two double vectors, but only at the indices indicated by a given selection vector.
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param selectionVector The vector indicating which indices the operation should be performed at.
     * @param selectionVectorLength The length of the valid portion of {@code selectionVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(
            Float8Vector lhsArrowVector,
            double[] rhsArrayVector,
            int rhsArrayVectorLength,
            int[] selectionVector,
            int selectionVectorLength,
            double[] result) {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrayVectorLength;

        for (int i = 0; i < selectionVectorLength; i++) {
            int selectedIndex = selectionVector[i];
            result[selectedIndex] = lhsArrowVector.get(selectedIndex) * rhsArrayVector[selectedIndex];
        }

        return vectorLength;
    }

    /**
     * Method to multiply two double vectors.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(double[] lhsArrayVector, int lhsArrayVectorLength, double[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        for (int i = 0; i < lhsArrayVectorLength; i++) {
            result[i] = lhsArrayVector[i] * rhsArrayVector[i];
        }

        return lhsArrayVectorLength;
    }

    /**
     * Method to multiply two double vectors, but only at the indices indicated by a selection vector.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiply(
            double[] lhsArrayVector,
            int lhsArrayVectorLength,
            double[] rhsArrayVector,
            int rhsArrayVectorLength,
            int[] selectionVector,
            int selectionVectorLength,
            double[] result)
    {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        for (int i = 0; i < selectionVectorLength; i++) {
            int selectedIndex = selectionVector[i];
            result[selectedIndex] = lhsArrayVector[selectedIndex] * rhsArrayVector[selectedIndex];
        }

        return lhsArrayVectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" adding a double vector to a scalar value.
     * @param lhsScalar The left-hand side scalar value to add from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int add(int lhsScalar, Float8Vector rhsArrowVector, double[] result) {
        int vectorLength = rhsArrowVector.getValueCount();

        for (int i = 0; i < vectorLength; i++) {
            result[i] = ((double) lhsScalar) + rhsArrowVector.get(i);
        }

        return vectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" adding a double vector to a scalar value, but
     * only at the indices indicating by a given selection vector.
     * @param lhsScalar The left-hand side scalar value to add from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param selectionVector The vector indicating which indices the operation should be performed at.
     * @param selectionVectorLength The length of the valid portion of {@code selectionVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int add(
            int lhsScalar,
            Float8Vector rhsArrowVector,
            int[] selectionVector,
            int selectionVectorLength,
            double[] result)
    {
        int vectorLength = rhsArrowVector.getValueCount();

        for (int i = 0; i < selectionVectorLength; i++) {
            int selectedIndex = selectionVector[i];
            result[selectedIndex] = ((double) lhsScalar) + rhsArrowVector.get(selectedIndex);
        }

        return vectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" subtracting a double vector from a scalar value.
     * @param lhsScalar The left-hand side scalar value to subtract from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int subtract(int lhsScalar, Float8Vector rhsArrowVector, double[] result) {
        int vectorLength = rhsArrowVector.getValueCount();

        for (int i = 0; i < vectorLength; i++) {
            result[i] = ((double) lhsScalar) - rhsArrowVector.get(i);
        }

        return vectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" subtracting a double vector from a scalar value
     * only at the indices of a given selection vector.
     * @param lhsScalar The left-hand side scalar value to subtract from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param selectionVector The vector indicating which indices the operation should be performed at.
     * @param selectionVectorLength The length of the valid portion of {@code selectionVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int subtract(
            int lhsScalar,
            Float8Vector rhsArrowVector,
            int[] selectionVector,
            int selectionVectorLength,
            double[] result)
    {
        int vectorLength = rhsArrowVector.getValueCount();

        for (int i = 0; i < selectionVectorLength; i++) {
            int resultIndex = selectionVector[i];
            result[resultIndex] = ((double) lhsScalar) - rhsArrowVector.get(resultIndex);
        }

        return vectorLength;
    }

    /**
     * Method to divide two vectors.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side integer vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int divide(double[] lhsArrayVector, int lhsArrayVectorLength, int[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        for (int i = 0; i < lhsArrayVectorLength; i++) {
            result[i] = lhsArrayVector[i] / rhsArrayVector[i];
        }

        return lhsArrayVectorLength;
    }

}
