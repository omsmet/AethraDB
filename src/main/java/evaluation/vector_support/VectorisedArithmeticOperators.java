package evaluation.vector_support;

import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.Float8Vector;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * Class containing vectorised primitives for arithmetic operators.
 */
public class VectorisedArithmeticOperators extends VectorisedOperators {

    /**
     * The {@link VectorSpecies<Double>} to use for the SIMD-ed primitives in this class.
     */
    private static final VectorSpecies<Double> DOUBLE_SPECIES_PREFERRED =
            jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;

    /**
     * The {@link VectorSpecies<Integer>} to use for the SIMD-ed primitives in this class.
     */
    private static final VectorSpecies<Integer> INTEGER_SPECIES_PREFERRED =
            jdk.incubator.vector.IntVector.SPECIES_PREFERRED;

    /**
     * The {@link VectorSpecies<Integer>} to use for SIMD-ed primitives in this class when in
     * combined operation with doubles.
     */
    private static final VectorSpecies<Integer> INTEGER_SPECIES_DOUBLE_PREFERRED =
            (jdk.incubator.vector.IntVector.SPECIES_512.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_512
          : (jdk.incubator.vector.IntVector.SPECIES_256.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_256
          : (jdk.incubator.vector.IntVector.SPECIES_128.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_128
          : (jdk.incubator.vector.IntVector.SPECIES_64.length()  == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_64
          : null; // should not happen

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
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiplySIMD(Float8Vector lhsArrowVector, double[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrayVectorLength;

        // Initialise the memory segment
        long lhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment lhsMemorySegment = MemorySegment.ofAddress(lhsArrowVector.getDataBufferAddress(), lhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    lhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsArrayVector,
                    currentIndex);

            var resultSIMDVector = lhsSIMDVector.mul(rhsSIMDVector);
            resultSIMDVector.intoArray(result, currentIndex);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            result[currentIndex] = lhsArrowVector.get(currentIndex) * rhsArrayVector[currentIndex];
        }

        return vectorLength;
    }

    /**
     * Method to multiply two double vectors, but only at the indices indicated by a given validity mask.
     * @param lhsArrowVector The left-hand side double vector, represented as an arrow vector.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param validityMask The mask indicating which indices the operation should be performed at.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiplySIMD(
            Float8Vector lhsArrowVector,
            double[] rhsArrayVector,
            int rhsArrayVectorLength,
            boolean[] validityMask,
            int validityMaskLength,
            double[] result)
    {
        int vectorLength = lhsArrowVector.getValueCount();
        assert vectorLength == rhsArrayVectorLength;

        // Initialise the memory segment
        long lhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment lhsMemorySegment = MemorySegment.ofAddress(lhsArrowVector.getDataBufferAddress(), lhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var applicationMask = DOUBLE_SPECIES_PREFERRED.loadMask(validityMask, currentIndex);

            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    lhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN,
                    applicationMask);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsArrayVector,
                    currentIndex,
                    applicationMask);

            var resultSIMDVector = lhsSIMDVector.mul(rhsSIMDVector, applicationMask);
            resultSIMDVector.intoArray(result, currentIndex, applicationMask);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            if (validityMask[currentIndex])
                result[currentIndex] = lhsArrowVector.get(currentIndex) * rhsArrayVector[currentIndex];
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
     * Method to multiply two double vectors.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiplySIMD(double[] lhsArrayVector, int lhsArrayVectorLength, double[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(lhsArrayVectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    lhsArrayVector,
                    currentIndex);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsArrayVector,
                    currentIndex);

            var resultSIMDVector = lhsSIMDVector.mul(rhsSIMDVector);
            resultSIMDVector.intoArray(result, currentIndex);
        }

        // Process the tail
        for (; currentIndex < lhsArrayVectorLength; currentIndex++) {
            result[currentIndex] = lhsArrayVector[currentIndex] * rhsArrayVector[currentIndex];
        }

        return lhsArrayVectorLength;
    }

    /**
     * Method to multiply two double vectors, but only at the indices indicated by a given validity mask.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side double vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param validityMask The mask indicating which indices the operation should be performed at.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int multiplySIMD(
            double[] lhsArrayVector,
            int lhsArrayVectorLength,
            double[] rhsArrayVector,
            int rhsArrayVectorLength,
            boolean[] validityMask,
            int validityMaskLength,
            double[] result)
    {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(lhsArrayVectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var applicationMask = DOUBLE_SPECIES_PREFERRED.loadMask(validityMask, currentIndex);

            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    lhsArrayVector,
                    currentIndex,
                    applicationMask);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsArrayVector,
                    currentIndex,
                    applicationMask);

            var resultSIMDVector = lhsSIMDVector.mul(rhsSIMDVector, applicationMask);
            resultSIMDVector.intoArray(result, currentIndex, applicationMask);
        }

        // Process the tail
        for (; currentIndex < lhsArrayVectorLength; currentIndex++) {
            result[currentIndex] = lhsArrayVector[currentIndex] * rhsArrayVector[currentIndex];
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
     * Method to create a double vector by "pairwise" adding a double vector to a scalar value.
     * @param lhsScalar The left-hand side scalar value to add from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int addSIMD(int lhsScalar, Float8Vector rhsArrowVector, double[] result) {
        int vectorLength = rhsArrowVector.getValueCount();

        // Initialise the memory segment
        long rhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment rhsMemorySegment = MemorySegment.ofAddress(rhsArrowVector.getDataBufferAddress(), rhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.broadcast(
                    DOUBLE_SPECIES_PREFERRED,
                    (double) lhsScalar);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var resultSIMDVector = lhsSIMDVector.add(rhsSIMDVector);
            resultSIMDVector.intoArray(result, currentIndex);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            result[currentIndex] = ((double) lhsScalar) + rhsArrowVector.get(currentIndex);
        }

        return vectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" adding a double vector to a scalar value, but
     * only at the indices indicated by a given validity mask.
     * @param lhsScalar The left-hand side scalar value to add from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param validityMask The mask indicating which indices the operation should be performed at.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int addSIMD(
            int lhsScalar,
            Float8Vector rhsArrowVector,
            boolean[] validityMask,
            int validityMaskLength,
            double[] result)
    {
        int vectorLength = rhsArrowVector.getValueCount();

        // Initialise the memory segment
        long rhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment rhsMemorySegment = MemorySegment.ofAddress(rhsArrowVector.getDataBufferAddress(), rhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var applicationMask = DOUBLE_SPECIES_PREFERRED.loadMask(validityMask, currentIndex);

            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.broadcast(
                    DOUBLE_SPECIES_PREFERRED,
                    (double) lhsScalar);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN,
                    applicationMask);

            var resultSIMDVector = lhsSIMDVector.add(rhsSIMDVector, applicationMask);
            resultSIMDVector.intoArray(result, currentIndex, applicationMask);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            result[currentIndex] = ((double) lhsScalar) + rhsArrowVector.get(currentIndex);
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
     * Method to create a double vector by "pairwise" subtracting a double vector from a scalar value.
     * @param lhsScalar The left-hand side scalar value to subtract from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int subtractSIMD(int lhsScalar, Float8Vector rhsArrowVector, double[] result) {
        int vectorLength = rhsArrowVector.getValueCount();

        // Initialise the memory segment
        long rhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment rhsMemorySegment = MemorySegment.ofAddress(rhsArrowVector.getDataBufferAddress(), rhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.broadcast(
                    DOUBLE_SPECIES_PREFERRED,
                    (double) lhsScalar);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN);

            var resultSIMDVector = lhsSIMDVector.sub(rhsSIMDVector);
            resultSIMDVector.intoArray(result, currentIndex);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            result[currentIndex] = ((double) lhsScalar) - rhsArrowVector.get(currentIndex);
        }

        return vectorLength;
    }

    /**
     * Method to create a double vector by "pairwise" subtracting a double vector from a scalar value
     * only at the indices indicated by a given validity mask.
     * @param lhsScalar The left-hand side scalar value to subtract from.
     * @param rhsArrowVector The right-hand side double vector, represented as an arrow vector.
     * @param validityMask The mask indicating which indices the operation should be performed at.
     * @param validityMaskLength The length of the valid portion of {@code validityMask}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int subtractSIMD(
            int lhsScalar,
            Float8Vector rhsArrowVector,
            boolean[] validityMask,
            int validityMaskLength,
            double[] result)
    {
        int vectorLength = rhsArrowVector.getValueCount();

        // Initialise the memory segment
        long rhsBufferSize = (long) vectorLength * Float8Vector.TYPE_WIDTH;
        MemorySegment rhsMemorySegment = MemorySegment.ofAddress(rhsArrowVector.getDataBufferAddress(), rhsBufferSize);

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(vectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var applicationMask = DOUBLE_SPECIES_PREFERRED.loadMask(validityMask, currentIndex);

            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.broadcast(
                    DOUBLE_SPECIES_PREFERRED,
                    (double) lhsScalar);

            var rhsSIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES_PREFERRED,
                    rhsMemorySegment,
                    (long) currentIndex * Float8Vector.TYPE_WIDTH,
                    ByteOrder.LITTLE_ENDIAN,
                    applicationMask);

            var resultSIMDVector = lhsSIMDVector.sub(rhsSIMDVector, applicationMask);
            resultSIMDVector.intoArray(result, currentIndex, applicationMask);
        }

        // Process the tail
        for (; currentIndex < vectorLength; currentIndex++) {
            if (validityMask[currentIndex])
                result[currentIndex] = ((double) lhsScalar) - rhsArrowVector.get(currentIndex);
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

    /**
     * Method to divide two vectors using SIMD acceleration.
     * @param lhsArrayVector The left-hand side double vector, represented as an array vector.
     * @param lhsArrayVectorLength The length of the valid portion of {@code lhsArrayVector}.
     * @param rhsArrayVector The right-hand side integer vector, represented as an array.
     * @param rhsArrayVectorLength The length of the valid portion of {@code rhsArrayVector}.
     * @param result The array to which the result should be written.
     * @return The length of the valid portion of {@code result}.
     */
    public static int divideSIMD(double[] lhsArrayVector, int lhsArrayVectorLength, int[] rhsArrayVector, int rhsArrayVectorLength, double[] result) {
        assert lhsArrayVectorLength == rhsArrayVectorLength;

        // Perform vectorised processing
        int currentIndex = 0;
        for(; currentIndex < DOUBLE_SPECIES_PREFERRED.loopBound(lhsArrayVectorLength); currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
            var lhsSIMDVector = jdk.incubator.vector.DoubleVector.fromArray(
                    DOUBLE_SPECIES_PREFERRED,
                    lhsArrayVector,
                    currentIndex);

            var rhsSIMDVectorInt = jdk.incubator.vector.IntVector.fromArray(
                    INTEGER_SPECIES_DOUBLE_PREFERRED,
                    rhsArrayVector,
                    currentIndex);
            var rhsSIMDVectorDouble = rhsSIMDVectorInt.castShape(DOUBLE_SPECIES_PREFERRED, 0);

            var resultSIMDVector = lhsSIMDVector.div(rhsSIMDVectorDouble);
            resultSIMDVector.intoArray(result, currentIndex);
        }

        // Process the tail
        for (; currentIndex < lhsArrayVectorLength; currentIndex++) {
            result[currentIndex] = lhsArrayVector[currentIndex] / rhsArrayVector[currentIndex];
        }

        return lhsArrayVectorLength;
    }

}
