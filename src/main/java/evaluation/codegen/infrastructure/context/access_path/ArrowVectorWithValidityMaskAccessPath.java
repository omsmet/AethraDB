package evaluation.codegen.infrastructure.context.access_path;

import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * {@link AccessPath} type for accessing an Arrow vector where only some values are valid as
 * indicated by a validity mask of type boolean[].
 */
public class ArrowVectorWithValidityMaskAccessPath extends AccessPath {

    /**
     * The name of the Arrow vector variable accessible through {@code this}.
     */
    private final String arrowVectorVariable;

    /**
     * The name of the validity mask variable indicating the validity of records.
     */
    private final String validityMaskVariable;

    /**
     * The name of the variable representing the length of the valid portion of the validity mask.
     */
    private final String validityMaskLengthVariable;

    /**
     * Construct an {@link ArrowVectorWithValidityMaskAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to use.
     * @param validityMaskVariable The validity mask variable to use.
     * @param validityMaskLengthVariable The validity mask length variable to use.
     */
    public ArrowVectorWithValidityMaskAccessPath(
            String arrowVectorVariable,
            String validityMaskVariable,
            String validityMaskLengthVariable
    ) {
        this.arrowVectorVariable = arrowVectorVariable;
        this.validityMaskVariable = validityMaskVariable;
        this.validityMaskLengthVariable = validityMaskLengthVariable;
    }

    /**
     * Method to get the name of the Arrow vector variable wrapped by {@code this}.
     * @return The name of the Arrow vector variable wrapped by {@code this}.
     */
    public String getArrowVectorVariable() {
        return this.arrowVectorVariable;
    }

    /**
     * Create an access path to the Arrow vector variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the Arrow vector represented by {@code this}.
     */
    public Java.Rvalue readArrowVector() {
        return createAmbiguousNameRef(
                getLocation(),
                this.arrowVectorVariable
        );
    }

    /**
     * Create an access path to the validity mask variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask represented by {@code this}.
     */
    public Java.Rvalue readValidityMask() {
        return createAmbiguousNameRef(
                getLocation(),
                this.validityMaskVariable
        );
    }

    /**
     * Create an access path to the validity mask length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask length represented by {@code this}.
     */
    public Java.Rvalue readValidityMaskLength() {
        return createAmbiguousNameRef(
                getLocation(),
                this.validityMaskLengthVariable
        );
    }

}
