package evaluation.codegen.infrastructure.context.access_path;

import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * {@link AccessPath} type for accessing an Arrow vector where only some values are valid as
 * indicated by a selection vector of type int[].
 */
public class ArrowVectorWithSelectionVectorAccessPath extends AccessPath {

    /**
     * The name of the Arrow vector variable accessible through {@code this}.
     */
    private final String arrowVectorVariable;

    /**
     * The name of the selection vector variable indicating the validity of records.
     */
    private final String selectionVectorVariable;

    /**
     * The name of the variable representing the length of the selection vector.
     */
    private final String selectionVectorLengthVariable;

    /**
     * Construct an {@link ArrowVectorWithSelectionVectorAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to use.
     * @param selectionVectorVariable The selection vector variable to use.
     * @param selectionVectorLengthVariable The selection vector length variable to use.
     */
    public ArrowVectorWithSelectionVectorAccessPath(
            String arrowVectorVariable,
            String selectionVectorVariable,
            String selectionVectorLengthVariable
    ) {
        this.arrowVectorVariable = arrowVectorVariable;
        this.selectionVectorVariable = selectionVectorVariable;
        this.selectionVectorLengthVariable = selectionVectorLengthVariable;
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
     * Create an access path to the selection vector variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector represented by {@code this}.
     */
    public Java.Rvalue readSelectionVector() {
        return createAmbiguousNameRef(
                getLocation(),
                this.selectionVectorVariable
        );
    }

    /**
     * Create an access path to the selection vector length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector length represented by {@code this}.
     */
    public Java.Rvalue readSelectionVectorLength() {
        return createAmbiguousNameRef(
                getLocation(),
                this.selectionVectorLengthVariable
        );
    }

}
