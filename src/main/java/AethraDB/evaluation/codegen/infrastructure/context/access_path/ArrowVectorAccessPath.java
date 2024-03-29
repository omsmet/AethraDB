package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * {@link AccessPath} type for accessing a variable containing an Arrow vector.
 */
public class ArrowVectorAccessPath extends AccessPath {

    /**
     * The variable that is accessible through {@code this}.
     */
    private final String variableToAccess;

    /**
     * Construct an {@link ArrowVectorAccessPath} instance for a specific variable name.
     * @param variableToAccess The variable that should be accessible through {@code this}.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrowVectorAccessPath(String variableToAccess, QueryVariableType type) {
        super(type);
        this.variableToAccess = variableToAccess;
    }

    /**
     * Method to get the name of the Arrow vector variable wrapped by {@code this}.
     * @return The name of the Arrow vector variable wrapped by {@code this}.
     */
    public String getVariableName() {
        return this.variableToAccess;
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the variable represented by {@code this}.
     */
    public Java.Rvalue read() {
        return createAmbiguousNameRef(
                getLocation(),
                this.variableToAccess
        );
    }

}
