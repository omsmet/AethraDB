package evaluation.codegen.infrastructure.context.access_path;

import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * {@link AccessPath} type for accessing java primitive types stored in a variable with a given name.
 */
public class ScalarVariableAccessPath extends AccessPath {

    /**
     * The variable that is accessible through {@code this}.
     */
    private final String variableToAccess;

    /**
     * Construct an {@link ScalarVariableAccessPath} instance for a specific variable name.
     * @param variableToAccess The variable that should be accessible through {@code this}.
     */
    public ScalarVariableAccessPath(String variableToAccess) {
        this.variableToAccess = variableToAccess;
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
