package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing a variable containing a hashmap. The requirement is that
 * any wrapped variable by an {@link AccessPath} of this type should expose a "get" method taking
 * a single key parameter.
 */
public class MapAccessPath extends AccessPath {

    /**
     * The variable that is accessible through {@code this}.
     */
    private final String variableToAccess;

    /**
     * Construct an {@link MapAccessPath} instance for a specific variable name.
     * @param variableToAccess The variable that should be accessible through {@code this}.
     * @param type The type of the variable accessible through {@code this}.
     */
    public MapAccessPath(String variableToAccess, QueryVariableType type) {
        super(type);
        this.variableToAccess = variableToAccess;
    }

    /**
     * Method to obtain the variable name of the variable that is wrapped by {@code this}.
     * @return The name of the variable wrapped by {@code this}.
     */
    public String getVariableName() {
        return this.variableToAccess;
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the variable represented by {@code this}.
     */
    public Java.Rvalue read() {
        return JaninoGeneralGen.createAmbiguousNameRef(
                JaninoGeneralGen.getLocation(),
                this.variableToAccess
        );
    }

}
