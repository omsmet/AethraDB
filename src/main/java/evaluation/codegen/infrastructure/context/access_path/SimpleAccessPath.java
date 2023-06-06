package evaluation.codegen.infrastructure.context.access_path;

import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * {@link AccessPath} type for simply accessing the value stored in a variable with a given name.
 */
public class SimpleAccessPath extends AccessPath {

    /**
     * The variable that is access through {@code this}.
     */
    private final String variableToAccess;

    /**
     * Construct an {@link SimpleAccessPath} instance for a specific variable name.
     * @param variableToAccess The variable that should be accessible through {@code this}.
     */
    public SimpleAccessPath(String variableToAccess) {
        this.variableToAccess = variableToAccess;
    }

    @Override
    public Java.Rvalue read() {
        return createAmbiguousNameRef(
                getLocation(),
                this.variableToAccess
        );
    }
}
