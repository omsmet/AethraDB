package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;

/**
 * Super-type for classes that keep track of information of how to access ordinals of the logical
 * plan during the code generation process.
 */
public abstract class AccessPath {

    /**
     * The type of the variable accessible through {@code this}.
     */
    protected final QueryVariableType type;

    /**
     * Base constructor for any {@link AccessPath} path descendant.
     * @param type The type of the variable accessible through {@code this}.
     */
    public AccessPath(QueryVariableType type) {
        this.type = type;
    }

    /**
     * Method to obtain the type of the value represented by this {@link AccessPath}
     */
    public QueryVariableType getType() {
        return this.type;
    }

}
