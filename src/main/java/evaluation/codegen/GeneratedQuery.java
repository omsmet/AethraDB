package evaluation.codegen;

import evaluation.codegen.infrastructure.CodeGenContext;

/**
 * The type representing a generated query.
 */
public abstract class GeneratedQuery {

    /**
     * The {@link CodeGenContext} that was used during the generation phase of this query.
     * This context is used for accessing objects that were created at compilation time during execution.
     */
    protected CodeGenContext cCtx;

    /**
     * Creates an instance of a generated query.
     * @param cCtx The {@link CodeGenContext} that was used for generating the query.
     */
    public GeneratedQuery(CodeGenContext cCtx) {
        this.cCtx = cCtx;
    }

    /**
     * Method to execute the generated query.
     */
    public abstract void execute();

}
