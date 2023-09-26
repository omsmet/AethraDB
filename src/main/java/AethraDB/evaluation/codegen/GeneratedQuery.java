package AethraDB.evaluation.codegen;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;

import java.io.IOException;

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
     * The {@link OptimisationContext} that was used during the generation phase of this query.
     * This context is used for performing optimisations during compilation and execution time.
     */
    protected OptimisationContext oCtx;

    /**
     * Creates an instance of a generated query.
     * @param cCtx The {@link CodeGenContext} that was used for generating the query.
     * @param oCtx The {@link OptimisationContext} that was used for generating the query.
     */
    public GeneratedQuery(CodeGenContext cCtx, OptimisationContext oCtx) {
        this.cCtx = cCtx;
        this.oCtx = oCtx;
    }

    /**
     * Method to execute the generated query.
     * @throws IOException when an I/O issue occurs during query execution.
     */
    public abstract void execute() throws IOException;

    /**
     * Obtain the {@link CodeGenContext} belonging to this query.
     * @return the {@link CodeGenContext} belonging to this query.
     */
    public CodeGenContext getCCtx() {
        return this.cCtx;
    }

    /**
     * Obtain the {@link OptimisationContext} belonging to this query.
     * @return the {@link OptimisationContext} belonging to this query.
     */
    public OptimisationContext getOCtx() {
        return this.oCtx;
    }

}
