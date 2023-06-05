package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.CodeGenContext;
import evaluation.codegen.infrastructure.OptimisationContext;
import org.apache.calcite.rel.RelNode;

/**
 * Class that is extended by all code generator operators.
 * Wraps the {@link RelNode} representing the logical sub-plan that is implemented by this node.
 */
public abstract class CodeGenOperator {

    /**
     * The {@link CodeGenOperator} that consumes the result from this operator.
     */
    private CodeGenOperator parent;

    /**
     * The logical sub-plan that is represented by the query operator tree rooted at this operator.
     */
    private final RelNode logicalSubplan;

    /**
     * Initialises a new instance of a specific {@link CodeGenOperator}.
     * @param logicalSubplan The logical sub-plan that should be implemented by this operator.
     */
    protected CodeGenOperator(RelNode logicalSubplan) {
        this.parent = null;
        this.logicalSubplan = logicalSubplan;
    }

    /**
     * Obtain the logical query plan that should be implemented by this operator.
     * @return The logical query plan implemented by the query operator tree rooted at this node.
     */
    public RelNode getLogicalSubplan() {
        return this.logicalSubplan;
    }

    /**
     * Method to set the parent of this {@link CodeGenOperator}.
     * @param parent The parent of this operator.
     */
    public void setParent(CodeGenOperator parent) {
        this.parent = parent;
    }

    /**
     * Method to indicate whether the current operator can produce code for non-vectorised execution.
     */
    public abstract boolean canProduceNonVectorised();

    /**
     * Method to indicate whether the current operator can produce code for vectorised execution.
     */
    public abstract boolean canProduceVectorised();

// TODO: ensure we can really do without these operators
//
//    /**
//     * Method to indicate whether the current operator can consume code for non-vectorised execution.
//     */
//    public abstract boolean canConsumeNonVectorised();
//
//    /**
//     * Method to indicate whether the current operator can consume code for vectorised execution.
//     */
//    public abstract boolean canConsumeVectorised();

    /**
     * Method for generating non-vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     */
    public abstract void produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx);

    /**
     * Method for generating non-vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     */
    public abstract void consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx);

    /**
     * Method for generating vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     */
    public abstract void produceVec(CodeGenContext cCtx, OptimisationContext oCtx);

    /**
     * Method for generating vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     */
    public abstract void consumeVec(CodeGenContext cCtx, OptimisationContext oCtx);

}
