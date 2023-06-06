package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.List;

/**
 * Class that is extended by all code generator operators.
 * Wraps the {@link RelNode} representing the logical sub-plan that is implemented by this node.
 */
public abstract class CodeGenOperator<T extends RelNode> {

    /**
     * The {@link CodeGenOperator} that consumes the result from this operator.
     */
    protected CodeGenOperator<?> parent;

    /**
     * The logical sub-plan that is represented by the query operator tree rooted at this operator.
     */
    private final T logicalSubplan;

    /**
     * Initialises a new instance of a specific {@link CodeGenOperator}.
     * @param logicalSubplan The logical sub-plan that should be implemented by this operator.
     */
    protected CodeGenOperator(T logicalSubplan) {
        this.parent = null;
        this.logicalSubplan = logicalSubplan;
    }

    /**
     * Obtain the logical query plan that should be implemented by this operator.
     * @return The logical query plan implemented by the query operator tree rooted at this node.
     */
    public T getLogicalSubplan() {
        return this.logicalSubplan;
    }

    /**
     * Method to set the parent of this {@link CodeGenOperator}.
     * @param parent The parent of this operator.
     */
    public void setParent(CodeGenOperator<?> parent) {
        this.parent = parent;
    }

    /**
     * Method to indicate whether the current operator can produce code for non-vectorised execution.
     */
    public boolean canProduceNonVectorised() {
        return false;
    };

    /**
     * Method to indicate whether the current operator can produce code for vectorised execution.
     */
    public boolean canProduceVectorised() {
        return false;
    };

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
     * @return The generated query code.
     */
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    };

    /**
     * Method for generating non-vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

    /**
     * Method for generating vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    }

    /**
     * Method for generating vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

}
