package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.CodeGenContext;
import evaluation.codegen.infrastructure.OptimisationContext;
import org.apache.calcite.rel.RelNode;

/**
 * An {@link CodeGenOperator} which can be the top-level operator of a {@link CodeGenOperator} tree
 * and simply prints all results to the standard output.
 */
public class QueryResultPrinterOperator extends CodeGenOperator {

    /**
     * The {@link CodeGenOperator} producing the query result that is printed by {@code this}.
     */
    private final CodeGenOperator child;

    /**
     * Create an {@link QueryResultPrinterOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public QueryResultPrinterOperator(RelNode logicalSubplan, CodeGenOperator child) {
        super(logicalSubplan);
        this.child = child;
        this.child.setParent(this);
    }

    @Override
    public boolean canProduceNonVectorised() {
        // Since the QueryResultPrinterOperator operator will never produce a result for its
        // "parent" (as it will not have a parent) but rather print its result to the standard output
        // we say that it can produce both the vectorised and non-vectorised type.
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        // Since the QueryResultPrinterOperator operator will never produce a result for its
        // "parent" (as it will not have a parent) but rather print its result to the standard output
        // we say that it can produce both the vectorised and non-vectorised type.
        return true;
    }

    @Override
    public void produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        this.child.produceNonVec(cCtx, oCtx);
    }

    @Override
    public void consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // TODO: implement
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        this.child.produceVec(cCtx, oCtx);
    }

    @Override
    public void consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // TODO: implement
        throw new UnsupportedOperationException("Not implemented");
    }
}
