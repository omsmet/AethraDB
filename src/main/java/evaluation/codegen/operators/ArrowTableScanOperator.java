package evaluation.codegen.operators;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.infrastructure.CodeGenContext;
import evaluation.codegen.infrastructure.OptimisationContext;

/**
 * {@link CodeGenOperator} which generates code for reading data from an Arrow file.
 */
public class ArrowTableScanOperator extends CodeGenOperator {

    /**
     * Creates an {@link ArrowTableScanOperator} for a specific {@link LogicalArrowTableScan}.
     * @param logicalSubplan The {@link LogicalArrowTableScan} to create a scan operator for.
     */
    public ArrowTableScanOperator(LogicalArrowTableScan logicalSubplan) {
        super(logicalSubplan);
    }

    @Override
    public boolean canProduceNonVectorised() {
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        return true;
    }

    @Override
    public void produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // TODO: implement
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("An ArrowTableScanOperator cannot consume data");
    }

    @Override
    public void produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // TODO: implement
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("An ArrowTableScanOperator cannot consume data");
    }
}
