package evaluation.codegen;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.operators.AggregationOperator;
import evaluation.codegen.operators.ArrowTableScanOperator;
import evaluation.codegen.operators.CodeGenOperator;
import evaluation.codegen.operators.FilterOperator;
import evaluation.codegen.operators.JoinOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Class which can translate a Calcite logical plan into a tree of {@link CodeGenOperator}s that
 * can be used to generate query code.
 */
public class QueryTranslator {

    /**
     * Method to translate a logical plan into a tree of {@link CodeGenOperator}s that can be used
     * to generate query code.
     * @param logicalPlanRoot The logical plan to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return The {@link CodeGenOperator} tree that can generate an executable query corresponding to
     * {@code logicalPlanRoot}.
     */
    public CodeGenOperator<?> translate(RelNode logicalPlanRoot, boolean simdEnabled) {
        return convert(logicalPlanRoot, simdEnabled);
    };

    /**
     * Method to translate a given {@link RelNode} of a logical plan into a {@link CodeGenOperator}.
     * @param logicalPlan The logical (sub)-plan to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return A {@link CodeGenOperator} representing the logical plan.
     */
    protected final CodeGenOperator<?> convert(RelNode logicalPlan, boolean simdEnabled) {
        if (logicalPlan instanceof LogicalArrowTableScan logicalScan)
            return convert(logicalScan, simdEnabled);
        else if (logicalPlan instanceof LogicalFilter logicalFilter)
            return convert(logicalFilter, simdEnabled);
        else if (logicalPlan instanceof LogicalAggregate logicalAggregate)
            return convert(logicalAggregate, simdEnabled);
        else if (logicalPlan instanceof LogicalJoin logicalJoin)
            return convert(logicalJoin, simdEnabled);

        throw new IllegalArgumentException(
                "The supplied logical plan cannot be converted due to an unimplemented RelNode type");
    }

    /**
     * Method to translate an {@link LogicalArrowTableScan} into a {@link CodeGenOperator}.
     * @param scan The {@link LogicalArrowTableScan} to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return A {@link CodeGenOperator} corresponding to {@code scan}.
     */
    protected CodeGenOperator<LogicalArrowTableScan> convert(
            LogicalArrowTableScan scan, boolean simdEnabled) {
            return new ArrowTableScanOperator(scan, simdEnabled);
    };

    /**
     * Method to translate an {@link LogicalFilter} into a {@link CodeGenOperator}.
     * @param filter The {@link LogicalFilter} to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return A {@link CodeGenOperator} corresponding to {@code filter}.
     */
    protected CodeGenOperator<LogicalFilter> convert(LogicalFilter filter, boolean simdEnabled) {
        return new FilterOperator(
                filter,
                simdEnabled,
                convert(filter.getInput(), simdEnabled)
        );
    }

    /**
     * Method to translate an {@link LogicalAggregate} into a {@link CodeGenOperator}.
     * @param aggregation The {@link LogicalAggregate} to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return A {@link CodeGenOperator} corresponding to {@code aggregation}.
     */
    protected CodeGenOperator<LogicalAggregate> convert(LogicalAggregate aggregation, boolean simdEnabled) {
        return new AggregationOperator(
                aggregation,
                simdEnabled,
                convert(aggregation.getInput(), simdEnabled)
        );
    }

    /**
     * Method to translate an {@link LogicalJoin} into a {@link CodeGenOperator}.
     * @param join The {@link LogicalJoin} to translate.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @return A {@link CodeGenOperator} corresponding to {@code join}.
     */
    protected  CodeGenOperator<LogicalJoin> convert(LogicalJoin join, boolean simdEnabled) {
        return new JoinOperator(
                join,
                simdEnabled,
                convert(join.getLeft(), simdEnabled),
                convert(join.getRight(), simdEnabled)
        );
    }

}
