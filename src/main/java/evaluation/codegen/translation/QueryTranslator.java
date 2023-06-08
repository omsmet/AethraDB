package evaluation.codegen.translation;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.operators.AggregationOperator;
import evaluation.codegen.operators.ArrowTableScanOperator;
import evaluation.codegen.operators.CodeGenOperator;
import evaluation.codegen.operators.FilterOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * Class which can translate a Calcite logical plan into a tree of {@link CodeGenOperator}s that
 * can be used to generate query code.
 */
public class QueryTranslator {

    /**
     * Method to translate a logical plan into a tree of {@link CodeGenOperator}s that can be used
     * to generate query code.
     * @param logicalPlanRoot The logical plan to translate.
     * @return The {@link CodeGenOperator} tree that can generate an executable query corresponding to
     * {@code logicalPlanRoot}.
     */
    public CodeGenOperator<?> translate(RelNode logicalPlanRoot) {
        return convert(logicalPlanRoot);
    };

    /**
     * Method to translate a given {@link RelNode} of a logical plan into a {@link CodeGenOperator}.
     * @param logicalPlan The logical (sub)-plan to translate.
     * @return A {@link CodeGenOperator} representing the logical plan.
     */
    protected final CodeGenOperator<?> convert(RelNode logicalPlan) {
        if (logicalPlan instanceof LogicalArrowTableScan logicalScan)
            return convert(logicalScan);
        else if (logicalPlan instanceof LogicalFilter logicalFilter)
            return convert(logicalFilter);
        else if (logicalPlan instanceof LogicalAggregate logicalAggregate)
            return convert(logicalAggregate);

        throw new IllegalArgumentException(
                "The supplied logical plan cannot be converted due to an unimplemented RelNode type");
    }

    /**
     * Method to translate an {@link LogicalArrowTableScan} into a {@link CodeGenOperator}.
     * @param scan The {@link LogicalArrowTableScan} to translate.
     * @return A {@link CodeGenOperator} corresponding to {@code scan} in the paradigm implemented
     * by the {@link QueryTranslator} descendant.
     */
    protected CodeGenOperator<LogicalArrowTableScan> convert(LogicalArrowTableScan scan) {
            return new ArrowTableScanOperator(scan);
    };

    /**
     * Method to translate an {@link LogicalFilter} into a {@link CodeGenOperator}.
     * @param filter The {@link LogicalFilter} to translate.
     * @return A {@link CodeGenOperator} corresponding to {@code filter} in the paradigm implemented
     * by the {@link QueryTranslator} descendant.
     */
    protected CodeGenOperator<LogicalFilter> convert(LogicalFilter filter) {
        return new FilterOperator(
                filter,
                convert(
                        filter.getInput()
                )
        );
    }

    /**
     * Method to translate an {@link LogicalAggregate} into a {@link CodeGenOperator}.
     * @param aggregation The {@link LogicalAggregate} to translate.
     * @return A {@link CodeGenOperator} corresponding to {@code filter} in the paradigm implemented
     * by the {@link QueryTranslator} descendant.
     */
    protected CodeGenOperator<LogicalAggregate> convert(LogicalAggregate aggregation) {
        return new AggregationOperator(
                aggregation,
                convert(
                        aggregation.getInput()
                )
        );
    }

}
