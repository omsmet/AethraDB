package evaluation.codegen.translation;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;

/**
 * Interface implemented by classes which can translate a Calcite logical plan into a {@link QueryCodeGenerator}.
 */
public abstract class QueryTranslator {

    /**
     * Method to translate a logical plan into a {@link QueryCodeGenerator}.
     * @param logicalPlanRoot The logical plan to translate.
     * @return A {@link QueryCodeGenerator} that can generate an executable query corresponding to
     * {@code logicalPlanRoot}.
     */
    public abstract QueryCodeGenerator translate(RelNode logicalPlanRoot);

    /**
     * Method to translate a given {@link RelNode} of a logical plan into a {@link CodeGenOperator}.
     * @param logicalPlan The logical (sub)-plan to translate.
     * @return A {@link CodeGenOperator} representing the logical plan.
     */
    protected final CodeGenOperator convert(RelNode logicalPlan) {
        if (logicalPlan instanceof LogicalArrowTableScan)
            return convert((LogicalArrowTableScan) logicalPlan);

        throw new IllegalArgumentException("The supplied logical plan cannot be converted due to an unimplemented RelNode type");
    }

    /**
     * Method to translate an {@link LogicalArrowTableScan} into an {@link CodeGenOperator}.
     * @param scan the {@link LogicalArrowTableScan} to translate.
     * @return a {@link CodeGenOperator} corresponding to {@code scan} in the paradigm implemented by the {@link QueryTranslator} descendant.
     */
    protected abstract CodeGenOperator convert(LogicalArrowTableScan scan);
}
