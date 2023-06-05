package evaluation.codegen.translation;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.operators.ArrowTableScanOperator;
import evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;

/**
 * Query translator which ensures that code is generated for non-vectorised query processing.
 */
public class NonVectorisedQueryTranslator extends QueryTranslator {

    /**
     * Method which creates a {@link QueryCodeGenerator} that generates non-vectorised query processing
     * code for a logical plan.
     * @param logicalPlanRoot The logical plan to translate.
     * @return The {@link QueryCodeGenerator} for producing a data-centric query execution system
     * corresponding to {@code logicalPlanRoot}.
     */
    @Override
    public QueryCodeGenerator translate(RelNode logicalPlanRoot) {
        CodeGenOperator rootOperator = convert(logicalPlanRoot);
        QueryCodeGenerator result = new QueryCodeGenerator(rootOperator);
        return result;
    }

    @Override
    protected CodeGenOperator convert(LogicalArrowTableScan scan) {
        return new ArrowTableScanOperator(scan);
    }
}
