package evaluation.codegen.translation;

import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.operators.CodeGenOperator;
import evaluation.codegen.operators.QueryResultPrinterOperator;
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
        CodeGenOperator<?> rootOperator = convert(logicalPlanRoot);
        CodeGenOperator<?> printOperator = new QueryResultPrinterOperator(rootOperator.getLogicalSubplan(), rootOperator);
        return new QueryCodeGenerator(printOperator, false);
    }

}
