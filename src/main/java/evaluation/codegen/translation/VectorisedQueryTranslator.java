package evaluation.codegen.translation;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;

/**
 * Query translator which ensures that code is generated for vectorised query processing.
 */
public class VectorisedQueryTranslator extends QueryTranslator {

    /**
     * Method which creates a {@link QueryCodeGenerator} that generates vectorised query processing
     * code for a logical plan.
     * @param logicalPlanRoot The logical plan to translate.
     * @return The {@link QueryCodeGenerator} for producing a vectorised query execution system
     * corresponding to {@code logicalPlanRoot}.
     */
    @Override
    public QueryCodeGenerator translate(RelNode logicalPlanRoot) {
        // TODO: implement
        throw new UnsupportedOperationException("Not Implemented Yet");
    }

    @Override
    protected CodeGenOperator convert(LogicalArrowTableScan scan) {
        // TODO: implement
        throw new UnsupportedOperationException("Not Implemented Yet");
    }
}
