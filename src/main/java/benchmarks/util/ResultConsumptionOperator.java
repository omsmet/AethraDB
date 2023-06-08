package benchmarks.util;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;

/**
 * {@link CodeGenOperator} for transferring the result of a generated query to a JMH benchmark.
 */
public class ResultConsumptionOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the query result that is to be transferred to a benchmark.
     */
    private final CodeGenOperator<?> child;

    /**
     * Create an {@link ResultConsumptionOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public ResultConsumptionOperator(RelNode logicalSubplan, CodeGenOperator<?> child) {
        super(logicalSubplan);
        this.child = child;
        this.child.setParent(this);
    }

    @Override
    public boolean canProduceNonVectorised() {
        return this.child.canProduceNonVectorised();
    }

    @Override
    public boolean canProduceVectorised() {
        return this.child.canProduceVectorised();
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceNonVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Simply obtain the access path from the ordinal mapping and consume the value of each column
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();

        // Generate the required consumption method invocation statement
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            Java.Type ordinalType = sqlTypeToScalarJavaType(
                    (BasicSqlType) this.getLogicalSubplan().getRowType().getFieldList().get(i).getType());
            Java.Rvalue resultValue = getRValueFromAccessPathNonVec(cCtx, oCtx, i, ordinalType, codegenResult);

            codegenResult.add(
                    // cCtx.getResultConsumptionTarget().consumeResultItem(resultValue);
                    createMethodInvocationStm(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getResultConsumptionTarget"
                            ),
                            "consumeResultItem",
                            new Java.Rvalue[] { resultValue }
                    )
            );
        }

        return codegenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Simply obtain the access path from the ordinal mapping and print each column
        // Separate columns by a line return
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();

        // Generate the required print statements
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {

            // Differentiate by type of result
            AccessPath ordinalAccessPath = currentOrdinalMapping.get(i);
            if  (ordinalAccessPath instanceof ScalarVariableAccessPath svap) {
                // This should only occur for aggregation queries in the vectorised paradigm
                codegenResult.add(
                        // cCtx.getResultConsumptionTarget().consumeResultItem(svap.read());
                        createMethodInvocationStm(
                                getLocation(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "cCtx"),
                                        "getResultConsumptionTarget"
                                ),
                                "consumeResultItem",
                                new Java.Rvalue[] { svap.read() }
                        )
                );

            } else {
                throw new UnsupportedOperationException("ResultConsumptionOperator.consumeVec does not support this ordinal type");
            }

        }

        return codegenResult;
    }
}
