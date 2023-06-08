package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createStringLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;

/**
 * A {@link CodeGenOperator} which can be the top-level operator of a {@link CodeGenOperator} tree
 * and simply prints all results to the standard output.
 */
public class QueryResultPrinterOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the query result that is printed by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Create an {@link QueryResultPrinterOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public QueryResultPrinterOperator(RelNode logicalSubplan, CodeGenOperator<?> child) {
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
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceNonVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Simply obtain the access path from the ordinal mapping and print the value of each column
        // for this row separated by comma's
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();

        // Generate the required print statements
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            Java.Type ordinalType = sqlTypeToScalarJavaType(
                    (BasicSqlType) this.getLogicalSubplan().getRowType().getFieldList().get(i).getType());

            boolean lastElement = (i == currentOrdinalMapping.size() - 1);
            String methodName = lastElement ? "println" : "print";
            Java.Rvalue printValue = getRValueFromAccessPathNonVec(cCtx, oCtx, i, ordinalType, codegenResult);
            if (!lastElement)
                printValue = plus(
                        getLocation(),
                        printValue,
                        createStringLiteral(getLocation(), "\", \"")
                );

            codegenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System.out"),
                            methodName,
                            new Java.Rvalue[] { printValue }
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
            if (ordinalAccessPath instanceof ArrowVectorAccessPath avap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(
                                        getLocation(),
                                        "evaluation.vector_support.VectorisedPrintOperators"
                                ),
                                "print",
                                new Java.Rvalue[] {
                                        avap.read()
                                }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(
                                        getLocation(),
                                        "evaluation.vector_support.VectorisedPrintOperators"
                                ),
                                "print",
                                new Java.Rvalue[] {
                                        avwsvap.readArrowVector(),
                                        avwsvap.readSelectionVector(),
                                        avwsvap.readSelectionVectorLength()
                                }
                        )
                );

            } else if (ordinalAccessPath instanceof ScalarVariableAccessPath svap) {
                // This should only occur for aggregation queries in the vectorised paradigm
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "System.out"),
                                "println",
                                new Java.Rvalue[] { svap.read() }
                        )
                );

            } else {
                throw new UnsupportedOperationException("QueryResultPrinterOperator.consumeVec does not support this ordinal type");
            }

            if (i == currentOrdinalMapping.size() - 1) {
                codegenResult.add(
                        JaninoMethodGen.createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "System.out"),
                                "println"
                        )
                );
            }
        }

        return codegenResult;
    }
}
