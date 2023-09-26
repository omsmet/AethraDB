package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createStringLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;

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
     * Boolean keeping track of whether standard date definitions have already been added to the code.
     */
    private boolean dateDefinitionsPresent;

    /**
     * The name of the day_zero variable if {@code this.dateDefinitionsPresent == true}.
     */
    private String dayZeroName;

    /**
     * Create an {@link QueryResultPrinterOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public QueryResultPrinterOperator(RelNode logicalSubplan, CodeGenOperator<?> child) {
        super(logicalSubplan, false);
        this.child = child;
        this.child.setParent(this);
        this.dateDefinitionsPresent = false;
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
            boolean lastElement = (i == currentOrdinalMapping.size() - 1);
            String methodName = lastElement ? "println" : "print";
            Java.Rvalue printValue = getRValueFromOrdinalAccessPathNonVec(cCtx, i, codegenResult);

            // Convert fixed length strings
            QueryVariableType ordinalType = cCtx.getCurrentOrdinalMapping().get(i).getType();
            if (ordinalType == QueryVariableType.S_FL_BIN || ordinalType == QueryVariableType.S_VARCHAR) {
                printValue = createClassInstance(
                        getLocation(),
                        createReferenceType(getLocation(), "java.lang.String"),
                        new Java.Rvalue[] { printValue });

            } else if (ordinalType == QueryVariableType.P_INT_DATE) {
                addDateDefinitions(cCtx);
                printValue = createMethodInvocation(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), this.dayZeroName),
                        "plusDays",
                        new Java.Rvalue[] { printValue }
                );

            } else if (ordinalType == QueryVariableType.P_DOUBLE) {
                printValue = createMethodInvocation(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "String"),
                        "format",
                        new Java.Rvalue[] {
                                createStringLiteral(getLocation(), "\"%.2f\""),
                                printValue
                        }
                );

            }

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
            QueryVariableType ordinalType = ordinalAccessPath.getType();
            boolean isDate =
                       ordinalType == QueryVariableType.ARROW_DATE_VECTOR
                    || ordinalType == QueryVariableType.ARRAY_INT_DATE_VECTOR
                    || ordinalType == QueryVariableType.ARROW_DATE_VECTOR_W_SELECTION_VECTOR
                    || ordinalType == QueryVariableType.ARROW_DATE_VECTOR_W_VALIDITY_MASK;
            String vectorisedPrintOperatorsMethodName = isDate ? "printDate" : "print";

            if (ordinalAccessPath instanceof ArrowVectorAccessPath avap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedPrintOperators"),
                                vectorisedPrintOperatorsMethodName,
                                new Java.Rvalue[] {
                                        avap.read()
                                }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedPrintOperators"),
                                vectorisedPrintOperatorsMethodName,
                                new Java.Rvalue[] {
                                        avwsvap.readArrowVector(),
                                        avwsvap.readSelectionVector(),
                                        avwsvap.readSelectionVectorLength()
                                }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedPrintOperators"),
                                vectorisedPrintOperatorsMethodName,
                                new Java.Rvalue[] {
                                        avwvmap.readArrowVector(),
                                        avwvmap.readValidityMask(),
                                        avwvmap.readValidityMaskLength()
                                }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrayVectorAccessPath avap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedPrintOperators"),
                                vectorisedPrintOperatorsMethodName,
                                new Java.Rvalue[]{
                                        avap.getVectorVariable().read(),
                                        avap.getVectorLengthVariable().read()
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

    private void addDateDefinitions(CodeGenContext cCtx) {
        if (dateDefinitionsPresent)
            return;

        String dtfName = cCtx.defineQueryGlobalVariable(
                "dateTimeFormatter",
                createReferenceType(getLocation(), "java.time.format.DateTimeFormatter"),
                createMethodInvocation(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "java.time.format.DateTimeFormatter"),
                        "ofPattern",
                        new Java.Rvalue[] {
                                createStringLiteral(getLocation(), "\"yyyy-MM-dd\"")
                        }
                ),
                false
        );

        this.dayZeroName = cCtx.defineQueryGlobalVariable(
                "day_zero",
                createReferenceType(getLocation(), "java.time.LocalDate"),
                createMethodInvocation(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "java.time.LocalDate"),
                        "parse",
                        new Java.Rvalue[] {
                                createStringLiteral(getLocation(), "\"1970-01-01\""),
                                createAmbiguousNameRef(getLocation(), dtfName)
                        }
                ),
                false
        );

    }
}
