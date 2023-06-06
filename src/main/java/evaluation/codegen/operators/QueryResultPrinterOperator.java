package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import org.apache.calcite.rel.RelNode;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createStringLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * An {@link CodeGenOperator} which can be the top-level operator of a {@link CodeGenOperator} tree
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
            String methodName = (i != currentOrdinalMapping.size() - 1) ? "print" : "println";
            try {
                codegenResult.add(
                        JaninoMethodGen.createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "System.out"),
                                methodName,
                                new Java.Rvalue[] {
                                        JaninoOperatorGen.plus(
                                                getLocation(),
                                                currentOrdinalMapping.get(i).read(),
                                                createStringLiteral(getLocation(), "\", \"")
                                        )
                                }
                        )
                );
            } catch (CompileException e) {
                throw new RuntimeException("Could not generate method invocation statement during code generation phase", e);
            }
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
            String methodName = (i != currentOrdinalMapping.size() - 1) ? "print" : "println";
            try {
                codegenResult.add(
                        JaninoMethodGen.createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "System.out"),
                                methodName,
                                new Java.Rvalue[] {
                                        currentOrdinalMapping.get(i).read(),
                                }
                        )
                );
                codegenResult.add(
                        JaninoMethodGen.createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "System.out"),
                                "println"
                        )
                );
            } catch (CompileException e) {
                throw new RuntimeException("Could not generate method invocation statement during code generation phase", e);
            }
        }

        return codegenResult;
    }
}
