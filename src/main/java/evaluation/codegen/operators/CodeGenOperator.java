package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedMapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.codehaus.janino.Java;

import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * Class that is extended by all code generator operators.
 * Wraps the {@link RelNode} representing the logical sub-plan that is implemented by this node.
 */
public abstract class CodeGenOperator<T extends RelNode> {

    /**
     * The {@link CodeGenOperator} that consumes the result from this operator.
     */
    protected CodeGenOperator<?> parent;

    /**
     * The logical sub-plan that is represented by the query operator tree rooted at this operator.
     */
    private final T logicalSubplan;

    /**
     * Whether a {@link CodeGenOperator} is allowed to use SIMD for processing.
     */
    protected final boolean simdEnabled;

    /**
     * Initialises a new instance of a specific {@link CodeGenOperator}.
     * @param logicalSubplan The logical sub-plan that should be implemented by this operator.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     */
    protected CodeGenOperator(T logicalSubplan, boolean simdEnabled) {
        this.parent = null;
        this.logicalSubplan = logicalSubplan;
        this.simdEnabled = simdEnabled;
    }

    /**
     * Obtain the logical query plan that should be implemented by this operator.
     * @return The logical query plan implemented by the query operator tree rooted at this node.
     */
    public T getLogicalSubplan() {
        return this.logicalSubplan;
    }

    /**
     * Method to set the parent of this {@link CodeGenOperator}.
     * @param parent The parent of this operator.
     */
    public void setParent(CodeGenOperator<?> parent) {
        this.parent = parent;
    }

    /**
     * Method to indicate whether the current operator can produce code for non-vectorised execution.
     */
    public boolean canProduceNonVectorised() {
        return false;
    };

    /**
     * Method to indicate whether the current operator can produce code for vectorised execution.
     */
    public boolean canProduceVectorised() {
        return false;
    };

// TODO: ensure we can really do without these operators
//
//    /**
//     * Method to indicate whether the current operator can consume code for non-vectorised execution.
//     */
//    public abstract boolean canConsumeNonVectorised();
//
//    /**
//     * Method to indicate whether the current operator can consume code for vectorised execution.
//     */
//    public abstract boolean canConsumeVectorised();

    /**
     * Method for generating non-vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    };

    /**
     * Method for generating non-vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

    /**
     * Method to invoke the parent's consumption method in the non-vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated consumption code.
     */
    protected List<Java.Statement> nonVecParentConsume(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Push the ordinal mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the for loop
        List<Java.Statement> parentCode = this.parent.consumeNonVec(cCtx, oCtx);

        // Pop the CodeGenContext and ordinal to access path mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        return parentCode;
    }

    /**
     * Method for generating vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    }

    /**
     * Method for generating vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

    /**
     * Method to invoke the parent's consumption method in the vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated consumption code.
     */
    protected List<Java.Statement> vecParentConsume(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Push the ordinal mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the for loop
        List<Java.Statement> parentCode = this.parent.consumeVec(cCtx, oCtx);

        // Pop the CodeGenContext and ordinal to access path mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        return parentCode;
    }

    /**
     * Method to retrieve a {@link Java.Rvalue} for a non-vector {@link AccessPath}.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param accessPathIndex The index in {@code cCtx.getCurrentOrdinalMapping()} of the
     *                        {@link AccessPath} to generate the {@link Java.Rvalue} for. Must be
     *                        a non-vector type.
     * @param codegenTarget The current list of statements being generated to perform allocations of
     *                      variables if necessary for creating the {@link Java.Rvalue}.
     * @return The {@link Java.Rvalue} corresponding to {@code accessPath}.
     */
    protected Java.Rvalue getRValueFromAccessPathNonVec(
            CodeGenContext cCtx,
            int accessPathIndex,
            List<Java.Statement> codegenTarget
    ) {
        AccessPath accessPath = cCtx.getCurrentOrdinalMapping().get(accessPathIndex);

        // Expose the ordinal position value based on the supported type. When the access path is
        // not a simple scalar variable, transform it to reduce method calls where possible.
        if (accessPath instanceof ScalarVariableAccessPath svap) {
            return svap.read();

        } else if (accessPath instanceof IndexedArrowVectorElementAccessPath
                || accessPath instanceof IndexedMapAccessPath) {
            QueryVariableType ordinalType;
            Java.Rvalue variableValue;
            if (accessPath instanceof IndexedArrowVectorElementAccessPath iaveap) {
                ordinalType = iaveap.getType();
                variableValue = iaveap.read();
            } else { // if (accessPath instanceof IndexedMapAccessPath imap) {
                IndexedMapAccessPath imap = (IndexedMapAccessPath) accessPath;
                ordinalType = imap.getType();
                variableValue = imap.read();
            }

            // Need to allocate a new variable
            // [scalar java type] ordinal_value = $arrowVectorVar$.get($indexVar$)
            String operandVariableName = cCtx.defineVariable("ordinal_value");
            codegenTarget.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), ordinalType),
                            operandVariableName,
                            variableValue
                    )
            );

            // Update the ordinal access path in the mapping to reflect the allocation of the variable
            var newOrdinalAccessPath = new ScalarVariableAccessPath(operandVariableName, ordinalType);
            cCtx.getCurrentOrdinalMapping().set(accessPathIndex, newOrdinalAccessPath);

            // Return the access path code
            return newOrdinalAccessPath.read();

        } else {
            throw new IllegalStateException(
                    "CodeGenOperator.getRValueFromAccessPathNonVec should never receive a vectorised access path");
        }
    }

    /**
     * Method to convert a {@link RexLiteral} to an appropriate {@link Java.Rvalue}.
     * @param literal The {@link RexLiteral} to convert.
     * @return The {@link Java.Rvalue} corresponding to {@code literal}.
     */
    Java.Rvalue rexLiteralToRvalue(RexLiteral literal) {
        // Simply return a literal corresponding to the operand
        BasicSqlType literalType = (BasicSqlType) literal.getType();

        return switch (literalType.getSqlTypeName()) {
            case DOUBLE -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Double.class).toString());
            case FLOAT -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Float.class).toString());
            case INTEGER -> createIntegerLiteral(getLocation(), literal.getValueAs(Integer.class).toString());
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.codeGenOperandNonVec does not support his literal type");
        };
    }

}
