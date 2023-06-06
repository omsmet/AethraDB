package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.SimpleAccessPath;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * A {@link CodeGenOperator} which filters out records according to a given condition.
 */
public class FilterOperator extends CodeGenOperator<LogicalFilter> {

    /**
     * The {@link CodeGenOperator} producing the records to be filtered by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Create a {@link FilterOperator} instance for a specific sub-query.
     * @param filter The logical filter (and sub-query) for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the records to be filtered.
     */
    public FilterOperator(LogicalFilter filter, CodeGenOperator<?> child) {
        super(filter);
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
        // Obtain the current ordinal mapping to access the exposed columns
        List<AccessPath> ordinalMapping = cCtx.getCurrentOrdinalMapping();

        // Obtain and process the filter condition
        RexNode filterConditionRaw = this.getLogicalSubplan().getCondition();
        return consumeNonVecOperator(cCtx, oCtx, filterConditionRaw, true);
    }

    /**
     * Method to generate the required code on the backward code generation pass based on the
     * specific filter operator used by the {@link LogicalFilter}.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 this method to generate the required code in a recursive fashion.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexNode filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        if (!(filterOperator instanceof RexCall castFilterOperator))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVec only supports RexCall conditions");

        // Important for all operators: since we are in the non-vectorised style, we must be processing
        // within a for-loop. Any record not matching the condition must therefore result in the
        // invocation of a continue statement to prevent the record from being processed.

        // Forward the generation obligation to the correct method based on the operator type.
        return switch (castFilterOperator.getKind()) {
            case AND -> consumeNonVecAndOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            case LESS_THAN -> consumeNonVecLtOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVecOperator does not support this operator type");
        };
    }

    /**
     * Method to invoke the parent's consumption method in the non-vectorised code generation process.
     * TODO: candidate for general {@link CodeGenOperator} method.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated consumption code.
     */
    private List<Java.Statement> nonVecParentConsume(CodeGenContext cCtx, OptimisationContext oCtx) {
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
     * Method to generate the required code on the backward code generation pass for an AND operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The AND operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecAndOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the AND operands and generate code for each of them
        // If a record does not match the operand, it will invoke a continue statement
        for (RexNode operand : filterOperator.getOperands())
            codegenResult.addAll(consumeNonVecOperator(cCtx, oCtx, operand, false));

        // Any record which processes beyond the above operands in the code-gen flow, matches the
        // conjunctive condition. Invoke the parent if required to consume the result.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codegenResult;
    }

    /**
     * Method to generate the required code on the backward code generation pass for a < operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The < operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecLtOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the operands
        RexNode lhs = filterOperator.getOperands().get(0);
        RexNode rhs = filterOperator.getOperands().get(1);

        // Convert the operands
        Java.Rvalue lhsRvalue = codeGenOperandNonVec(cCtx, oCtx, lhs, codegenResult);
        Java.Rvalue rhsRvalue = codeGenOperandNonVec(cCtx, oCtx, rhs, codegenResult);

        // Generate the required control flow
        // if (!(lhsRvalue < rhsRvalue))
        //     continue;
        codegenResult.add(
                createIfNotContinue(
                        getLocation(),
                        lt(getLocation(), lhsRvalue, rhsRvalue)
                )
        );

        // The condition matches. Invoke the parent consumption method if required.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the result
        return codegenResult;
    }

    /**
     * Generate code for a scalar operand in the non-vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param operand The operand to generate code for.
     * @param target The code generation result to add code to if required for accessing the operand.
     * @return The {@link Java.Rvalue} corresponding to the operand.
     */
    private Java.Rvalue codeGenOperandNonVec(CodeGenContext cCtx, OptimisationContext oCtx, RexNode operand, List<Java.Statement> target) {
        // Generate the required code based on the operand type
        if (operand instanceof RexInputRef inputRef) {
            // RexInputRefs refer to a specific ordinal position in the result of the previous operator
            int ordinalIndex = inputRef.getIndex();
            List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
            AccessPath ordinalAccessPath = currentOrdinalMapping.get(ordinalIndex);

            // If the access path is not a simple access path, allocate a variable to make it simple
            // This is done to reduce method calls where possible
            if (!(ordinalAccessPath instanceof SimpleAccessPath)) {
                String operandVariableName = cCtx.defineVariable("ordinal_" + ordinalIndex);
                target.add(
                    createLocalVariable(
                            getLocation(),
                            sqlTypeToScalarJavaType((BasicSqlType) operand.getType()),
                            operandVariableName,
                            ordinalAccessPath.read()
                    )
                );

                // Update the ordinal access path in the mapping to reflect the allocation of the variable
                ordinalAccessPath = new SimpleAccessPath(operandVariableName);
                cCtx.getCurrentOrdinalMapping().set(ordinalIndex, ordinalAccessPath);
            }

            // Return the (updated) access path
            return ordinalAccessPath.read();

        } else if (operand instanceof RexLiteral literal) {
            // Simply return a literal corresponding to the operand
            BasicSqlType literalType = (BasicSqlType) literal.getType();

            return switch (literalType.getSqlTypeName()) {
                case DOUBLE -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Double.class).toString());
                case FLOAT -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Float.class).toString());
                case INTEGER -> createIntegerLiteral(getLocation(), literal.getValueAs(Integer.class).toString());
                default -> throw new UnsupportedOperationException("FilterOperator.codeGenOperandNonVec does not support his literal type");
            };
        } else {
            throw new UnsupportedOperationException("FilterOperator.codeGenOperandNonVec does not support the provide operand");
        }
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceVec(cCtx, oCtx);
    }

//    @Override
//    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
//        // TODO: Implement
//    }

}
