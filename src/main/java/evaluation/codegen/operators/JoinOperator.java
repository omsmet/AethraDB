package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclarationStm;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createPublicFinalFieldDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * {@link CodeGenOperator} which performs a join over two tables for a given join condition. All
 * joins implemented by this operator are currently hash-joins over a single equality predicate.
 */
public class JoinOperator extends CodeGenOperator<LogicalJoin> {

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "left" side
     * of the join.
     */
    private final CodeGenOperator<?> leftChild;

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "right" side
     * of the join.
     */
    private final CodeGenOperator<?> rightChild;

    /**
     * The variable holding the generated class type that will be used to store the records of the
     * left child in the hash-table during the build stage.
     */
    private Java.LocalClassDeclarationStatement leftChildRecordType;

    /**
     * Boolean indicating if the consume method should perform a hash-table build or a hash-table probe.
     * Will be initialised as false to indicate a hash-table build first.
     */
    private boolean consumeInProbePhase;

    /**
     * Creates a new {@link JoinOperator} instance for a specific sub-query.
     * @param join The logical join (and sub-query) for which the operator is created.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @param leftChild The {@link CodeGenOperator} producing the left input side of the join.
     * @param rightChild The {@link CodeGenOperator} producing the right input side of the join.
     */
    public JoinOperator(
            LogicalJoin join,
            boolean simdEnabled,
            CodeGenOperator<?> leftChild,
            CodeGenOperator<?> rightChild
    ) {
        super(join, simdEnabled);
        this.leftChild = leftChild;
        this.leftChild.setParent(this);
        this.rightChild = rightChild;
        this.rightChild.setParent(this);
        this.consumeInProbePhase = false;

        // Check pre-conditions
        if (join.getJoinType() != JoinRelType.INNER)
            throw new UnsupportedOperationException("JoinOperator currently only supports inner joins");

        RexNode joinCondition = join.getCondition();
        if (!(joinCondition instanceof RexCall joinConditionCall))
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions wrapped in a RexCall");

        if (joinConditionCall.getOperator().getKind() != SqlKind.EQUALS)
            throw new UnsupportedOperationException("JoinOperator currently only supports equality join conditions");

        if (joinConditionCall.getOperands().size() != 2)
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions over two operands");

        List<RexNode> joinConditionOperands = joinConditionCall.getOperands();
        for (RexNode joinConditionOperand : joinConditionOperands) {
            if (!(joinConditionOperand instanceof RexInputRef))
                throw new UnsupportedOperationException("JoinOperator currently only supports join condition operands that refer to an input column");
        }

        if (joinConditionOperands.get(0).getType() != joinConditionOperands.get(1).getType())
            throw new UnsupportedOperationException("JoinOperator expects the join condition operands to be of the same type");
    }

    @Override
    public boolean canProduceNonVectorised() {
        // Since this is a blocking operator, we can always expose the result in the non-vectorised paradigm.
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        // Since this is a blocking operator, we can always expose the result in the vectorised paradigm.
        return true;
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // First build the hash-table by calling the produce method on the left child operator,
        // which will eventually invoke the consumeNonVec method on @this which should perform the
        // hash-table build.
        // Additionally, the consumeNonVec method will initialise the record type for the hash table
        // which will have to be added to the codeGenResult first.
        cCtx.pushCodeGenContext();
        List<Java.Statement> leftChildProduceResult = this.leftChild.produceNonVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        codeGenResult.add(this.leftChildRecordType);
        codeGenResult.addAll(leftChildProduceResult);

        // Next, call the produce method on the right child operator, which will eventually invoke
        // the consumeNonvec method on @this, which should perform the hash-table probe and call
        // the consumeNonVec method on the parent.
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.rightChild.produceNonVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        if (!this.consumeInProbePhase) {
            // Mark that on the next call, we are in the probe phase
            this.consumeInProbePhase = true;

            // Then introduce the class to store records of the left-child in the hash-table
            this.leftChildRecordType = generateRecordTypeForRelation(cCtx.getCurrentOrdinalMapping());

            // And build the hash table
            return this.consumeNonVecBuild(cCtx, oCtx);

        } else {
            // Perform the probe (which also has the parent operator consume the result)
            return this.consumeNonVecProbe(cCtx, oCtx);

        }
    }

    /**
     * Method for generating the code that performs the hash-table build in the non-vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecBuild(CodeGenContext cCtx, OptimisationContext oCtx) {

        return new ArrayList<>();
    }

    /**
     * Method for generating the code that performs the hash-table probe in the non-vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecProbe(CodeGenContext cCtx, OptimisationContext oCtx) {

        // Have the parent consume the result
        return this.nonVecParentConsume(cCtx, oCtx); // TODO: update once ready
    }

    /**
     * Method to generate a "record-like" type for storing the elements of an input relation in a hash-table.
     * @param relationType The input relation for which the record type should be generated.
     * @return Code representing a record class for the provided {@code relationType}.
     */
    private Java.LocalClassDeclarationStatement generateRecordTypeForRelation(List<AccessPath> relationType) {
        // Create a class for the record type
        Java.LocalClassDeclaration recordTypeClassDeclaration = createLocalClassDeclaration(
                getLocation(),
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PRIVATE.toString(), getLocation()),
                        new Java.AccessModifier("final", getLocation())
                },
                "JRT_" + Math.abs(relationType.hashCode())
        );

        // Add a field to the class for each column in the relation type
        // Additionally, prepare the formal parameters and assignment statements for the constructor
        Java.FunctionDeclarator.FormalParameter[] constructorParameters = new Java.FunctionDeclarator.FormalParameter[relationType.size()];
        List<Java.Statement> constructorAssignmentStatements = new ArrayList<>(relationType.size());

        for (int i = 0; i < relationType.size(); i++) {
            AccessPath columnAP = relationType.get(i);

            QueryVariableType fieldPrimitiveType = primitiveType(columnAP.getType());
            String fieldName = "ord_" + i;

            recordTypeClassDeclaration.addFieldDeclaration(
                createPublicFinalFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), fieldPrimitiveType),
                        createSimpleVariableDeclaration(getLocation(), fieldName)
                )
            );

            constructorParameters[i] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), fieldPrimitiveType),
                    fieldName);

            constructorAssignmentStatements.add(
                    i,
                    createVariableAssignmentStm(
                            getLocation(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    new Java.ThisReference(getLocation()),
                                    fieldName
                            ),
                            createAmbiguousNameRef(getLocation(), fieldName)
                    )
            );

        }

        // Next, create the constructor to initialise each field
        JaninoMethodGen.createConstructor(
                getLocation(),
                recordTypeClassDeclaration,
                Access.PUBLIC,
                createFormalParameters(
                        getLocation(),
                        constructorParameters
                ),
                null,
                constructorAssignmentStatements
        );

        // Return the created class declaration statement
        return createLocalClassDeclarationStm(recordTypeClassDeclaration);
    }

}
