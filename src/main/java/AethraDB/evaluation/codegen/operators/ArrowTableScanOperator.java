package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import AethraDB.util.arrow.ArrowFileSchemaExtractor;
import org.apache.arrow.vector.types.pojo.Field;
import org.codehaus.janino.Java;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrowTypeToArrowVectorType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.memberTypeForArrowVector;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;

/**
 * {@link CodeGenOperator} which generates code for reading data from an Arrow file.
 */
public class ArrowTableScanOperator extends CodeGenOperator {

    /**
     * Boolean keeping track of whether SIMD production is allowed in this operator.
     */
     private final boolean SIMDProductionAllowed;

    /**
     * The path of the directory containing the database.
     */
    private final String databasePath;

    /**
     * The name of the table to scan over.
     */
    private final String tableName;

    /**
     * Whether the scan should be performed using a projecting table scan implementation.
     */
    private final boolean isProjecting;

    /**
     * The indices of the columns to project (only considered when {@code isProject == true}).
     */
    private final int[] projectedColumns;

    /**
     * Creates an {@link ArrowTableScanOperator} for a specific table.
     * @param databasePath The path of the directory containing the database.
     * @param tableName The name of the table to scan over.
     * @param isProjecting Whether the scan returns all columns, or projects out only a few.
     * @param projectedColumns The indices of the columns that are accessible via this scan.
     */
    public ArrowTableScanOperator(String databasePath, String tableName, boolean isProjecting, int[] projectedColumns) {
        this.SIMDProductionAllowed = false;
        this.databasePath = databasePath;
        this.tableName = tableName;
        this.isProjecting = isProjecting;
        this.projectedColumns = projectedColumns;
    }

    @Override
    public boolean canProduceNonVectorised() {
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        return true;
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Generate the basic code for
        // - Reading the arrow file
        // - Iterating over ad projecting out the relevant column vectors
        // Post-condition:
        // - cCtx's ordinal mapping contains the access paths to the projected column vectors
        // - whileLoopBody represents the partially generated while loop that iterates over them
        Java.Block whileLoopBody = createBlock(getLocation());
        List<Java.Statement> codegenResult = this.genericProduce(cCtx, oCtx, whileLoopBody);

        // Task to be performed: introduce a for-loop within the whileLoopBody to iterate over the
        // rows in the projected columns and update the ordinal mapping to the access path for the
        // row-values. Finally, make sure the parent operator fills the resulting for-loop body.

        Java.Block forLoopBody = createBlock(getLocation());
        if (this.SIMDProductionAllowed)
            throw new UnsupportedOperationException("ArrowTableScanOperator.produceNonVec no longer supports SIMD");

        // Allocate the value count
        // int recordCount = firstColumnVector.getValueCount();
        ScalarVariableAccessPath recordCountAP = new ScalarVariableAccessPath(
                cCtx.defineVariable("recordCount"),
                P_INT
        );
        whileLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        createPrimitiveType(getLocation(), Java.Primitive.INT),
                        recordCountAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                // Valid to cast to ArrowVectorAccessPath as this is delivered by genericProduce(..)
                                ((ArrowVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(0)).read(),
                                "getValueCount"
                        )
                )
        );

        // for (int aviv = 0; aviv < [recordCount]; aviv++) { [forLoopBody] }
        String avivName = cCtx.defineVariable("aviv");
        ScalarVariableAccessPath avivAccessPath = new ScalarVariableAccessPath(avivName, P_INT);
        whileLoopBody.addStatement(
                createForLoop(
                        getLocation(),
                        createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, avivName, "0"),
                        lt(
                                getLocation(),
                                avivAccessPath.read(),
                                recordCountAP.read()
                        ),
                        JaninoOperatorGen.postIncrement(getLocation(), createAmbiguousNameRef(getLocation(), avivName)),
                        forLoopBody
                )
        );

        // Update the ordinal to access path mapping by building a new mapping that replaces each
        // arrow vector variable in the current mapping with one that performs an indexed read using
        // the aviv index variable
        List<AccessPath> updatedOrdinalMapping =
                cCtx.getCurrentOrdinalMapping().stream().map(entry -> {
                    // Valid to cast to entry ArrowVectorAccessPath as this is delivered by genericProduce(..)
                    ArrowVectorAccessPath avap = (ArrowVectorAccessPath) entry;
                    return (AccessPath) new IndexedArrowVectorElementAccessPath(
                            avap,
                            avivAccessPath,
                            memberTypeForArrowVector(avap.getType())
                    );
                }).toList();
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Have the parent operator consume the result within the for loop
        forLoopBody.addStatements(nonVecParentConsume(cCtx, oCtx));

        // Return the generated code after wrapping it in the scan surrounding variables
        return codegenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("An ArrowTableScanOperator cannot consume data");
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Generate the basic code for
        // - Reading the arrow file
        // - Iterating over ad projecting out the relevant column vectors
        // Post-condition:
        // - cCtx's ordinal mapping contains the access paths to the projected column vectors
        // - whileLoopBody represents the partially generated while loop that iterates over them
        Java.Block whileLoopBody = createBlock(getLocation());
        List<Java.Statement> codegenResult = this.genericProduce(cCtx, oCtx, whileLoopBody);

        // Have the parent operator consume the result within the while loop
        whileLoopBody.addStatements(vecParentConsume(cCtx, oCtx));

        // Return the generated code after wrapping it in the scan surrounding variables
        return codegenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("An ArrowTableScanOperator cannot consume data");
    }

    /**
     * Method for generating generic code during a "forward" pass on a table scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param whileLoopBody The {@link Java.Block} representing the body of the generated while-loop
     *                      which iterates over the vectors of the Arrow table.
     * @return The generated generic query code for this operator.
     */
    private List<Java.Statement> genericProduce(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            Java.Block whileLoopBody
    ) {
        // Create an arrow reader instance
        File tableFile = new File(this.databasePath + "/" + this.tableName + ".arrow");
        ArrowTableReader arrowReader;
        try {
            arrowReader = new ABQArrowTableReader(
                    tableFile,
                    cCtx.getArrowRootAllocator(),
                    this.isProjecting,
                    this.projectedColumns
            );
        } catch (Exception e) {
            throw new RuntimeException("Could not create ArrowTableReader in query compilation stage.", e);
        }

        // Store the arrow reader in the CodeGenContext
        int arrowReaderIndex = cCtx.addArrowReader(arrowReader);

        // Perform the actual code generation
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Initialise a variable for accessing the arrow reader
        // ArrowTableReader $tablename$ = cCtx.getArrowReader(arrowReaderIndex);
        String arrowReaderVariableName = cCtx.defineVariable(this.tableName);

        codegenResult.add(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(
                                getLocation(),
                                "ArrowTableReader"
                        ),
                        arrowReaderVariableName,
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "cCtx"),
                                "getArrowReader",
                                new Java.Rvalue[] {
                                        createIntegerLiteral(getLocation(), arrowReaderIndex)
                                }
                        )
                )
        );

        // Loop over the vectors in the arrow file
        // while ([arrowReaderVariableName].loadNextBatch()) { [whileLoopBody] }
        codegenResult.add(
                createWhileLoop(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), arrowReaderVariableName),
                                "loadNextBatch"
                        ),
                        whileLoopBody
                )
        );

        // Project by creating an arrow vector variable per projected column in the [whileLoopBody]
        // [vectorType] [arrowReaderVariableName]_vc_[outputColumnIndex] =
        //     ([vectorType]) [arrowReaderVariableName].get([originalColumnIndex])

        List<Field> schemaFields;
        try {
            schemaFields = ArrowFileSchemaExtractor.getFieldDescriptionFromTableFile(tableFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int numberOutputColumns = this.projectedColumns.length;
        List<AccessPath> projectedColumnAccessPaths = new ArrayList<>(numberOutputColumns);

        for (int outputColumnIndex = 0; outputColumnIndex < numberOutputColumns; outputColumnIndex++) {
            // Obtain data about the column to project
            int originalColumnIndex = this.projectedColumns[outputColumnIndex];
            QueryVariableType vectorType = arrowTypeToArrowVectorType(schemaFields.get(originalColumnIndex).getType());

            // Define and expose an access path to a variable representing vectors of the projected column
            String preferredOutputColumnVariableName = arrowReaderVariableName + "_vc_" + outputColumnIndex;
            String outputColumnVariableName = cCtx.defineVariable(preferredOutputColumnVariableName);
            projectedColumnAccessPaths.add(
                    outputColumnIndex,
                    new ArrowVectorAccessPath(outputColumnVariableName, vectorType)
            );

            // Do the projection by generating the variable for this column's vector
            Java.Type projectedColumnType = toJavaType(getLocation(), vectorType);
            whileLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            projectedColumnType,
                            outputColumnVariableName,
                            createCast(
                                    getLocation(),
                                    projectedColumnType,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), arrowReaderVariableName),
                                            "getVector",
                                            new Java.Rvalue[] {
                                                    createIntegerLiteral(getLocation(), originalColumnIndex)
                                            }
                                    )
                            )
                    )
            );
        }

        // Store the ordinal to vector variable mapping
        cCtx.setCurrentOrdinalMapping(projectedColumnAccessPaths);

        return codegenResult;
    }
}
