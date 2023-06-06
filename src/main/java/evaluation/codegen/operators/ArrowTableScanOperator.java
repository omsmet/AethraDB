package evaluation.codegen.operators;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SimpleAccessPath;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.DirectArrowTableReader;
import evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.janino.Java;
import util.arrow.ArrowTable;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;

/**
 * {@link CodeGenOperator} which generates code for reading data from an Arrow file.
 */
public class ArrowTableScanOperator extends CodeGenOperator<LogicalArrowTableScan> {

    /**
     * Creates an {@link ArrowTableScanOperator} for a specific {@link LogicalArrowTableScan}.
     * @param logicalSubplan The {@link LogicalArrowTableScan} to create a scan operator for.
     */
    public ArrowTableScanOperator(LogicalArrowTableScan logicalSubplan) {
        super(logicalSubplan);
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

        // for (int aviv = 0; aviv < firstColumnVector.getValueCount; aviv++) { [forLoopBody] }
        String avivName = cCtx.defineVariable("aviv");
        SimpleAccessPath avivAccessPath = new SimpleAccessPath(avivName);
        Java.Block forLoopBody = createBlock(getLocation());
        whileLoopBody.addStatement(
                createForLoop(
                        getLocation(),
                        createPrimitiveLocalVar(
                                getLocation(),
                                Java.Primitive.INT,
                                avivName,
                                "0"
                        ),
                        JaninoOperatorGen.lt(
                                getLocation(),
                                avivAccessPath.read(),
                                createMethodInvocation(
                                        getLocation(),
                                        cCtx.getCurrentOrdinalMapping().get(0).read(),
                                        "getValueCount"
                                )
                        ),
                        JaninoOperatorGen.postIncrement(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), avivName)
                        ),
                        forLoopBody
                )
        );

        // Update the ordinal to access path mapping by building a new mapping that replaces each
        // arrow vector variable in the current mapping with one that performs an indexed read using
        // the aviv index variable
        List<AccessPath> updatedOrdinalMapping =
                cCtx.getCurrentOrdinalMapping().stream().map(entry ->
                        (AccessPath) new IndexedArrowVectorElementAccessPath((SimpleAccessPath) entry, avivAccessPath)
                ).toList();
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Push the ordinal to access path mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the for loop
        forLoopBody.addStatements(this.parent.consumeNonVec(cCtx, oCtx));

        // Pop the CodeGenContext and ordinal to access path mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        // Return the generated code
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

        // Push the ordinal to access path mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the while loop
        whileLoopBody.addStatements(this.parent.consumeVec(cCtx, oCtx));

        // And pop the CodeGenContext and ordinal to variable name mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        // Return the generated code
        return codegenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("An ArrowTableScanOperator cannot consume data");
    }

    /**
     * Method for converting the column type in the logical plan to a specific Arrow vector type name.
     * @param logicalType The list of logical types per column of a record in this scan operator.
     * @param columnIndex The index for which the Arrow vector type is requested.
     * @return The type identifier of the Arrow vector type for the requested column index.
     */
    private String getArrowVectorTypeForColumn(List<RelDataTypeField> logicalType, int columnIndex) {
        BasicSqlType colType = (BasicSqlType) logicalType.get(columnIndex).getType();
        SqlTypeName colTypeName = colType.getSqlTypeName();
        return switch (colTypeName) {
            case INTEGER -> "org.apache.arrow.vector.IntVector";
            default -> throw new UnsupportedOperationException(
                    "This Arrow column type is currently not supported for the ArrowTableScanOperator");
        };
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
        var relOptTable = (RelOptTableImpl) this.getLogicalSubplan().getTable();
        var arrowTable = (ArrowTable) relOptTable.table();

        ArrowTableReader arrowReader;
        try {
            arrowReader = new DirectArrowTableReader(
                    arrowTable.getArrowFile(),
                    cCtx.getArrowRootAllocator()
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not locate Arrow file in query compilation stage.", e);
        }

        // Store the arrow reader in the CodeGenContext
        int arrowReaderIndex = cCtx.addArrowReader(arrowReader);

        // Perform the actual code generation
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Initialise a variable for accessing the arrow reader
        // ArrowTableReader $tablename$ = cCtx.getArrowReader(arrowReaderIndex);
        String arrowReaderVariableName = cCtx.defineVariable(arrowTable.getName());

        codegenResult.add(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(
                                getLocation(),
                                "evaluation.codegen.infrastructure.data.ArrowTableReader"
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

        // Obtain the VectorSchemaRoot for the Arrow reader
        // VectorSchemaRoot [arrowReaderVariableName]_sr = [arrowReaderVariableName].getVectorSchemaRoot();
        String schemaRootVariableName = cCtx.defineVariable(arrowReaderVariableName + "_sr");

        codegenResult.add(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), "org.apache.arrow.vector.VectorSchemaRoot"),
                        schemaRootVariableName,
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), arrowReaderVariableName),
                                "getVectorSchemaRoot"
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
        //     ([vectorType]) [schemaRootVariableName].get([originalColumnIndex])

        List<RelDataTypeField> rowType = relOptTable.getRowType().getFieldList();
        int numberOutputColumns = this.getLogicalSubplan().projects.size();
        List<AccessPath> projectedColumnAccessPaths = new ArrayList<>(numberOutputColumns);

        for (int outputColumnIndex = 0; outputColumnIndex < numberOutputColumns; outputColumnIndex++) {
            // Obtain data about the column to project
            int originalColumnIndex = this.getLogicalSubplan().projects.get(outputColumnIndex);
            String vectorType = getArrowVectorTypeForColumn(rowType, originalColumnIndex);

            // Define and expose an access path to a variable representing vectors of the projected column
            String preferredOutputColumnVariableName = arrowReaderVariableName + "_vc_" + outputColumnIndex;
            String outputColumnVariableName = cCtx.defineVariable(preferredOutputColumnVariableName);
            projectedColumnAccessPaths.add(outputColumnIndex, new SimpleAccessPath(outputColumnVariableName));

            // Do the projection by generating the variable for this column's vector
            Java.Type projectedColumnType = createReferenceType(getLocation(), vectorType);
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
                                            createAmbiguousNameRef(getLocation(), schemaRootVariableName),
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
