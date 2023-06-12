package evaluation.codegen.operators;

import calcite.operators.LogicalArrowTableScan;
import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDMemorySegmentAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDVectorMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDVectorSpeciesAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.DirectArrowTableReader;
import evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.janino.Java;
import util.arrow.ArrowTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignment;

/**
 * {@link CodeGenOperator} which generates code for reading data from an Arrow file.
 */
public class ArrowTableScanOperator extends CodeGenOperator<LogicalArrowTableScan> {

    /**
     * Creates an {@link ArrowTableScanOperator} for a specific {@link LogicalArrowTableScan}.
     * @param logicalSubplan The {@link LogicalArrowTableScan} to create a scan operator for.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     */
    public ArrowTableScanOperator(LogicalArrowTableScan logicalSubplan, boolean simdEnabled) {
        super(logicalSubplan, simdEnabled);
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

        // The specific for-loop that needs to be generated depends on whether SIMD is enabled
        Java.Block forLoopBody = createBlock(getLocation());
        if (!this.simdEnabled) {
            // for (int aviv = 0; aviv < firstColumnVector.getValueCount; aviv++) { [forLoopBody] }
            String avivName = cCtx.defineVariable("aviv");
            ScalarVariableAccessPath avivAccessPath = new ScalarVariableAccessPath(avivName);
            whileLoopBody.addStatement(
                    createForLoop(
                            getLocation(),
                            createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, avivName, "0"),
                            lt(
                                    getLocation(),
                                    avivAccessPath.read(),
                                    createMethodInvocation(
                                            getLocation(),
                                            // Valid to cast to ArrowVectorAccessPath as this is delivered by genericProduce(..)
                                            ((ArrowVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(0)).read(),
                                            "getValueCount"
                                    )
                            ),
                            JaninoOperatorGen.postIncrement(getLocation(), createAmbiguousNameRef(getLocation(), avivName)),
                            forLoopBody
                    )
            );

            // Update the ordinal to access path mapping by building a new mapping that replaces each
            // arrow vector variable in the current mapping with one that performs an indexed read using
            // the aviv index variable
            List<AccessPath> updatedOrdinalMapping =
                    cCtx.getCurrentOrdinalMapping().stream().map(entry ->
                            // Valid to cast to entry ArrowVectorAccessPath as this is delivered by genericProduce(..)
                            (AccessPath) new IndexedArrowVectorElementAccessPath((ArrowVectorAccessPath) entry, avivAccessPath)
                    ).toList();
            cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);
        } else {
            // int commonSIMDVectorLength = ...; (allocated as scan surrounding variable for reuse)
            int commonSIMDVectorLength =
                    OptimisationContext.computeCommonSIMDVectorLength(this.getLogicalSubplan().getRowType().getFieldList());
            String commonSIMDVectorLengthName = cCtx.defineScanSurroundingVariables(
                    "commonSIMDVectorLength",
                    createPrimitiveType(getLocation(), Java.Primitive.INT),
                    createIntegerLiteral(getLocation(), commonSIMDVectorLength),
                    false);
            ScalarVariableAccessPath commonSIMDVectorLengthAP = new ScalarVariableAccessPath(commonSIMDVectorLengthName);

            // int arrowVectorLength = firstColumnVector.getValueCount();
            String arrowVectorLengthName = cCtx.defineVariable("arrowVectorLength");
            ScalarVariableAccessPath arrowVectorLengthAP = new ScalarVariableAccessPath(arrowVectorLengthName);
            whileLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            arrowVectorLengthName,
                            createMethodInvocation(
                                    getLocation(),
                                    // Valid to cast to ArrowVectorAccessPath as this is delivered by genericProduce(..)
                                    ((ArrowVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(0)).read(),
                                    "getValueCount"
                            )
                    )
            );

            // Wrap compatible arrow vectors in a memory segment (based on the row type)
            // and store them so they can be added to the access path
            List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
            List<RelDataTypeField> columnTypes = this.getLogicalSubplan().getRowType().getFieldList();
            List<SIMDMemorySegmentAccessPath> memorySegmentAccessPaths = new ArrayList<>(currentOrdinalMapping.size());
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                if (columnTypes.get(i).getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                    // MemorySegment col_[i]_ms = MemorySegment.ofAddress(
                    //                    [currentOrdinalMapping.get(i)].getDataBufferAddress(),
                    //                    [arrowVectorLength] * [currentOrdinalMapping.get(i)].TYPE_WIDTH);
                    String memorySegmentName = cCtx.defineVariable("col_" + i + "_ms");
                    whileLoopBody.addStatement(
                            createLocalVariable(
                                    getLocation(),
                                    createReferenceType(getLocation(), "java.lang.foreign.MemorySegment"),
                                    memorySegmentName,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "oCtx"),
                                            "createMemorySegmentForAddress",
                                            new Java.Rvalue[] {
                                                    createMethodInvocation(
                                                            getLocation(),
                                                            // Valid to cast to ArrowVectorAccessPath as this is delivered by genericProduce(..)
                                                            ((ArrowVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(i)).read(),
                                                            "getDataBufferAddress"
                                                    ),
                                                    mul(
                                                            getLocation(),
                                                            arrowVectorLengthAP.read(),
                                                            createAmbiguousNameRef(
                                                                    getLocation(),
                                                                    ((ArrowVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(i)).getVariableName() + ".TYPE_WIDTH"
                                                            )
                                                    )
                                            }
                                    )
                            )
                    );
                    memorySegmentAccessPaths.add(i, new SIMDMemorySegmentAccessPath(memorySegmentName));

                } else {
                    throw new UnsupportedOperationException("ArrowScanOperator.produceNonVec: Cannot wrap this arrow vector in a memory segment");
                }
            }

            // for (int currentVectorOffset = 0; currentVectorOffset < arrowVectorLength; currentVectorOffset += commonSIMDVectorLength) { [forLoopBody] }
            String currentVectorOffsetName = cCtx.defineVariable("currentVectorOffset");
            ScalarVariableAccessPath currentVectorOffsetAccessPath = new ScalarVariableAccessPath(currentVectorOffsetName);
            whileLoopBody.addStatement(
                    createForLoop(                      // for (
                            getLocation(),
                            createPrimitiveLocalVar(    // int currentVectorOffset = 0;
                                    getLocation(),
                                    Java.Primitive.INT,
                                    currentVectorOffsetName,
                                    "0"
                            ),
                            lt(                         // currentVectorOffset < firstColumnVector.getValueCount()
                                        getLocation(),
                                        currentVectorOffsetAccessPath.read(),
                                        arrowVectorLengthAP.read()
                            ),
                            createVariableAdditionAssignment(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), currentVectorOffsetName),
                                    commonSIMDVectorLengthAP.read()
                            ),
                            forLoopBody
                    )
            );

            // Add the in-range mask to the for-loop body: assume an int vector for now as most common
            // Todo: fix properly

            // Define integer vector species outside the scan
            // VectorSpecies<Integer> [intVectorSpeciesVarName] = oCtx.getIntVectorSpecies(commonSIMDVectorLength);
            String intVectorSpeciesVarName = cCtx.defineScanSurroundingVariables(
                "IntVectorSpecies",
                    createReferenceType(
                            getLocation(),
                            "jdk.incubator.vector.VectorSpecies",
                            "Integer"
                    ),
                    createMethodInvocation(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "oCtx"),
                            "getIntVectorSpecies",
                            new Java.Rvalue[] {
                                    commonSIMDVectorLengthAP.read()
                            }
                    ),
                    false
            );
            // Note this vector species has been defined
            Map<SqlTypeName, SIMDVectorSpeciesAccessPath> definedVectorSpecies = new HashMap<>();
            definedVectorSpecies.put(SqlTypeName.INTEGER, new SIMDVectorSpeciesAccessPath(intVectorSpeciesVarName));

            // VectorMask<Integer> inRangeSIMDMask = [intVectorSpeciesVarName].indexInRange(
            //      [currentVectorOffsetAccessPath.read()],
            //      [arrowVectorLengthAP.read()]
            // );
            String inRangeSIMDMaskName = cCtx.defineVariable("inRangeSIMDMask");
            SIMDVectorMaskAccessPath inRangeSIMDMaskAP = new SIMDVectorMaskAccessPath(inRangeSIMDMaskName);
            forLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(
                                    getLocation(),
                                    "jdk.incubator.vector.VectorMask",
                                    "Integer"
                            ),
                            inRangeSIMDMaskName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), intVectorSpeciesVarName),
                                    "indexInRange",
                                    new Java.Rvalue[] {
                                            currentVectorOffsetAccessPath.read(),
                                            arrowVectorLengthAP.read()
                                    }
                            )
                    )
            );

            // Update the ordinal to access path mapping by building a new mapping that replaces each
            // arrow vector variable in the current mapping with one that symbolises the current SIMD
            // compatible wrapping
            List<AccessPath> updatedOrdinalMapping = new ArrayList<>();
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                // Compute the vector species required for this Arrow vector row values based on its type
                SIMDVectorSpeciesAccessPath svsap;
                SqlTypeName currentVectorType =
                        this.getLogicalSubplan().getRowType().getFieldList().get(i).getType().getSqlTypeName();

                if (currentVectorType == SqlTypeName.INTEGER) {
                    if (!definedVectorSpecies.containsKey(currentVectorType)) {
                        // Need to define the vector species first
                        String vectorSpeciesVarName = cCtx.defineScanSurroundingVariables(
                                "IntVectorSpecies",
                                createReferenceType(
                                        getLocation(),
                                        "jdk.incubator.vector.VectorSpecies",
                                        "Integer"
                                ),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "oCtx"),
                                        "getIntVectorSpecies",
                                        new Java.Rvalue[] {
                                                commonSIMDVectorLengthAP.read()
                                        }
                                ),
                                false
                        );

                        definedVectorSpecies.put(
                                currentVectorType,
                                new SIMDVectorSpeciesAccessPath(vectorSpeciesVarName)
                        );
                    }

                    svsap = definedVectorSpecies.get(currentVectorType);
                } else {
                    throw new UnsupportedOperationException(
                            "ArrowTableScanOperator.produceNonVec does not support this data type for SIMD wrapping");
                }

                // Create the updated access path
                updatedOrdinalMapping.add(i, new SIMDLoopAccessPath(
                        // Valid to cast to entry ArrowVectorAccessPath as this is delivered by genericProduce(..)
                        (ArrowVectorAccessPath) currentOrdinalMapping.get(i),
                        arrowVectorLengthAP,
                        currentVectorOffsetAccessPath,
                        commonSIMDVectorLengthAP,
                        inRangeSIMDMaskAP,
                        memorySegmentAccessPaths.get(i),
                        svsap
                ));
            }

            cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);
        }

        // Have the parent operator consume the result within the for loop
        forLoopBody.addStatements(nonVecParentConsume(cCtx, oCtx));

        // Return the generated code after wrapping it in the scan surrounding variables
        return wrapInScanSurroundingVariables(cCtx, oCtx, codegenResult);
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
        return wrapInScanSurroundingVariables(cCtx, oCtx, codegenResult);
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
        } catch (Exception e) {
            throw new RuntimeException("Could not create ArrowTableReader in query compilation stage.", e);
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
            projectedColumnAccessPaths.add(outputColumnIndex, new ArrowVectorAccessPath(outputColumnVariableName));

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

    /**
     * Method for handling the allocation of scan-surrounding variables.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param scanCodeToWrap The code of the generated scan operator to wrap.
     */
    public List<Java.Statement> wrapInScanSurroundingVariables(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            List<Java.Statement> scanCodeToWrap
    ) {
        List<Java.Statement> result = new ArrayList<>();

        // Allocate the variables
        var scanSurroundingAllocations = cCtx.getScanSurroundingVariables();
        result.addAll(scanSurroundingAllocations.left);

        // Add the code of the scan operator
        result.addAll(scanCodeToWrap);

        // Deallocate the scan variables
        for (String varToDallocate : scanSurroundingAllocations.right) {
            result.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "release",
                            new Java.Rvalue[] {
                                    createAmbiguousNameRef(getLocation(), varToDallocate)
                            }
                    )
            );
        }

        return result;
    }
}
