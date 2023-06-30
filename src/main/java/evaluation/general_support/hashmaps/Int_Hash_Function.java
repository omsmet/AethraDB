package evaluation.general_support.hashmaps;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mod;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;

/**
 * The standard hash function used for computing the hash value of an integer column.
 * The hash function is a fixed universal hash function (CLRS page 267) based on the maximum integer
 * value, as this is the maximum number of elements which can occur in an array.
 *
 * It uses the following constants:
 *  - p = 4 294 967 459 > Integer.MAX_VALUE
 *  - a = 3 044 339 450 (random number between 1 and p - 1)
 *  - b = 4 157 137 050 (random number between 0 and p - 1)
 */
public final class Int_Hash_Function {

    /**
     * Prevent instantiation of this class.
     */
    private Int_Hash_Function() {

    }

    /**
     * Hash constant p.
     */
    public static final long hashConstantP = 4_294_967_459L;

    /**
     * Hash constant a.
     */
    public static final long hashConstantA = 3_044_339_450L;

    /**
     * Hash constant b.
     */
    public static final long hashConstantB = 4_157_137_050L;

    /**
     * Method to compute the pre-hash of an integer key. That is, compute the value
     * {@code (a * key + b) mod p} (which is thus not truncated to the appropriate hash length.
     * @param key The key to compute teh pre-hash value for.
     * @return The pre-hash value {@code (a * key + b) mod p}.
     */
    public static long preHash(int key) {
        return (hashConstantA * key + hashConstantB) % hashConstantP;
    }

    /**
     * Special class to encapsulate the objects that result from performing SIMD-ed pre-hashing and
     * loop flattening.
     */
    public static final class Int_Hash_Function_PreHash_And_Flatten_Return_Object {
        public List<Java.Statement> generatedCode = new ArrayList<>();
        public ScalarVariableAccessPath keyColumnAccessPath;
        public ScalarVariableAccessPath keyColumnPreHashAccessPath;
        public Java.Block flattenedForLoopBody;
        public ScalarVariableAccessPath simdVectorIAp;
    }

    /**
     * Method to perform code generation for SIMD-ed per-hashing in the non-vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param keyColumnAccessPath The {@link AccessPath} to the key column to be pre-hashed.
     * @return The generated code and some helper objects to access the flattened for-loop.
     */
    public static Int_Hash_Function_PreHash_And_Flatten_Return_Object preHashAndFlattenSIMD(
            CodeGenContext cCtx,
            AccessPath keyColumnAccessPath
    ) {
        var result = new Int_Hash_Function_PreHash_And_Flatten_Return_Object();

        // Handling depends on the ordinal type that we receive
        if (keyColumnAccessPath instanceof SIMDLoopAccessPath kcap_slap) {
            // Initialise the integer SIMD key vector
            // IntVector [SIMD_Key_Vector_Int] = IntVector.fromSegment(
            //      [kcap_slap.readVectorSpecies()],
            //      [kcap_slap.readMemorySegment()],
            //      [kcap_slap.readArrowVectorOffset()] * [kcap_slap.readArrowVector().TYPE_WIDTH],
            //      java.nio.ByteOrder.LITTLE_ENDIAN,
            //      [kcap_slap.readSIMDMask()]
            // );
            String SIMDIntKeyVectorName = cCtx.defineVariable("SIMD_Key_Vector_Int");
            result.generatedCode.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), kcap_slap.getType()),
                            SIMDIntKeyVectorName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "oCtx"),
                                    "createIntVector",
                                    new Java.Rvalue[]{
                                            kcap_slap.readVectorSpecies(),
                                            kcap_slap.readMemorySegment(),
                                            mul(
                                                    getLocation(),
                                                    kcap_slap.readArrowVectorOffset(),
                                                    createAmbiguousNameRef(
                                                            getLocation(),
                                                            kcap_slap.getArrowVectorAccessPath().getVariableName() + ".TYPE_WIDTH"
                                                    )
                                            ),
                                            createAmbiguousNameRef(getLocation(), "java.nio.ByteOrder.LITTLE_ENDIAN"),
                                            kcap_slap.readSIMDMask()
                                    }
                            )
                    )
            );

            // Cast the SIMD int key vector to a SIMD long key vector
            String SIMDLongKeyVectorName = cCtx.defineVariable("SIMD_Key_Vector_Long");
            // TODO: consider replacing getVectorSpeciesLong() with an allocated variable.
            // LongVector [SIMD_Key_Vector_Long] = (LongVector) [SIMD_Key_Vector_Int].castShape([oCtx.getVectorSpeciesLong()], 0);
            result.generatedCode.add(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(getLocation(), "jdk.incubator.vector.LongVector"),
                            SIMDLongKeyVectorName,
                            createCast(
                                    getLocation(),
                                    createReferenceType(getLocation(), "jdk.incubator.vector.LongVector"),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), SIMDIntKeyVectorName),
                                            "castShape",
                                            new Java.Rvalue[]{
                                                    createMethodInvocation(
                                                            getLocation(),
                                                            createAmbiguousNameRef(getLocation(), "oCtx"),
                                                            "getVectorSpeciesLong"
                                                    ),
                                                    createIntegerLiteral(getLocation(), 0)
                                            }
                                    )
                            )
                    )
            );

            // Compute the a * key part of the pre-hashing
            // LongVector [SIMD_a_mul_key_vector] = [SIMD_Key_Vector_Long].mul(Int_Hash_Function.hashConstantA);
            String SIMDAMulKeyVectorName = cCtx.defineVariable("SIMD_a_mul_key_vector");
            result.generatedCode.add(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(getLocation(), "jdk.incubator.vector.LongVector"),
                            SIMDAMulKeyVectorName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), SIMDLongKeyVectorName),
                                    "mul",
                                    new Java.Rvalue[]{
                                            createAmbiguousNameRef(getLocation(), "Int_Hash_Function.hashConstantA")
                                    }
                            )
                    )
            );

            // Compute the a * key + b part of the pre-hashing
            // LongVector [SIMD_a_mul_key_plus_b_vector] = [SIMD_a_mul_key_vector].add(Int_Hash_Function.hashConstantB);
            String SIMDAMulKeyPlusBVectorName = cCtx.defineVariable("SIMD_a_mul_key_plus_b_vector");
            result.generatedCode.add(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(getLocation(), "jdk.incubator.vector.LongVector"),
                            SIMDAMulKeyPlusBVectorName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), SIMDAMulKeyVectorName),
                                    "add",
                                    new Java.Rvalue[]{
                                            createAmbiguousNameRef(getLocation(), "Int_Hash_Function.hashConstantB")
                                    }
                            )
                    )
            );

            // Still need to take the final computed value mod p which is done below during flattening
            // Flatten the SIMD processing using a for-loop
            // Get the pre-hash values as an array
            // long[] pre_hash_values = [keyColumnPreHashAccessPath].toLongArray();
            String preHashValuesName = cCtx.defineVariable("pre_hash_values");
            result.generatedCode.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveArrayType(getLocation(), Java.Primitive.LONG),
                            preHashValuesName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), SIMDAMulKeyPlusBVectorName),
                                    "toLongArray"
                            )
                    )
            );

            // for (int [simd_vector_i] = 0; [simd_vector_i] < [kcap_slap.readSIMDVectorLengthVariable()]; [simd_vector_i]++) { [simdForLoopBody] }
            String simdVectorIName = cCtx.defineVariable("simd_vector_i");
            result.simdVectorIAp = new ScalarVariableAccessPath(simdVectorIName, P_INT);
            result.flattenedForLoopBody = new Java.Block(getLocation());
            result.generatedCode.add(
                    createForLoop(
                            getLocation(),
                            createPrimitiveLocalVar(
                                    getLocation(),
                                    Java.Primitive.INT,
                                    result.simdVectorIAp.getVariableName(),
                                    "0"
                            ),
                            lt(getLocation(), result.simdVectorIAp.read(), kcap_slap.readSIMDVectorLengthVariable()),
                            postIncrement(getLocation(), result.simdVectorIAp.write()),
                            result.flattenedForLoopBody
                    )
            );

            // Check if the current vector element is valid
            // if (! [kcap_slap.readSIMDMask()].laneIsSet([simd_vector_i])) continue;
            result.flattenedForLoopBody.addStatement(
                    createIfNotContinue(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    kcap_slap.readSIMDMask(),
                                    "laneIsSet",
                                    new Java.Rvalue[] { result.simdVectorIAp.read() }
                            )
                    )
            );

            // Create a variable for the key value
            // int flattened_key = [kcap_slap.readArrowVector()].get([kcap_slap.readArrowVectorOffset()] + [simd_vector_i]);
            result.keyColumnAccessPath = new ScalarVariableAccessPath(cCtx.defineVariable("flattened_key"), P_INT);
            result.flattenedForLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), result.keyColumnAccessPath.getType()),
                            result.keyColumnAccessPath.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    kcap_slap.readArrowVector(),
                                    "get",
                                    new Java.Rvalue[]{
                                            plus(getLocation(), kcap_slap.readArrowVectorOffset(), result.simdVectorIAp.read())
                                    }
                            )
                    )
            );

            // Create a variable for the pre-hash value
            // long pre_hash_value = pre_hash_values[simd_vector_i] % Int_Hash_Function.hashConstantP;
            result.keyColumnPreHashAccessPath = new ScalarVariableAccessPath(cCtx.defineVariable("pre_hash_value"), P_LONG);
            result.flattenedForLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), result.keyColumnPreHashAccessPath.getType()),
                            result.keyColumnPreHashAccessPath.getVariableName(),
                            mod(
                                    getLocation(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), preHashValuesName),
                                            result.simdVectorIAp.read()
                                    ),
                                    createAmbiguousNameRef(getLocation(), "Int_Hash_Function.hashConstantP")
                            )
                    )
            );

        } else {
            throw new UnsupportedOperationException(
                    "Int_Hash_Function.preHashAndFlattenSIMD does not support this access path for SIMD pre-hashing");
        }

        return result;
    }
}
