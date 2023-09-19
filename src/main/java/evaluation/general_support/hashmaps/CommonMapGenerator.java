package evaluation.general_support.hashmaps;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.codehaus.janino.Java;

import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_VARCHAR;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * Class containing functionality which is shared between the different map generators.
 */
public class CommonMapGenerator {

    /**
     * Prevent instantiation of this class.
     */
    private CommonMapGenerator() {

    }

    /**
     * Method which generates the {@link Java.Rvalue} for an assignment in a map where the assigned
     * value is potentially a byte array, which needs to be copied into a new variable.
     * @param sourceType The type of the variable to be assigned from.
     * @param sourceVariableName The name of the variable to be assigned from.
     * @param codeGenResult The target to introduce generated code to if necessary.
     * @return The {@link Java.Rvalue} corresponding to a non-byte array variable for non-byte array
     * variables and a {@link Java.Rvalue} corresponding to a copy of a byte array for byte
     * array values.
     */
    public static Java.Rvalue createMapAssignmentRValue(
            QueryVariableType sourceType, String sourceVariableName, Java.Block codeGenResult) {

        // Determine if we need to copy the value into a new array
        if (sourceType == S_FL_BIN || sourceType == S_VARCHAR) {
            String copyVariableName = sourceVariableName + "_copy";

            // int source_length = source.length;
            ScalarVariableAccessPath keyLength =
                    new ScalarVariableAccessPath(sourceVariableName + "_length", P_INT);
            codeGenResult.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), keyLength.getType()),
                            keyLength.getVariableName(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), sourceVariableName),
                                    "length"
                            )
                    )
            );

            // byte[] source_copy = new byte[source_length];
            codeGenResult.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), sourceType),
                            copyVariableName,
                            createNewPrimitiveArray(
                                    getLocation(),
                                    Java.Primitive.BYTE,
                                    keyLength.read()
                            )
                    )
            );

            // System.arraycopy(source, 0, source_copy, 0, source_length);
            codeGenResult.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    createAmbiguousNameRef(getLocation(), sourceVariableName),
                                    createIntegerLiteral(getLocation(), 0),
                                    createAmbiguousNameRef(getLocation(), copyVariableName),
                                    createIntegerLiteral(getLocation(), 0),
                                    keyLength.read()
                            }
                    )
            );

            // Return an r-value corresponding to the copied array
            return createAmbiguousNameRef(getLocation(), copyVariableName);

        } else {
            // Return an r-value corresponding to the original variable
            return createAmbiguousNameRef(getLocation(), sourceVariableName);

        }

    }

    /**
     * Method which generates the {@link Java.Rvalue} for an assignment in a map where the assigned
     * value is potentially a byte array, which needs to be copied into a new variable.
     * @param sourceType The type of the variable to be assigned from.
     * @param sourceVariableName The name of the variable to be assigned from.
     * @param codeGenResult The target to introduce generated code to if necessary.
     * @return The {@link Java.Rvalue} corresponding to a non-byte array variable for non-byte array
     * variables and a {@link Java.Rvalue} corresponding to a copy of a byte array for byte
     * array values.
     */
    public static Java.Rvalue createMapAssignmentRValue(
            QueryVariableType sourceType, String sourceVariableName, List<Java.Statement> codeGenResult) {

        // Determine if we need to copy the value into a new array
        if (sourceType == S_FL_BIN || sourceType == S_VARCHAR) {
            String copyVariableName = sourceVariableName + "_copy";

            // int source_length = source.length;
            ScalarVariableAccessPath keyLength =
                    new ScalarVariableAccessPath(sourceVariableName + "_length", P_INT);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), keyLength.getType()),
                            keyLength.getVariableName(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), sourceVariableName),
                                    "length"
                            )
                    )
            );

            // byte[] source_copy = new byte[source_length];
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), sourceType),
                            copyVariableName,
                            createNewPrimitiveArray(
                                    getLocation(),
                                    Java.Primitive.BYTE,
                                    keyLength.read()
                            )
                    )
            );

            // System.arraycopy(source, 0, source_copy, 0, source_length);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    createAmbiguousNameRef(getLocation(), sourceVariableName),
                                    createIntegerLiteral(getLocation(), 0),
                                    createAmbiguousNameRef(getLocation(), copyVariableName),
                                    createIntegerLiteral(getLocation(), 0),
                                    keyLength.read()
                            }
                    )
            );

            // Return an r-value corresponding to the copied array
            return createAmbiguousNameRef(getLocation(), copyVariableName);

        } else {
            // Return an r-value corresponding to the original variable
            return createAmbiguousNameRef(getLocation(), sourceVariableName);

        }

    }

}
