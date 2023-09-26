package AethraDB.evaluation.general_support.hashmaps;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import org.codehaus.janino.Java;

import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;

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
        if (sourceType == QueryVariableType.S_FL_BIN || sourceType == QueryVariableType.S_VARCHAR) {
            String copyVariableName = sourceVariableName + "_copy";

            // int source_length = source.length;
            ScalarVariableAccessPath keyLength =
                    new ScalarVariableAccessPath(sourceVariableName + "_length", QueryVariableType.P_INT);
            codeGenResult.addStatement(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), keyLength.getType()),
                            keyLength.getVariableName(),
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName),
                                    "length"
                            )
                    )
            );

            // byte[] source_copy = new byte[source_length];
            codeGenResult.addStatement(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), sourceType),
                            copyVariableName,
                            JaninoGeneralGen.createNewPrimitiveArray(
                                    JaninoGeneralGen.getLocation(),
                                    Java.Primitive.BYTE,
                                    keyLength.read()
                            )
                    )
            );

            // System.arraycopy(source, 0, source_copy, 0, source_length);
            codeGenResult.addStatement(
                    JaninoMethodGen.createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), copyVariableName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    keyLength.read()
                            }
                    )
            );

            // Return an r-value corresponding to the copied array
            return JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), copyVariableName);

        } else {
            // Return an r-value corresponding to the original variable
            return JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName);

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
        if (sourceType == QueryVariableType.S_FL_BIN || sourceType == QueryVariableType.S_VARCHAR) {
            String copyVariableName = sourceVariableName + "_copy";

            // int source_length = source.length;
            ScalarVariableAccessPath keyLength =
                    new ScalarVariableAccessPath(sourceVariableName + "_length", QueryVariableType.P_INT);
            codeGenResult.add(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), keyLength.getType()),
                            keyLength.getVariableName(),
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName),
                                    "length"
                            )
                    )
            );

            // byte[] source_copy = new byte[source_length];
            codeGenResult.add(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), sourceType),
                            copyVariableName,
                            JaninoGeneralGen.createNewPrimitiveArray(
                                    JaninoGeneralGen.getLocation(),
                                    Java.Primitive.BYTE,
                                    keyLength.read()
                            )
                    )
            );

            // System.arraycopy(source, 0, source_copy, 0, source_length);
            codeGenResult.add(
                    JaninoMethodGen.createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), copyVariableName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    keyLength.read()
                            }
                    )
            );

            // Return an r-value corresponding to the copied array
            return JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), copyVariableName);

        } else {
            // Return an r-value corresponding to the original variable
            return JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), sourceVariableName);

        }

    }

}
