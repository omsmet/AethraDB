package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.commons.nullanalysis.NotNull;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import javax.annotation.Nullable;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * Class containing helper methods for generating method-related code with Janino.
 */
public class JaninoMethodGen {

    /**
     * Prevent this class from being instantiated.
     */
    private JaninoMethodGen() {

    }

    /**
     * Method for generating a set of formal parameters used in a method invocation/definition.
     * @param location The location from which the formal parameters are requested for generation.
     * @param parameters The {@code FormalParameter}s to generate for.
     * @return A {@code FormalParameters} object containing the provided formal parameters.
     */
    public static Java.FunctionDeclarator.FormalParameters createFormalParameters(
            Location location,
            Java.FunctionDeclarator.FormalParameter[] parameters
    ) {
        return new Java.FunctionDeclarator.FormalParameters(
                location,
                parameters,
                false
        );
    }

    /**
     * Method for generating a formal parameter used in a method invocation/definition.
     * @param location The location from which the formal parameter is requested for generation.
     * @param parameterType The type of the parameter to generate.
     * @param parameterName The name of the parameter to generate.
     * @return The generated formal paramter.
     */
    public static Java.FunctionDeclarator.FormalParameter createFormalParameter(
            Location location,
            Java.Type parameterType,
            String parameterName
    ) {
        return new Java.FunctionDeclarator.FormalParameter(
                location,
                new Java.Modifier[0], // Modifiers currently not used
                parameterType,
                parameterName
        );
    }

    /**
     * Method for adding a method definition to an existing class definition.
     * @param location The location from which the method is requested for generation.
     * @param targetClazz The class to which the method should be added.
     * @param accessModifier The access that should be applied to the method.
     * @param returnType The type of the result returned by the method.
     * @param methodName The name of the method to be generated.
     * @param statements The body of the method.
     */
    public static void createMethod(
            Location location,
            Java.PackageMemberClassDeclaration targetClazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            List<? extends Java.BlockStatement> statements
    ) {
        createMethod(
                location,
                targetClazz,
                accessModifier,
                returnType,
                methodName,
                new Java.FunctionDeclarator.FormalParameters(getLocation()),
                statements
        );
    }

    /**
     * Method for adding a method definition to an existing class definition.
     * @param location The location from which the method is requested for generation.
     * @param targetClazz The class to which the method should be added.
     * @param accessModifier The access that should be applied to the method.
     * @param returnType The type of the result returned by the method.
     * @param methodName The name of the method to be generated.
     * @param params The parameters that the method takes.
     * @param statements The body of the method.
     */
    public static void createMethod(
            Location location,
            Java.PackageMemberClassDeclaration targetClazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            @NotNull Java.FunctionDeclarator.FormalParameters params,
            List<? extends Java.BlockStatement> statements
    ) {
        createMethod(
                location,
                targetClazz,
                accessModifier,
                returnType,
                methodName,
                params,
                new Java.Type[0],
                statements
        );
    }

    /**
     * Method for adding a method definition to an existing class definition.
     * @param location The location from which the method is requested for generation.
     * @param targetClazz The class to which the method should be added.
     * @param accessModifier The access that should be applied to the method.
     * @param returnType The type of the result returned by the method.
     * @param methodName The name of the method to be generated.
     * @param params The parameters that the method takes.
     * @param exceptions The exceptions that the method may throw.
     * @param statements The body of the method.
     */
    public static void createMethod(
            Location location,
            Java.PackageMemberClassDeclaration targetClazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            @NotNull Java.FunctionDeclarator.FormalParameters params,
            @NotNull Java.Type[] exceptions,
            List<? extends Java.BlockStatement> statements
    ) {
        Java.MethodDeclarator method = new Java.MethodDeclarator(
                location,
                null,
                new Java.Modifier[] { new Java.AccessModifier(accessModifier.toString(), getLocation()) },
                null,
                returnType,
                methodName,
                params,
                exceptions,
                null,
                statements
        );
        targetClazz.addDeclaredMethod(method);
    }

    /**
     * Method for adding a constructor method to an existing class definition.
     * @param location The location at which the constructor generation is requested.
     * @param targetClazz The class to which the constructor should be added.
     * @param accessModifier The access modifier that should be applied to the constructor.
     * @param formalParameters The parameters that the constructor takes.
     * @param constructorInvocation A possible super-class constructor invocation that should be performed.
     * @param statements The body of the constructor.
     */
    public static void createConstructor(
            Location location,
            Java.PackageMemberClassDeclaration targetClazz,
            Access accessModifier,
            Java.FunctionDeclarator.FormalParameters formalParameters,
            @Nullable Java.ConstructorInvocation constructorInvocation,
            List<? extends Java.BlockStatement> statements
    ) {
        Java.ConstructorDeclarator constructor = new Java.ConstructorDeclarator(
                location,                                                               // location
                null,                                                                   // docComment
                new Java.Modifier[] {                                                   // modifiers
                        new Java.AccessModifier(accessModifier.toString(), location) },
                formalParameters,                                                       // formalParameters
                new Java.Type[0],                                                       // constructor exceptions currently not used
                constructorInvocation,                                                  // possibility to invoke super constructor
                statements                                                              // the additional statements to execute
        );
        targetClazz.addConstructor(constructor);
    }

    /**
     * Method for creating a statement that invokes a method.
     * @param location The location at which the method invocation statement is being requested for generation.
     * @param targetContainer The object containing the method to be invoked.
     * @param methodName The name of the method to be invoked.
     * @return The generated method invocation statement.
     */
    public static Java.Statement createMethodInvocationStm(
            Location location,
            Java.Atom targetContainer,
            String methodName
    ) {
        return createMethodInvocationStm(location, targetContainer, methodName, new Java.Rvalue[0]);
    }

    /**
     * Method for creating a statement that invokes a method.
     * @param location The location at which the method invocation statement is being requested for generation.
     * @param targetContainer The object containing the method to be invoked.
     * @param methodName The name of the method to be invoked.
     * @param arguments The arguments to be used on method invocation.
     * @return The generated method invocation statement.
     */
    public static Java.Statement createMethodInvocationStm(
            Location location,
            Java.Atom targetContainer,
            String methodName,
            Java.Rvalue[] arguments
    ) {
        try {
            return new Java.ExpressionStatement(createMethodInvocation(
                    location,
                    targetContainer,
                    methodName,
                    arguments
            ));
        } catch (CompileException e) {
            throw new RuntimeException(
                    "Exception occurred while executing createMethodInvocationStm", e);
        }
    }

    /**
     * Method for creating a method invocation expression.
     * @param location The location at which the method invocation is being requested for generation.
     * @param targetContainer The object containing the method to be invoked.
     * @param methodName The name of the method to be invoked.
     * @return The generated method invocation expression.
     */
    public static Java.MethodInvocation createMethodInvocation(
            Location location,
            Java.Atom targetContainer,
            String methodName
    ) {
        return createMethodInvocation(location, targetContainer, methodName, new Java.Rvalue[0]);
    }

    /**
     * Method for creating a method invocation expression.
     * @param location The location at which the method invocation is being requested for generation.
     * @param targetContainer The object containing the method to be invoked.
     * @param methodName The name of the method to be invoked.
     * @param arguments The arguments to be used on method invocation.
     * @return The generated method invocation expression.
     */
    public static Java.MethodInvocation createMethodInvocation(
        Location location,
        Java.Atom targetContainer,
        String methodName,
        Java.Rvalue[] arguments
    ) {
        return new Java.MethodInvocation(
                location,
                targetContainer,
                methodName,
                arguments
        );
    }

    /**
     * Method for generating a block which can contain statements.
     * @param location The location at which the block is being requested for generation.
     * @return A new {@link org.codehaus.janino.Java.Block} instance.
     */
    public static Java.Block createBlock(Location location) {
        return new Java.Block(location);
    }

    /**
     * Method for generating a return statement.
     * @param location The location at which the return statement is requested for generation.
     * @param returnVal The value to be returned by the return statement.
     * @return The generated return statement.
     */
    public static Java.ReturnStatement createReturnStm(Location location, Java.Rvalue returnVal) {
        return new Java.ReturnStatement(
                location,
                returnVal
        );
    }

}
