package util.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.commons.nullanalysis.NotNull;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import java.util.List;

/**
 * This class contains helper methods to make code generation with Janino easier.
 */
public class GeneratorMethods {

    /**
     * Prevent instantiation of this class.
     */
    private GeneratorMethods() {

    }

    // TODO: javadoc
    public static Java.PackageMemberClassDeclaration createClass(
            Java.CompilationUnit targetUnit,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            String className,
            Java.Type extendsType
    ) {
        Java.PackageMemberClassDeclaration clazz = new Java.PackageMemberClassDeclaration(
                getLocation(),
                null,
                new Java.Modifier[] { new Java.AccessModifier(accessModifier.toString(), getLocation()) },
                className,
                null, // typeParameters not used for now
                extendsType,
                new Java.Type[0] // implementedTypes not used for now
        );
        targetUnit.addPackageMemberTypeDeclaration(clazz);
        return clazz;
    }

    // TODO: javadoc
    public static Java.Block createBlock() {
        return new Java.Block(getLocation());
    }

    // TODO: javadoc

    public static Java.LocalVariableDeclarationStatement createLocalVarDecl(
            Java.Primitive primitiveType,
            String variableName
    ) {
        return createLocalVarDecl(primitiveType, variableName, null);
    }

    public static Java.LocalVariableDeclarationStatement createLocalVarDecl(
            Java.Primitive primitiveType,
            String variableName,
            String initialValue
    ) {
        Java.Literal initialValueLiteral = null;
        if (initialValue != null) { // Allow declaration of uninitialised variables
            initialValueLiteral = switch (primitiveType) {
                case INT -> createIntegerLiteral(initialValue);
                case DOUBLE, FLOAT -> createFloatingPointLiteral(initialValue);
                default ->
                        throw new UnsupportedOperationException("This primitive type is not yet supported for local variable declarations");
            };
        }


        return new Java.LocalVariableDeclarationStatement(
                getLocation(),
                new Java.Modifier[0], // Local variables do not need an access modifier ? todo: are these only used in methods or also in classes?
                createPrimitiveType(primitiveType),
                new Java.VariableDeclarator[] {
                        new Java.VariableDeclarator(
                                getLocation(),
                                variableName,
                                0,
                                initialValueLiteral
                        )
                }
        );
    }

    public static Java.AmbiguousName createVariableRef(String varName) {
        return new Java.AmbiguousName(
                getLocation(),
                new String[] { varName }
        );
    }

    public static Java.ExpressionStatement createVariableAssignmentExpr(Java.Lvalue lhs, Java.Rvalue rhs) throws CompileException {
        return new Java.ExpressionStatement(
                createVariableAssignment(lhs, rhs)
        );
    }

    private static Java.Assignment createVariableAssignment(Java.Lvalue lhs, Java.Rvalue rhs) {
        return new Java.Assignment(
                getLocation(),
                lhs,
                "=",
                rhs
        );
    }

    public static Java.Type createPrimitiveType(Java.Primitive type) {
        return new Java.PrimitiveType(getLocation(), type);
    }

    public static Java.IntegerLiteral createIntegerLiteral(String value) {
        return new Java.IntegerLiteral(getLocation(), value);
    }

    public static Java.FloatingPointLiteral createFloatingPointLiteral(String value) {
        return new Java.FloatingPointLiteral(getLocation(), value);
    }

    public static Java.BinaryOperation createAdditionOp(Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(
                getLocation(),
                lhs,
                "+",
                rhs
        );
    }

    public static Java.BinaryOperation createMultiplicationOp(Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(
                getLocation(),
                lhs,
                "*",
                rhs
        );
    }

    public static void createMethod(
            Java.PackageMemberClassDeclaration clazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            List<? extends Java.BlockStatement> statements
    ) {
        createMethod(
                clazz,
                accessModifier,
                returnType,
                methodName,
                new Java.FunctionDeclarator.FormalParameters(getLocation()),
                statements
        );
    }

    public static void createMethod(
            Java.PackageMemberClassDeclaration clazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            @NotNull Java.FunctionDeclarator.FormalParameters params,
            List<? extends Java.BlockStatement> statements
    ) {
        createMethod(
                clazz,
                accessModifier,
                returnType,
                methodName,
                params,
                new Java.Type[0],
                statements
        );
    }

    public static void createMethod(
            Java.PackageMemberClassDeclaration clazz,
            Access accessModifier, // TODO: Also think about static, abstract, final etc.
            Java.Type returnType,
            String methodName,
            @NotNull Java.FunctionDeclarator.FormalParameters params,
            @NotNull Java.Type[] exceptions,
            List<? extends Java.BlockStatement> statements
    ) {
        Java.MethodDeclarator method = new Java.MethodDeclarator(
                getLocation(),
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
        clazz.addDeclaredMethod(method);
    }

    public static Java.ReturnStatement createReturnStm(Java.Rvalue returnVal) {
        return new Java.ReturnStatement(
                getLocation(),
                returnVal
        );
    }

    /**
     * A "Clever" method to get a location from a stack trace.
     */
    private static Location getLocation() {
        Exception         e   = new Exception();
        StackTraceElement ste = e.getStackTrace()[1]; // we only care about our caller
        return new Location(ste.getFileName(), ste.getLineNumber(), 0);
    }

}
