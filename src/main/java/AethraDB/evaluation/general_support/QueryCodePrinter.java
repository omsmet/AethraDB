package AethraDB.evaluation.general_support;

import org.codehaus.janino.Java;

import java.util.List;

/**
 * Class containing functionality to print generated query code to a human-readable format.
 */
public class QueryCodePrinter {

    /**
     * Prevent instantiation of the class.
     */
    private QueryCodePrinter() {

    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     */
    public static void printCode(List<Java.Statement> code) {
        printCode(code, 0);
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     * @param indentationLevel The number of spaces to add before each line of the code.
     */
    private static void printCode(List<? extends Java.BlockStatement> code, int indentationLevel) {
        for (Java.BlockStatement statement : code)
            printCode(statement, indentationLevel);
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     * @param indentationLevel The number of spaces to add before each line of the code.
     */
    private static void printCode(Java.BlockStatement code, int indentationLevel) {
        if (code instanceof Java.Block block) {
            printCode(block.statements, indentationLevel);

        } else if (code instanceof Java.WhileStatement whileStatement) {
            String whileGuardLine = "while (" + printRvalue(whileStatement.condition) + ") {";
            System.out.print(whileGuardLine.indent(indentationLevel));
            printCode(whileStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.ForStatement forStatement) {
            String forGuardLine =
                    "for (" + forStatement.init.toString() + " "
                            + forStatement.condition.toString() + "; "
                            + forStatement.update[0].toString() + ") {";
            System.out.print(forGuardLine.indent(indentationLevel));
            printCode(forStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.ForEachStatement forEachStatement) {
            String forGuardLine =
                    "foreach (" + forEachStatement.currentElement.toString() + " : "
                            + forEachStatement.expression.toString() + ") {";
            System.out.print(forGuardLine.indent(indentationLevel));
            printCode(forEachStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.IfStatement ifStatement) {
            String ifGuardLine = "if (" + printRvalue(ifStatement.condition) + ") {";

            System.out.print(ifGuardLine.indent(indentationLevel));
            printCode(ifStatement.thenStatement, indentationLevel + 4);
            if (ifStatement.elseStatement != null) {
                System.out.print("} else {".indent(indentationLevel));
                printCode(ifStatement.elseStatement, indentationLevel + 4);
            }
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.LocalClassDeclarationStatement lcdStatement) {
            Java.LocalClassDeclaration lcd = lcdStatement.lcd;
            String classDeclarationLine = "";
            for (Java.Modifier modifier : lcd.getModifiers())
                classDeclarationLine += modifier.toString() + " ";
            classDeclarationLine += "class " + lcd.getName() + " {";
            System.out.print(classDeclarationLine.indent(indentationLevel));

            for (Java.MemberTypeDeclaration mtd : lcd.getMemberTypeDeclarations()) {
                printCode(mtd, indentationLevel + 4);
            }

            printCode(lcd.fieldDeclarationsAndInitializers, indentationLevel + 4);
            System.out.println();

            for (Java.ConstructorDeclarator constructorDeclarator : lcd.constructors) {
                String constructorDeclarationLine = constructorDeclarator.getModifiers()[0].toString();
                constructorDeclarationLine += " " + constructorDeclarator.getDeclaringType().toString() + "(";

                Java.FunctionDeclarator.FormalParameter[] parameters = constructorDeclarator.formalParameters.parameters;
                for (int i = 0; i < parameters.length; i++) {
                    constructorDeclarationLine += parameters[i].toString();

                    if (i != parameters.length - 1)
                        constructorDeclarationLine += ", ";
                }

                constructorDeclarationLine += ") {";
                System.out.print(constructorDeclarationLine.indent(indentationLevel + 4));
                if (constructorDeclarator.constructorInvocation != null)
                    printCode(constructorDeclarator.constructorInvocation, indentationLevel + 8);

                printCode(constructorDeclarator.statements, indentationLevel + 8);
                System.out.print("}".indent(indentationLevel + 4));
            }
            System.out.println();

            for (Java.MethodDeclarator methodDeclarator : lcd.getMethodDeclarations()) {
                String methodHeader = methodDeclarator.modifiers[0].toString() + " ";
                methodHeader += methodDeclarator.type.toString() + " ";
                methodHeader += methodDeclarator + " {";
                System.out.print(methodHeader.indent(indentationLevel + 4));

                printCode(methodDeclarator.statements, indentationLevel + 8);

                System.out.print("}".indent(indentationLevel + 4));

            }

            System.out.println("}".indent(indentationLevel));

        } else if (code instanceof Java.FieldDeclaration fieldDeclaration) {
            System.out.print((fieldDeclaration + ";").indent(indentationLevel));

        } else if (code instanceof Java.AlternateConstructorInvocation alternateConstructorInvocation) {
            String constructorInvocationLine = "this(";

            Java.Rvalue[] args = alternateConstructorInvocation.arguments;
            for (int i = 0; i < args.length; i++) {
                if (i == args.length - 1)
                    constructorInvocationLine += args[i] + ");";
                else
                    constructorInvocationLine += args[i] + ", ";
            }

            System.out.print(constructorInvocationLine.indent(indentationLevel));

        } else if (code instanceof Java.ExpressionStatement eStm) {
            System.out.print((printRvalue(eStm.rvalue) + ";").indent(indentationLevel));

        } else if (code instanceof Java.LocalVariableDeclarationStatement lvds) {
            Java.VariableDeclarator vd = lvds.variableDeclarators[0];
            String varDeclLine = lvds.type.toString() + " " + vd.name;
            if (vd.initializer == null)
                varDeclLine += ";";
            else
                varDeclLine += " = " + printInitializerOrRvalue(vd.initializer) + ";";
            System.out.print(varDeclLine.indent(indentationLevel));

        } else {
            System.out.print(code.toString().indent(indentationLevel));
        }
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     * @param indentationLevel The number of spaces to add before each line of the code.
     */
    private static void printCode(Java.MemberTypeDeclaration code, int indentationLevel) {
        if (code instanceof Java.MemberClassDeclaration mcd) {
            String classDeclarationLine = "";
            for (Java.Modifier modifier : mcd.getModifiers())
                classDeclarationLine += modifier.toString() + " ";
            classDeclarationLine += "class " + mcd.getName() + " {";
            System.out.print(classDeclarationLine.indent(indentationLevel));

            for (Java.MemberTypeDeclaration mtd : mcd.getMemberTypeDeclarations()) {
                printCode(mtd, indentationLevel + 4);
            }

            printCode(mcd.fieldDeclarationsAndInitializers, indentationLevel + 4);
            System.out.println();

            for (Java.ConstructorDeclarator constructorDeclarator : mcd.constructors) {
                String constructorDeclarationLine = constructorDeclarator.getModifiers()[0].toString();
                constructorDeclarationLine += " " + constructorDeclarator.getDeclaringType().toString() + "(";

                Java.FunctionDeclarator.FormalParameter[] parameters = constructorDeclarator.formalParameters.parameters;
                for (int i = 0; i < parameters.length; i++) {
                    constructorDeclarationLine += parameters[i].toString();

                    if (i != parameters.length - 1)
                        constructorDeclarationLine += ", ";
                }

                constructorDeclarationLine += ") {";
                System.out.print(constructorDeclarationLine.indent(indentationLevel + 4));
                if (constructorDeclarator.constructorInvocation != null)
                    printCode(constructorDeclarator.constructorInvocation, indentationLevel + 8);

                printCode(constructorDeclarator.statements, indentationLevel + 8);
                System.out.print("}".indent(indentationLevel + 4));
            }
            System.out.println();

            for (Java.MethodDeclarator methodDeclarator : mcd.getMethodDeclarations()) {
                String methodHeader = methodDeclarator.modifiers[0].toString() + " ";
                methodHeader += methodDeclarator.type.toString() + " ";
                methodHeader += methodDeclarator + " {";
                System.out.print(methodHeader.indent(indentationLevel + 4));

                printCode(methodDeclarator.statements, indentationLevel + 8);

                System.out.print("}".indent(indentationLevel + 4));

            }

            System.out.println("}".indent(indentationLevel));

        } else {
            System.out.println(code.toString().indent(indentationLevel));
        }
    }

    /**
     * Method to properly print a {@link Java.ArrayInitializerOrRvalue} in a human-readable format.
     * @param value The {@link Java.ArrayInitializerOrRvalue} to print.
     * @return The {@link String} representing {@code value}.
     */
    private static String printInitializerOrRvalue(Java.ArrayInitializerOrRvalue value) {
        if (value instanceof Java.Rvalue rvalue)
            return printRvalue(rvalue);
        else
            throw new UnsupportedOperationException("printInitializerOrRvalue does not support this value type");
    }

    /**
     * Method to properly print {@link Java.Rvalue}s in a human-readable format.
     * @param rValue The {@link Java.Rvalue} to convert to a string.
     * @return The {@link String} representing {@code rValue}.
     */
    private static String printRvalue(Java.Rvalue rValue) {
        if (rValue instanceof Java.UnaryOperation uo) {
            return uo.operator + "(" + printRvalue(uo.operand) + ")";
        } else if (rValue instanceof Java.BinaryOperation bo) {
            return "(" + printRvalue(bo.lhs) + " " + bo.operator + " " + printRvalue(bo.rhs) + ")";
        } else if (rValue instanceof Java.Assignment ass) {
            return printRvalue(ass.lhs) + " " + ass.operator + " " + printRvalue(ass.rhs);
        } else if (rValue instanceof Java.NewArray newArray) {
            String newExpr = "new " + newArray.type;
            for (Java.Rvalue dim : newArray.dimExprs) {
                newExpr += "[" + printRvalue(dim) + "]";
            }
            for (int i = 0; i < newArray.dims; i++)
                newExpr += "[]";
            return newExpr;
        } else if (rValue instanceof Java.Cast cast) {
            return "((" + cast.targetType.toString() + ") " + printRvalue(cast.value) + ')';
        } else if (rValue instanceof Java.ConditionalExpression cExp) {
            return "(" + cExp.lhs + ") ? " + cExp.mhs + " : " + cExp.rhs;
        } else if (rValue instanceof Java.MethodInvocation mi) {
            String miLine = mi.target + "." + mi.methodName + "(";
            for (int argumentIndex = 0; argumentIndex < mi.arguments.length; argumentIndex++) {
                miLine += printRvalue(mi.arguments[argumentIndex]);
                if (argumentIndex != mi.arguments.length - 1)
                    miLine += ", ";
            }
            miLine += ")";
            return miLine;
        } else if (rValue instanceof Java.NewInitializedArray nia) {
            String niaLine = "new " + nia.arrayType.toString() + " { ";
            for (int i = 0; i < nia.arrayInitializer.values.length; i++) {
                niaLine += printInitializerOrRvalue(nia.arrayInitializer.values[i]);
                if (i != nia.arrayInitializer.values.length - 1)
                    niaLine += ", ";
            }
            niaLine += " }";
            return niaLine;
        } else if (rValue instanceof Java.SimpleConstant sc) {
            String defaultString = sc.toString();
            return defaultString.substring(1, defaultString.length() - 1);
        } else {
            return rValue.toString();
        }
    }

}
