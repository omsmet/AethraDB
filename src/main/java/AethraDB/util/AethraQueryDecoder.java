package AethraDB.util;

import AethraDB.evaluation.codegen.operators.AggregationOperator;
import AethraDB.evaluation.codegen.operators.ArrowTableScanOperator;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import AethraDB.evaluation.codegen.operators.FilterOperator;
import AethraDB.evaluation.codegen.operators.JoinOperator;
import AethraDB.evaluation.codegen.operators.ProjectOperator;
import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraBinaryFunction;
import AethraDB.util.language.function.AethraFunction;
import AethraDB.util.language.function.aggregation.AethraCountAggregation;
import AethraDB.util.language.function.aggregation.AethraSumAggregation;
import AethraDB.util.language.function.logic.AethraAndFunction;
import AethraDB.util.language.function.logic.AethraCaseFunction;
import AethraDB.util.language.value.AethraInputRef;
import AethraDB.util.language.value.literal.AethraDateDayLiteral;
import AethraDB.util.language.value.literal.AethraDateIntervalLiteral;
import AethraDB.util.language.value.literal.AethraDoubleLiteral;
import AethraDB.util.language.value.literal.AethraIntegerLiteral;
import AethraDB.util.language.value.literal.AethraStringLiteral;

import java.util.ArrayList;

/**
 * Class which decodes a query given in the Aethra Engine Plan Format into a tree of
 * {@link CodeGenOperator}s.
 */
public class AethraQueryDecoder {

    /**
     * Method to decode a query plan given in Aethra Engine Plan Format into a tree of
     * {@link CodeGenOperator}s.
     * @param databasePath The path of the folder containing the database.
     * @param queryPlan The plan to decode.
     * @return The root of the tree of {@link CodeGenOperator}s corresponding to the given plan.
     */
    public static CodeGenOperator decode(final String databasePath, final String queryPlan) {
        String[] queryPlanLines = queryPlan.split("\n");
        int topLevelOperatorIndex = queryPlanLines.length - 1;
        return decode(databasePath, queryPlanLines, topLevelOperatorIndex);
    }

    private static CodeGenOperator decode(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        char operatorDefinition = queryPlanLines[operatorToDecodeIndex].charAt(0);
        if (operatorDefinition == 'A')
            return decodeAggregation(databasePath, queryPlanLines, operatorToDecodeIndex);

        else if (operatorDefinition == 'F')
            return decodeFilter(databasePath, queryPlanLines, operatorToDecodeIndex);

        else if (operatorDefinition == 'J')
            return decodeJoin(databasePath, queryPlanLines, operatorToDecodeIndex);

        else if (operatorDefinition == 'P')
            return decodeProject(databasePath, queryPlanLines, operatorToDecodeIndex);

        else if (operatorDefinition == 'S')
            return decodeScan(databasePath, queryPlanLines, operatorToDecodeIndex);

        else
            throw new UnsupportedOperationException("AethraQueryDecoder.decode found unsupported operator definition");
    }

    private static CodeGenOperator decodeAggregation(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        String aggregationToDecode = queryPlanLines[operatorToDecodeIndex];

        // Parse the aggregation definition
        // Line form: A;{input node line index};{possible group-by column indides};{aggregation expressions separated by comma's}
        String[] aggregationDefinition = aggregationToDecode.split(";");
        int inputNodeIndex = Integer.parseInt(aggregationDefinition[1]);

        boolean isGroupByAggregate = !aggregationDefinition[2].isEmpty();
        int[] groupByColumns;
        if (isGroupByAggregate) {
            String[] groupByColumnIndicesRaw = aggregationDefinition[2].split(",");
            groupByColumns = new int[groupByColumnIndicesRaw.length];
            for (int i = 0; i < groupByColumns.length; i++)
                groupByColumns[i] = Integer.parseInt(groupByColumnIndicesRaw[i]);
        } else {
            groupByColumns = null;
        }

        // First decode the input
        CodeGenOperator inputNode = decode(databasePath, queryPlanLines, inputNodeIndex);

        // Then decode the aggregation expressions
        String[] rawAggregationExpressions = obtainTopLevelExpressions(aggregationDefinition[3]);
        AethraExpression[] aggregationExpressions = decodeExpressions(rawAggregationExpressions);

        return new AggregationOperator(inputNode, isGroupByAggregate, groupByColumns, aggregationExpressions);
    }

    private static CodeGenOperator decodeFilter(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        String filterToDecode = queryPlanLines[operatorToDecodeIndex];

        // Parse the filter definition
        // Line form: F;{input node line index};{condition}\n
        String[] filterDefinition = filterToDecode.split(";");
        int inputNodeIndex = Integer.parseInt(filterDefinition[1]);

        // First, decode the input node
        CodeGenOperator inputNode = decode(databasePath, queryPlanLines, inputNodeIndex);

        // Then decode the filter condition
        AethraExpression filterExpression = decodeExpression(filterDefinition[2]);

        return new FilterOperator(inputNode, filterExpression);
    }

    private static CodeGenOperator decodeJoin(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        String joinToDecode = queryPlanLines[operatorToDecodeIndex];

        // Parse the join definition
        // Line form: J;{left input node line index};{right input node line index};{left_column_eq_index};{right_column_eq_index}\n
        String[] joinDefinition = joinToDecode.split(";");
        int leftInputNodeIndex = Integer.parseInt(joinDefinition[1]);
        int rightInputNodeIndex = Integer.parseInt(joinDefinition[2]);
        int leftJoinColumnEqIndex = Integer.parseInt(joinDefinition[3]);
        int rightJoinColumnEqIndex = Integer.parseInt(joinDefinition[4]);

        // First, decode the left and right input nodes
        CodeGenOperator leftInputNode = decode(databasePath, queryPlanLines, leftInputNodeIndex);
        CodeGenOperator rightInputNode = decode(databasePath, queryPlanLines, rightInputNodeIndex);

        return new JoinOperator(leftInputNode, rightInputNode, leftJoinColumnEqIndex, rightJoinColumnEqIndex);
    }

    private static CodeGenOperator decodeProject(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        String projectionToDecode = queryPlanLines[operatorToDecodeIndex];

        // Parse the projection definition
        // Line form: P;{input node line index};[{projection expressions}]\n
        String[] projectionDefinition = projectionToDecode.split(";");
        int inputNodeIndex = Integer.parseInt(projectionDefinition[1]);

        String projectionExpressionContainer = projectionDefinition[2];
        String[] projectionExpressions =
                obtainTopLevelExpressions(projectionExpressionContainer.substring(1, projectionExpressionContainer.length() - 1));

        // Now, decode the input first
        CodeGenOperator inputNode = decode(databasePath, queryPlanLines, inputNodeIndex);

        // Then, decode the projection expressions
        AethraExpression[] parsedProjectionExpression = decodeExpressions(projectionExpressions);

        return new ProjectOperator(inputNode, parsedProjectionExpression);
    }

    private static CodeGenOperator decodeScan(final String databasePath, final String[] queryPlanLines, int operatorToDecodeIndex) {
        String scanToDecode = queryPlanLines[operatorToDecodeIndex];

        // Parse the scan definition
        // Line form: S;{table name};{boolean indicating if columns are projected};{projected column indices separated by commas}\n
        String[] scanDefinition = scanToDecode.split(";");
        boolean isProjecting = Boolean.parseBoolean(scanDefinition[2]);

        String[] rawProjectingColumns = scanDefinition[3].split(",");
        int[] projectingColumns = new int[rawProjectingColumns.length];
        for (int i = 0; i < projectingColumns.length; i++)
            projectingColumns[i] = Integer.parseInt(rawProjectingColumns[i]);

        return new ArrowTableScanOperator(databasePath, scanDefinition[1], isProjecting, projectingColumns);
    }

    private static AethraExpression[] decodeExpressions(String[] expressions) {
        AethraExpression[] result = new AethraExpression[expressions.length];
        for (int i = 0; i < result.length; i++)
            result[i] = decodeExpression(expressions[i]);
        return result;
    }

    private static AethraExpression decodeExpression(String expression) {
        String trimmedExpression = expression.trim();
        char firstChar = trimmedExpression.charAt(0);

        // Rule: if the expressions starts with a $-sign, followed by a letter, we know it's an aggregation function
        if (firstChar == '$' && Character.isLetter(trimmedExpression.charAt(1))) {
            return decodeAggregationFunction(trimmedExpression);
        }

        // Rule: otherwise, if the expressions starts with a $-sign, but is not followed by a letter, it's an input reference
        else if (trimmedExpression.charAt(0) == '$') {
            int referenceIndex = Integer.parseInt(trimmedExpression.substring(1));
            return new AethraInputRef(referenceIndex);
        }

        // Rule: otherwise, if the expression starts with a letter, and contains both the ( and ) character it is a regular function
        else if (Character.isLetter(firstChar) && expression.contains("(") && expression.contains(")")) {
            return decodeFunction(trimmedExpression);
        }

        // Rule: we hard-code comparison and arithmetic operators as regular functions
        else if (firstChar == '+'
                || firstChar == '/'
                || firstChar == '*'
                || firstChar == '-'
                || firstChar == '>'
                || firstChar == '<'
                || firstChar == '='
        ) {
            return decodeFunction(trimmedExpression);
        }

        // Rule, otherwise, if it starts with a number, a quote character or contains "null", it is a literal
        else if (Character.isDigit(firstChar) || firstChar == '\'' || expression.contains("null")) {
            return decodeLiteral(trimmedExpression);
        }

        else {
            throw new UnsupportedOperationException(
                    "AethraQueryDecoder.decodeExpression cannot identify the expression given by: " + trimmedExpression);
        }
    }

    private static AethraExpression decodeAggregationFunction(String function) {
        if (function.startsWith("$SUM0")) {
            // Extract the expression over which to sum
            int sumExpressionStart = function.indexOf('(') + 1;
            String rawSumExpression = function.substring(sumExpressionStart, function.length() - 1);
            AethraExpression sumExpression = decodeExpression(rawSumExpression);
            if (!(sumExpression instanceof AethraInputRef sumExpressionRef))
                throw new UnsupportedOperationException(
                        "AethraQueryDecoder.decodeAggregationFunction expects SUM aggregations to be carried out over a single column");

            return new AethraSumAggregation(sumExpressionRef);

        }

        else {
            throw new UnsupportedOperationException(
                    "AethraQueryDecoder.decodeFunction cannot identify the function given by: " + function);
        }
    }

    private static AethraExpression decodeFunction(String function) {
        // First convert the operands of the function
        int operandsStartIndex = function.indexOf("(") + 1;
        String operandsString = function.substring(operandsStartIndex, function.length() - 1);
        String[] operandStrings = obtainTopLevelExpressions(operandsString);
        AethraExpression[] convertedOperands = decodeExpressions(operandStrings);

        // Then return the correct function object
        if (function.equals("COUNT()")) {
            return new AethraCountAggregation();
        }

        else if (function.startsWith("AND")) {
            return new AethraAndFunction(convertedOperands);
        } else if (function.startsWith("CASE")) {
            if (convertedOperands.length != 3 || !(convertedOperands[0] instanceof AethraFunction ifCondition)) {
                throw new UnsupportedOperationException(
                        "AethraQueryDecoder.decodeFunction currently only supports the if-then-else pattern for the CASE function");
            }
            return new AethraCaseFunction(ifCondition, convertedOperands[1], convertedOperands[2]);

        } else if (function.startsWith("=")) {
            return new AethraBinaryFunction(AethraFunction.Kind.EQ, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith(">")) {
            return new AethraBinaryFunction(AethraFunction.Kind.GT, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith(">=")) {
            return new AethraBinaryFunction(AethraFunction.Kind.GTE, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith("<")) {
            return new AethraBinaryFunction(AethraFunction.Kind.LT, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith("<=")) {
            return new AethraBinaryFunction(AethraFunction.Kind.LTE, convertedOperands[0], convertedOperands[1]);

        } else if (function.startsWith("+")) {
            return new AethraBinaryFunction(AethraFunction.Kind.ADD, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith("/")) {
            return new AethraBinaryFunction(AethraFunction.Kind.DIVIDE, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith("*")) {
            return new AethraBinaryFunction(AethraFunction.Kind.MULTIPLY, convertedOperands[0], convertedOperands[1]);
        } else if (function.startsWith("-")) {
            return new AethraBinaryFunction(AethraFunction.Kind.SUBTRACT, convertedOperands[0], convertedOperands[1]);
        }


        else {
            throw new UnsupportedOperationException(
                    "AethraQueryDecoder.decodeFunction cannot identify the function given by: " + function);
        }
    }

    private static AethraExpression decodeLiteral(String literal) {

        if (Character.isDigit(literal.charAt(0)) && literal.length() == 10 && literal.charAt(4) == '-' && literal.charAt(7) == '-') {
            // This is a date day literal
            return new AethraDateDayLiteral(literal);

        } else if (Character.isDigit(literal.charAt(0)) && literal.contains(":INTERVAL")) {
            // This is a date interval literal
            String[] intervalSpecification = literal.split(":");
            int quantifier = Integer.parseInt(intervalSpecification[0]);
            String unit = intervalSpecification[1].split(" ")[1];
            return new AethraDateIntervalLiteral(quantifier, unit);

        } else if (Character.isDigit(literal.charAt(0)) && !literal.contains(":")) {
            // This is an integer literal
            return new AethraIntegerLiteral(literal);

        } else if (literal.charAt(0) == '\'') {
            // This is a string literal
            return new AethraStringLiteral(literal.substring(1, literal.length() - 1));

        } else if (Character.isDigit(literal.charAt(0)) && literal.contains(":DECIMAL")) {
            // This is a decimal literal, which we convert into a double literal
            int literalEnd = literal.indexOf(':');
            return new AethraDoubleLiteral(literal.substring(0, literalEnd));

        } else if (literal.contains("null")) {
            // Decode the null literal to the appropriate type
            if (literal.contains("DOUBLE"))
                return new AethraDoubleLiteral(Double.NaN);
            else
                throw new UnsupportedOperationException("AethraQueryDecoder.decodeLiteral cannot determine the type of the null literal: " + literal);

        }

        else {
            throw new UnsupportedOperationException(
                    "AethraQueryDecoder.decodeLiteral cannot identify the literal represented by: " + literal);
        }

    }

    private static String[] obtainTopLevelExpressions(String expressionString) {
        // Deal with settings where no expressions are provided
        if (expressionString.length() == 0)
            return new String[0];

        ArrayList<String> topLevelExpressions = new ArrayList<>();

        // Need to split the operands on comma's, but not inner comma's
        int startIndex = 0;
        int currentIndex = 0;
        int nestedLevel = 0;
        while (currentIndex < expressionString.length()) {
            if (expressionString.charAt(currentIndex) == ',' && nestedLevel == 0) {
                topLevelExpressions.add(expressionString.substring(startIndex, currentIndex));
                startIndex = currentIndex + 1;
                currentIndex = startIndex;
                continue;

            } else if (expressionString.charAt(currentIndex) == '(') {
                nestedLevel++;

            } else if (expressionString.charAt(currentIndex) == ')') {
                nestedLevel--;

            }
            currentIndex++;
        }

        // Don't forget to add the last operand
        topLevelExpressions.add(expressionString.substring(startIndex, currentIndex));

        return topLevelExpressions.toArray(new String[0]);
    }

}
