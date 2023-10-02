package AethraDB.util.language.function;

import AethraDB.util.language.AethraExpression;

/**
 * {@link AethraFunction} definition for representing functions over two operands.
 */
public class AethraBinaryFunction extends AethraFunction {

    /**
     * The function which is expressed;
     */
    private final Kind kind;

    /**
     * The first operand over which the function is performed.
     */
    public final AethraExpression firstOperand;

    /**
     * The second operand over which the function is performed.
     */
    public final AethraExpression secondOperand;

    /**
     * Creates a new {@link AethraBinaryFunction} instance.
     * @param kind The function represented by {@code this}.
     * @param firstOperand The first operand over which the function is performed.
     * @param secondOperand The second operand over which the function is performed.
     */
    public AethraBinaryFunction(Kind kind, AethraExpression firstOperand, AethraExpression secondOperand) {
        this.kind = kind;
        this.firstOperand = firstOperand;
        this.secondOperand = secondOperand;
    }

    @Override
    public Kind getKind() {
        return this.kind;
    }

}
