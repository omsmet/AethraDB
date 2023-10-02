package AethraDB.util.language.function.logic;

import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraFunction;

/**
 * {@link AethraFunction} definition for representing an CASE function.
 * Currently only supports the "if-then-else" pattern.
 */
public class AethraCaseFunction extends AethraFunction {

    /**
     * The if condition of the case statement.
     */
    public final AethraFunction ifExpression;

    /**
     * The value represented by {@code this} if the condition holds.
     */
    public final AethraExpression trueValue;

    /**
     * The value represented by {@code this} if the condition does not hold.
     */
    public final AethraExpression falseValue;

    /**
     * Creates a new {@link AethraCaseFunction} instance implementing the if-then-else pattern.
     * @param ifExpression The if condition of the case statement.
     * @param trueValue The value represented by {@code this} if the condition holds.
     * @param falseValue The value represented by {@code this} if the condition does not hold.
     */
    public AethraCaseFunction(AethraFunction ifExpression, AethraExpression trueValue, AethraExpression falseValue) {
        this.ifExpression = ifExpression;
        this.trueValue = trueValue;
        this.falseValue = falseValue;
    }

    @Override
    public Kind getKind() {
        return Kind.CASE;
    }

}
