package AethraDB.util.language.function.logic;

import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraFunction;

/**
 * {@link AethraFunction} definition for representing an AND function.
 */
public class AethraAndFunction extends AethraFunction {

    /**
     * The operands to perform the AND function over.
     */
    public final AethraExpression[] operands;

    /**
     * Creates a new {@link AethraAndFunction} instance.
     * @param operands The operands to perform the AND function over.
     */
    public AethraAndFunction(final AethraExpression[] operands) {
        this.operands = operands;
    }

    @Override
    public Kind getKind() {
        return Kind.AND;
    }

}
