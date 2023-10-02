package AethraDB.util.language.function;

import AethraDB.util.language.AethraExpression;

/**
 * Generic super-class for representing {@link AethraExpression}s which represent a function.
 * Examples include aggregation functions, arithmetic expressions, conditions etc.
 */
public abstract class AethraFunction extends AethraExpression {

    /**
     * Enumeration of the functions expressible in AethraDB.
     */
    public enum Kind {

        AND,
        CASE,

        EQ,
        GT,
        GTE,
        LT,
        LTE,

        ADD,
        DIVIDE,
        MULTIPLY,
        SUBTRACT,

        COUNT,
        SUM
    }


    /**
     * Obtain the kind of computation expressed by an {@link AethraFunction}.
     * @return The kind of computation expressed by an {@link AethraFunction}.
     */
    public abstract Kind getKind();

}
