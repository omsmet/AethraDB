package AethraDB.util.language.value.literal;

import AethraDB.util.language.value.AethraValue;

/**
 * {@link AethraValue} for representing an double literal value.
 */
public class AethraDoubleLiteral extends AethraLiteral {

    /**
     * The double value represented by {@code this}.
     */
    public final double value;

    /**
     * Creates a new {@link AethraDoubleLiteral} for a specific double value.
     * @param value The value to represent.
     */
    public AethraDoubleLiteral(String value) {
        this.value = Double.parseDouble(value);
    }

    /**
     * Creates a new {@link AethraDoubleLiteral} for a specific double value.
     * @param value The value to represent.
     */
    public AethraDoubleLiteral(double value) {
        this.value = value;
    }

}
