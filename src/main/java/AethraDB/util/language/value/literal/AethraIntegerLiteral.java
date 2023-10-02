package AethraDB.util.language.value.literal;

import AethraDB.util.language.value.AethraValue;

/**
 * {@link AethraValue} for representing an integer literal value.
 */
public class AethraIntegerLiteral extends AethraLiteral {

    /**
     * The integer value represented by {@code this}.
     */
    public final int value;

    /**
     * Creates a new {@link AethraIntegerLiteral} for a specific integer value.
     * @param value The value to represent.
     */
    public AethraIntegerLiteral(String value) {
        this.value = Integer.parseInt(value);
    }

}
