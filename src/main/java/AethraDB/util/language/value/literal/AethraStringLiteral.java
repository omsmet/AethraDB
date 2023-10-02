package AethraDB.util.language.value.literal;

import AethraDB.util.language.value.AethraValue;

import java.nio.charset.StandardCharsets;

/**
 * {@link AethraValue} for representing a String literal value.
 */
public class AethraStringLiteral extends AethraLiteral {

    /**
     * The string value represented by {@code this}.
     */
    public final byte[] value;

    /**
     * Creates a new {@link AethraStringLiteral} for a specific String literal.
     * @param value The value to represent.
     */
    public AethraStringLiteral(String value) {
        this.value = value.getBytes(StandardCharsets.US_ASCII);
    }

}
