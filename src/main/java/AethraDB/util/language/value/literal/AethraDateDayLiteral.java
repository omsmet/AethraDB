package AethraDB.util.language.value.literal;

import AethraDB.util.language.value.AethraValue;

import java.time.LocalDate;

/**
 * {@link AethraValue} for representing a date literal value.
 */
public class AethraDateDayLiteral extends AethraLiteral {

    /**
     * The unix day which represents the date literal encoded by {@code this}.
     */
    public final int unixDay;

    /**
     * Creates a new {@link AethraDateDayLiteral} for a specific date.
     * @param dateLiteral The date literal to represent.
     */
    public AethraDateDayLiteral(String dateLiteral) {
        this.unixDay = (int) LocalDate.parse(dateLiteral).toEpochDay();
    }

}
