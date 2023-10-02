package AethraDB.util.language.value.literal;

/**
 * {@link AethraLiteral} for representing a date interval
 */
public class AethraDateIntervalLiteral extends AethraLiteral {

    /**
     * The quantifier of the interval represented by {@code this}.
     */
    public final int quantifier;

    /**
     * The units that a date interval can extend over.
     */
    public enum Unit {
        DAY,
        WEEK,
        MONTH,
        YEAR
    }

    /**
     * The unit of the interval represented by {@code this}.
     */
    public final Unit unit;

    /**
     * Creates a new {@link AethraDateIntervalLiteral} instance.
     * @param quantifier The quantifier of the new {@link AethraDateIntervalLiteral}.
     * @param unit The unit of the new {@link AethraDateIntervalLiteral}.
     */
    public AethraDateIntervalLiteral(int quantifier, String unit) {
        this.quantifier = quantifier;

        if (unit.equals("DAY"))
            this.unit = Unit.DAY;
        else if (unit.equals("WEEK"))
            this.unit = Unit.WEEK;
        else if (unit.equals("MONTH"))
            this.unit = Unit.MONTH;
        else if (unit.equals("YEAR"))
            this.unit = Unit.YEAR;
        else
            throw new IllegalArgumentException("Cannot determine the unit for '" + unit + "'");

    }

}
