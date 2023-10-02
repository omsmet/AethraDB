package AethraDB.util.language.function.aggregation;

import AethraDB.util.language.function.AethraFunction;
import AethraDB.util.language.value.AethraInputRef;

/**
 * {@link AethraFunction} definition for representing a SUM aggregation function.
 */
public class AethraSumAggregation extends AethraFunction {

    /**
     * The input ordinal that the summation should be performed over.
     */
    public final AethraInputRef summand;

    /**
     * Creates a new {@link AethraSumAggregation} instance.
     * @param summand The input ordinal to perform the summation over.
     */
    public AethraSumAggregation(AethraInputRef summand) {
        this.summand = summand;
    }

    @Override
    public Kind getKind() {
        return Kind.SUM;
    }

}
