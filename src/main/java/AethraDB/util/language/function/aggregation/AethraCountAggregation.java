package AethraDB.util.language.function.aggregation;

import AethraDB.util.language.function.AethraFunction;

/**
 * {@link AethraFunction} definition for representing a COUNT aggregation function.
 */
public class AethraCountAggregation extends AethraFunction {

    /**
     * Creates a new {@link AethraCountAggregation} instance.
     */
    public AethraCountAggregation() {

    }

    @Override
    public Kind getKind() {
        return Kind.COUNT;
    }

}
