package AethraDB.util.language.value;

/**
 * {@link AethraValue} for referring to an input column.
 */
public final class AethraInputRef extends AethraValue {

    /**
     * The index of the column being referred to by {@code this}.
     */
    public final int columnIndex;

    /**
     * Constructs a new {@link AethraInputRef} instance.
     * @param columnIndex The column to be referred to by the newly created instance.
     */
    public AethraInputRef(int columnIndex) {
        this.columnIndex = columnIndex;
    }

}
