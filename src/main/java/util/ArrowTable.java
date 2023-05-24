package util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.io.File;

/**
 * Class representing the Arrow table contained in a specific file.
 */
public class ArrowTable extends AbstractTable {

    /**
     * The Arrow file represented by {@code this}.
     */
    private final File arrowFile;

    /**
     * The data type of each "row" in the Arrow file represented by {@link this}.
     */
    private final RelDataType rowDataType;

    /**
     * Constructs an {@link ArrowTable} for a specific file with a given schema.
     * @param arrowFile The file to create the instance for.
     * @param rowDataType The proposed schema of the table.
     */
    public ArrowTable(File arrowFile, RelDataType rowDataType) {
        this.arrowFile = arrowFile;
        this.rowDataType = rowDataType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return this.rowDataType;
    }
}
