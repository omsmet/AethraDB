package AethraDB.util.arrow;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
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
     * The statistics for the table represented by {@link this}.
     */
    private final ArrowTableStatistics tableStatistics;

    /**
     * Constructs an {@link ArrowTable} for a specific file with a given schema.
     * @param arrowFile The file to create the instance for.
     * @param rowDataType The proposed schema of the table.
     * @param tableStatistics The statistics of the table.
     */
    public ArrowTable(File arrowFile, RelDataType rowDataType, ArrowTableStatistics tableStatistics) {
        this.arrowFile = arrowFile;
        this.rowDataType = rowDataType;
        this.tableStatistics = tableStatistics;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return this.rowDataType;
    }

    @Override
    public Statistic getStatistic() {
        return this.tableStatistics;
    }

    /**
     * Method to obtain the name of a table.
     * @return The name of the table represented by {@link this}.
     */
    public String getName() {
        return this.arrowFile.getName().replace(".arrow", "");
    }

    /**
     * Method to obtain the file backing the table.
     * @return The {@link File} backing the table.
     */
    public File getArrowFile() {
        return this.arrowFile;
    }
}
