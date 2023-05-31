package util.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class containing functionality for building a {@link CalciteSchema} for a database that is
 * represented by a directory containing several Apache Arrow IPC files. Each file in the directory
 * will become a table in the resulting schema.
 */
public class ArrowSchemaBuilder {

    /**
     * Create a schema for an Arrow database directory.
     * @param databaseDirectoryPath The directory to create the database schema from.
     * @param typeFactory The {@link RelDataTypeFactory} to use for creating the schema.
     * @return The schema representing the Arrow database in {@code databaseDirectoryPath}.
     */
    public static CalciteSchema fromDirectory(String databaseDirectoryPath, RelDataTypeFactory typeFactory) {
        File databaseDirectory = new File(databaseDirectoryPath);

        if (!databaseDirectory.exists() || !databaseDirectory.isDirectory())
            throw new IllegalStateException("Cannot create a schema for a non-existent database directory");

        return fromDirectory(databaseDirectory, typeFactory);
    }

    /**
     * Create a schema for an Arrow database directory.
     * @param databaseDirectory The directory to create the database schema from.
     * @param typeFactory The {@link RelDataTypeFactory} to use for creating the schema.
     * @return The schema representing the Arrow database in {@code databaseDirectory}.
     */
    public static CalciteSchema fromDirectory(File databaseDirectory, RelDataTypeFactory typeFactory) {
        // Create the root schema and type factory for the schema
        CalciteSchema databaseSchema = CalciteSchema.createRootSchema(false);

        // Find all the arrow files in the directory
        File[] arrowTableFiles = databaseDirectory.listFiles((dir, name) -> name.endsWith(".arrow"));
        if (arrowTableFiles == null || arrowTableFiles.length == 0)
            throw new IllegalStateException("Cannot create a schema for an empty database");

        // Add each arrow table to the schema
        for (File arrowTableFile : arrowTableFiles) {
            ArrowTable arrowTableInstance = createTableForArrowFile(arrowTableFile, typeFactory);
            databaseSchema.add(arrowTableInstance.getName(), arrowTableInstance);
        }

        // Return the final schema
        return databaseSchema;
    }

    /**
     * Create a {@link ArrowTable} instance representing the schema of a specific Arrow table.
     * @param arrowTable The {@link File} containing the Arrow table.
     * @param typeFactory The {@link RelDataTypeFactory} to use for creating the schema.
     * @return The type representing the Arrow table.
     */
    private static ArrowTable createTableForArrowFile(File arrowTable, RelDataTypeFactory typeFactory) {
        // Convert the arrow schema into the required RelDataType
        try (   // Initialise the objects needed to read the Arrow schema
                var rootAllocator = new RootAllocator();
                var arrowTableInputStream = new FileInputStream(arrowTable);
                var arrowTableFileReader = new ArrowFileReader(arrowTableInputStream.getChannel(), rootAllocator);
        ) {
            // Obtain the arrow schema
            VectorSchemaRoot arrowSchemaRoot = arrowTableFileReader.getVectorSchemaRoot();
            Schema arrowSchema = arrowSchemaRoot.getSchema();

            // Instantiate the statistics
            long arrowVectorCount = arrowTableFileReader.getRecordBlocks().size();
            long vectorRowCount = arrowTableFileReader.loadNextBatch() ? arrowTableFileReader.getVectorSchemaRoot().getRowCount() : 0;
            long approximateTableRowCount = arrowVectorCount * vectorRowCount;
            ArrowTableStatistics tableStatistics = new ArrowTableStatistics(approximateTableRowCount);

            // Create a builder for the calcite type
            RelDataTypeFactory.Builder builderForTable = typeFactory.builder();

            // Add each column to the calcite type
            for (Field column : arrowSchema.getFields()) {
                RelDataType columnType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(arrowToSqlType(column.getType())), false);
                builderForTable.add(column.getName(), columnType);
            }

            // Obtain the calcite type
            RelDataType tableType = builderForTable.build();

            // Construct the table instance
            return new ArrowTable(arrowTable, tableType, tableStatistics);

        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Cannot create a table schema from a non-existent table file: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException("I/O exception occurred while creating a schema for table file '" + arrowTable.getPath() + "': " + e.getMessage());
        }
    }

    /**
     * Method for converting an {@link ArrowType} into a {@link SqlTypeName}.
     * @param arrowType The {@link ArrowType} to convert.
     * @return The {@link SqlTypeName} corresponding to {@code arrowType}.
     */
    private static SqlTypeName arrowToSqlType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Int)
            return SqlTypeName.INTEGER;
        else if (arrowType instanceof ArrowType.LargeUtf8)
            return SqlTypeName.VARCHAR;
        else if (arrowType instanceof ArrowType.Decimal)
            return SqlTypeName.DECIMAL;
        else if (arrowType instanceof ArrowType.Date)
            return SqlTypeName.DATE;
        else
            throw new IllegalArgumentException("The provided ArrowType is currently not supported: " + arrowType.toString());
    }
}
