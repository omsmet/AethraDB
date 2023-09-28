package AethraDB.util.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.util.ArrayList;

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
     * @param rootAllocator The {@link RootAllocator} to use for creating the schema.
     * @return The schema representing the Arrow database in {@code databaseDirectoryPath}.
     */
    public static CalciteSchema fromDirectory(String databaseDirectoryPath, RelDataTypeFactory typeFactory, RootAllocator rootAllocator) {
        File databaseDirectory = new File(databaseDirectoryPath);

        if (!databaseDirectory.exists() || !databaseDirectory.isDirectory())
            throw new IllegalStateException("Cannot create a schema for a non-existent database directory");

        return fromDirectory(databaseDirectory, typeFactory, rootAllocator);
    }

    /**
     * Create a schema for an Arrow database directory.
     * @param databaseDirectory The directory to create the database schema from.
     * @param typeFactory The {@link RelDataTypeFactory} to use for creating the schema.
     * @param rootAllocator The {@link RootAllocator} to use for creating the schema.
     * @return The schema representing the Arrow database in {@code databaseDirectory}.
     */
    public static CalciteSchema fromDirectory(File databaseDirectory, RelDataTypeFactory typeFactory, RootAllocator rootAllocator) {
        // Create the root schema and type factory for the schema
        CalciteSchema databaseSchema = CalciteSchema.createRootSchema(false);

        // Find all the arrow files in the directory
        File[] arrowTableFiles = databaseDirectory.listFiles((dir, name) -> name.endsWith(".arrow"));
        if (arrowTableFiles == null || arrowTableFiles.length == 0)
            throw new IllegalStateException("Cannot create a schema for an empty database");

        // Add each arrow table to the schema
        for (File arrowTableFile : arrowTableFiles) {
            ArrowTable arrowTableInstance = createTableForArrowFile(arrowTableFile, typeFactory, rootAllocator);
            databaseSchema.add(arrowTableInstance.getName(), arrowTableInstance);
        }

        // Return the final schema
        return databaseSchema;
    }

    /**
     * Create a {@link ArrowTable} instance representing the schema of a specific Arrow table.
     * @param arrowTable The {@link File} containing the Arrow table.
     * @param typeFactory The {@link RelDataTypeFactory} to use for creating the schema.
     * @param rootAllocator The {@link RootAllocator} to use for creating the schema.
     * @return The type representing the Arrow table.
     */
    private static ArrowTable createTableForArrowFile(File arrowTable, RelDataTypeFactory typeFactory, RootAllocator rootAllocator) {
        // Get the arrow schema from the file
        ArrayList<Field> arrowSchemaFields;
        try {
            arrowSchemaFields = ArrowFileSchemaExtractor.getFieldDescriptionFromTableFile(arrowTable);
        } catch (Exception e) {
            throw new RuntimeException("Could not parse the arrow file schema for file '" + arrowTable.getPath() + "'", e);
        }

        // Create a builder for the calcite type
        RelDataTypeFactory.Builder builderForTable = typeFactory.builder();

        // Add each column to the calcite type
        for (Field column : arrowSchemaFields) {
            RelDataType columnType = typeFactory.createTypeWithNullability(arrowToSqlType(column.getType(), typeFactory), false);
            builderForTable.add(column.getName(), columnType);
        }

        // Obtain the calcite type
        RelDataType tableType = builderForTable.build();

        // Construct the table instance
        return new ArrowTable(arrowTable, tableType);
    }

    /**
     * Method for converting an {@link ArrowType} into a {@link RelDataType}.
     * @param arrowType The {@link ArrowType} to convert.
     * @return The {@link RelDataType} corresponding to {@code arrowType}.
     */
    private static RelDataType arrowToSqlType(ArrowType arrowType, RelDataTypeFactory typeFactory) {
        if (arrowType instanceof ArrowType.Int)
            return typeFactory.createSqlType(SqlTypeName.INTEGER);

        else if (arrowType instanceof ArrowType.FixedSizeBinary arrowFixedSizeBinaryType)
            return typeFactory.createSqlType(SqlTypeName.CHAR, arrowFixedSizeBinaryType.getByteWidth());

        else if (arrowType instanceof ArrowType.Utf8)
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);

        else if (arrowType instanceof ArrowType.LargeUtf8)
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);

        else if (arrowType instanceof ArrowType.Decimal arrowDecimalType)
            return typeFactory.createSqlType(SqlTypeName.DECIMAL, arrowDecimalType.getPrecision(), arrowDecimalType.getScale());

        else if (arrowType instanceof ArrowType.Date)
            return typeFactory.createSqlType(SqlTypeName.DATE);

        else if (arrowType instanceof ArrowType.FloatingPoint fpat && fpat.getPrecision() == FloatingPointPrecision.DOUBLE)
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);

        else
            throw new IllegalArgumentException("The provided ArrowType is currently not supported: " + arrowType.toString());
    }
}
