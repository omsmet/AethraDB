package evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * An {@link ArrowTableReader} specialisation which preloads all data in the arrow file and then
 * maps the methods of {@link ArrowTableReader} onto the cached data.
 */
public class CachingArrowTableReader extends ArrowTableReader {

    /**
     * The number of vectors in the current Arrow file.
     */
    private int numberOfVectors;

    /**
     * The number of columns in the current Arrow file.
     */
    private int columnCount;

    /**
     * The index of the vector that is currently exposed by the {@link ArrowTableReader}.
     */
    private int currentVectorIndex;

    /**
     * The actual field vectors that have been cached.
     */
    private FieldVector[][] fieldVectors;

    /**
     * Boolean keeping track of whether the {@code fieldVectors} variable has been initialised already.
     */
    private boolean fieldVectorsInitialised;

    /**
     * Creates a new {@link ArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public CachingArrowTableReader(File arrowFile, RootAllocator rootAllocator) throws Exception {
        super(arrowFile, rootAllocator);
        this.fieldVectorsInitialised = false;
        this.reset();
        this.fieldVectorsInitialised = true;
    }

    @Override
    public void reset() throws Exception {
        // Deallocate previous vectors if necessary
        if (fieldVectorsInitialised) {
            for (int i = 0; i < this.numberOfVectors; i++) {
                for (int j = 0; j < this.columnCount; j++) {
                    this.fieldVectors[i][j].close();
                }
            }
        }

        // Initialise the reader
        FileInputStream tableInputStream = new FileInputStream(this.arrowFile);
        ArrowFileReader tableFileReader =
                new ArrowFileReader(tableInputStream.getChannel(), this.tableAllocator);
        VectorSchemaRoot schemaRoot = tableFileReader.getVectorSchemaRoot();

        // Compute the number of vectors and columns
        this.numberOfVectors = tableFileReader.getRecordBlocks().size();
        this.columnCount = schemaRoot.getFieldVectors().size();
        this.fieldVectors = new FieldVector[this.numberOfVectors][columnCount];

        // Read and cache the data
        int cvi = 0;
        while (tableFileReader.loadNextBatch()) {
            for (int i = 0; i < columnCount; i++) {
                FieldVector fv_cvi_i = schemaRoot.getVector(i);
                if (fv_cvi_i instanceof IntVector int_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new IntVector(int_fv_cvi_i.getField(), this.tableAllocator);
                    int_fv_cvi_i.transferTo((IntVector) this.fieldVectors[cvi][i]);

                } else if (fv_cvi_i instanceof Float8Vector f8_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new Float8Vector(f8_fv_cvi_i.getField(), this.tableAllocator);
                    f8_fv_cvi_i.transferTo((Float8Vector) this.fieldVectors[cvi][i]);

                } else if (fv_cvi_i instanceof LargeVarCharVector lvc_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new LargeVarCharVector(lvc_fv_cvi_i.getField(), this.tableAllocator);
                    lvc_fv_cvi_i.transferTo((LargeVarCharVector) this.fieldVectors[cvi][i]);

                } else if (fv_cvi_i instanceof DateDayVector dd_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new DateDayVector(dd_fv_cvi_i.getField(), this.tableAllocator);
                    dd_fv_cvi_i.transferTo((DateDayVector) this.fieldVectors[cvi][i]);

                } else if (fv_cvi_i instanceof DecimalVector dd_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new DecimalVector(dd_fv_cvi_i.getField(), this.tableAllocator);
                    dd_fv_cvi_i.transferTo((DecimalVector) this.fieldVectors[cvi][i]);

                } else {
                    throw new UnsupportedOperationException(
                            "CachingArrowTableReader.reset could not cache the current field vector type: " + fv_cvi_i.getClass());
                }
            }
            cvi++;
        }

        // Set the correct state
        this.currentVectorIndex = -1;

        // Clean up
        schemaRoot.close();
        tableFileReader.close();
        tableInputStream.close();
    }

    @Override
    public boolean loadNextBatch() {
        this.currentVectorIndex++;
        return this.currentVectorIndex < this.numberOfVectors;
    }

    @Override
    public FieldVector getVector(int index) {
        return this.fieldVectors[this.currentVectorIndex][index];
    }


    @Override
    protected void specificClose() {
        for (int i = 0; i < this.numberOfVectors; i++) {
            for (int j = 0; j < this.columnCount; j++) {
                this.fieldVectors[i][j].close();
            }
        }
    }
}
