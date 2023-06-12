package evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
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
     * Creates a new {@link ArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public CachingArrowTableReader(File arrowFile, RootAllocator rootAllocator) throws Exception {
        super(arrowFile, rootAllocator);
        this.reset();
    }

    @Override
    public void reset() throws Exception {
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
                } else {
                    throw new UnsupportedOperationException("CachingArrowTableReader.reset could not cache the current field vector type");
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
