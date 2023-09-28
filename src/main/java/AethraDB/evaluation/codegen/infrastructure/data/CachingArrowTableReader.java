package AethraDB.evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.AethraArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.calcite.util.ImmutableIntList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

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
     * Creates a new {@link CachingArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @param useProjectingArrowReader Whether this {@link ArrowTableReader} should use the
     * {@link AethraArrowFileReader} implementation.
     * @param columnsToProject The columns of the {@code arrowFile} to actually project out.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public CachingArrowTableReader(File arrowFile, RootAllocator rootAllocator, boolean useProjectingArrowReader, ImmutableIntList columnsToProject) throws Exception {
        super(arrowFile, rootAllocator, useProjectingArrowReader, columnsToProject);
        this.initialise();
    }

    /**
     * Method which initialises the cached vectors in this {@link CachingArrowTableReader}.
     * @throws IOException If an {@link IOException} is thrown from an Arrow internal method.
     */
    public void initialise() throws IOException {
        // Initialise the reader
        FileInputStream tableInputStream = new FileInputStream(this.arrowFile);
        ArrowReader tableFileReader;
        if (useProjectingArrowReader) {
            tableFileReader = new AethraArrowFileReader(tableInputStream.getChannel(), this.tableAllocator, this.columnsToProject);
            this.numberOfVectors = ((AethraArrowFileReader) tableFileReader).getRecordBlocks().size();
        } else {
            tableFileReader = new ArrowFileReader(tableInputStream.getChannel(), this.tableAllocator);
            this.numberOfVectors = ((ArrowFileReader) tableFileReader).getRecordBlocks().size();
        }

        VectorSchemaRoot schemaRoot = tableFileReader.getVectorSchemaRoot();
        this.columnCount = schemaRoot.getFieldVectors().size();
        this.fieldVectors = new FieldVector[this.numberOfVectors][columnCount];

        // Read and cache the data
        int cvi = 0;
        while (tableFileReader.loadNextBatch()) {
            // Cache columns as indicated by the columns to project
            for (int i : this.columnsToProject) {
                FieldVector fv_cvi_i = schemaRoot.getVector(i);
                if (fv_cvi_i instanceof IntVector int_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new IntVector(int_fv_cvi_i.getField(), this.tableAllocator);
                    int_fv_cvi_i.transferTo((IntVector) this.fieldVectors[cvi][i]);

                } else if (fv_cvi_i instanceof FixedSizeBinaryVector fsbv_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new FixedSizeBinaryVector(fsbv_fv_cvi_i.getField(), this.tableAllocator);
                    fsbv_fv_cvi_i.transferTo((FixedSizeBinaryVector) this.fieldVectors[cvi][i]);

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

                } else if (fv_cvi_i instanceof VarCharVector vc_fv_cvi_i) {
                    this.fieldVectors[cvi][i] = new VarCharVector(vc_fv_cvi_i.getField(), this.tableAllocator);
                    vc_fv_cvi_i.transferTo((VarCharVector) this.fieldVectors[cvi][i]);

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
    public void reset() {
        // Set the correct state
        this.currentVectorIndex = -1;
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
            for (int j : this.columnsToProject) {
                this.fieldVectors[i][j].close();
            }
        }
    }
}
