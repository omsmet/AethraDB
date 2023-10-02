package AethraDB.evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.AethraArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * An {@link ArrowTableReader} specialisation which simply maps the methods of {@link ArrowTableReader}
 * directly onto an {@link ArrowReader} instance without performing any buffering or other optimisations.
 */
public class DirectArrowTableReader extends ArrowTableReader {

    /**
     * The {@link FileInputStream} used for reading the Arrow file.
     */
    private FileInputStream arrowFileStream;

    /**
     * The {@link ArrowReader} used for reading the Arrow file.
     */
    private ArrowReader arrowReader;

    /**
     * The {@link VectorSchemaRoot} used for reading the Arrow file.
     */
    private VectorSchemaRoot vectorSchemaRoot;

    /**
     * Creates a new {@link ArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @param useProjectingArrowReader Whether this {@link ArrowTableReader} should use the
     * {@link AethraArrowFileReader} implementation.
     * @param columnsToProject The columns of the {@code arrowFile} to actually project out.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public DirectArrowTableReader(File arrowFile, RootAllocator rootAllocator, boolean useProjectingArrowReader, int[] columnsToProject) throws Exception {
        super(arrowFile, rootAllocator, useProjectingArrowReader, columnsToProject);
        this.reset();
    }

    @Override
    public void reset() throws Exception {
        if (this.arrowReader != null)
            this.specificClose();

        this.arrowFileStream = new FileInputStream(this.arrowFile);
        if (this.useProjectingArrowReader)
            this.arrowReader = new AethraArrowFileReader(this.arrowFileStream.getChannel(), this.tableAllocator, this.columnsToProject);
        else
            this.arrowReader = new ArrowFileReader(this.arrowFileStream.getChannel(), this.tableAllocator);
        this.vectorSchemaRoot = this.arrowReader.getVectorSchemaRoot();
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return this.arrowReader.loadNextBatch();
    }

    @Override
    public FieldVector getVector(int index) {
        return this.vectorSchemaRoot.getVector(index);
    }

    @Override
    protected void specificClose() throws IOException {
        this.vectorSchemaRoot.close();
        this.arrowReader.close();
        this.arrowFileStream.close();
    }
}
