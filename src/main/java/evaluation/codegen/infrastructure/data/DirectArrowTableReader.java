package evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
     * Creates a new {@link ArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public DirectArrowTableReader(File arrowFile, RootAllocator rootAllocator) throws Exception {
        super(arrowFile, rootAllocator);
        this.reset();
    }

    @Override
    public void reset() throws Exception {
        if (this.arrowReader != null)
            this.specificClose();

        this.arrowFileStream = new FileInputStream(this.arrowFile);
        this.arrowReader = new ArrowFileReader(this.arrowFileStream.getChannel(), this.tableAllocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return this.arrowReader.loadNextBatch();
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
        return this.arrowReader.getVectorSchemaRoot();
    }

    @Override
    protected void specificClose() throws IOException {
        this.arrowReader.close();
        this.arrowFileStream.close();
    }
}
