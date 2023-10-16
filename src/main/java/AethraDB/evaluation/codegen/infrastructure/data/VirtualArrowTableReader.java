package AethraDB.evaluation.codegen.infrastructure.data;

import org.apache.arrow.vector.FieldVector;

import java.io.File;
import java.io.IOException;

/**
 * Class for representing an {@link ArrowTableReader} during code generation.
 */
public class VirtualArrowTableReader extends ArrowTableReader {

    /**
     * Creates a new {@link VirtualArrowTableReader} instance
     * @param arrowFile The Arrow IPC file representing the table.
     * @param useProjectingArrowReader Whether this {@link ArrowTableReader} should use a projecting table reader.
     * @param columnsToProject The columns of the {@code arrowFile} to actually project out.
     */
    public VirtualArrowTableReader(File arrowFile, boolean useProjectingArrowReader, int[] columnsToProject) {
        // Simplified constructor of the ArrowTableReader, which uses the protected no-args constructor
        super();
        this.arrowFile = arrowFile;
        this.useProjectingArrowReader = useProjectingArrowReader;
        this.columnsToProject = columnsToProject;
    }

    @Override
    public void reset() throws Exception {
        throw new UnsupportedOperationException("VirtualArrowTableReader cannot be reset");
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        throw new UnsupportedOperationException("VirtualArrowTableReader cannot load batches");
    }

    @Override
    public FieldVector getVector(int index) {
        throw new UnsupportedOperationException("VirtualArrowTableReader cannot obtain vectors");
    }

    @Override
    protected void specificClose() {

    }
}
