package AethraDB.evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.AethraArrowFileReader;

import java.io.File;
import java.io.IOException;

/**
 * Class for wrapping the reading infrastructure of an Arrow table so that optimisations can be
 * applied irrespective of code generation.
 */
public abstract class ArrowTableReader implements AutoCloseable {

    /**
     * The file to read from.
     */
    protected File arrowFile;

    /**
     * The allocator to use for reading the table.
     */
    protected BufferAllocator tableAllocator;

    /**
     * Whether to use the {@link AethraArrowFileReader} implementation.
     */
    protected boolean useProjectingArrowReader;

    /**
     * The list of columns to project.
     */
    protected int[] columnsToProject;

    /**
     * Perform the basic initialisation required for any descendant of {@link ArrowTableReader}.
     * @param arrowFile The Arrow file to be read from.
     * @param rootAllocator The {@link RootAllocator} that is used for Arrow allocations.
     * @param useProjectingArrowReader Whether this {@link ArrowTableReader} should use the
     * {@link AethraArrowFileReader} implementation.
     * @param columnsToProject The columns of the {@code arrowFile} to actually project out.
     */
    public ArrowTableReader(File arrowFile, RootAllocator rootAllocator, boolean useProjectingArrowReader, int[] columnsToProject) {
        this.arrowFile = arrowFile;
        // Initialise a specific allocator for this table, at twice the file size to be on the safe side
        this.tableAllocator = rootAllocator.newChildAllocator(arrowFile.getName(), 0L, 2 * arrowFile.getTotalSpace());
        this.useProjectingArrowReader = useProjectingArrowReader;
        this.columnsToProject = columnsToProject;
    }

    /**
     * Required no-args constructor for the simplified constructor in {@link VirtualArrowTableReader}.
     */
    protected ArrowTableReader() {
    }

    /**
     * Method to reset the {@link ArrowTableReader} so it can once more be read from.
     */
    public abstract void reset() throws Exception;

    /**
     * Method for loading the next arrow batch to be processed.
     * @return {@code true} if a new batch could be loaded, {@code false} if there are no more batches to process.
     * @throws IOException when an I/O issue occurs during batch loading.
     */
    public abstract boolean loadNextBatch() throws IOException;

    /**
     * Method for obtaining a specific {@link FieldVector} of the current arrow batch.
     * @param index The index of the {@link FieldVector} to retrieve.
     * @return The {@link FieldVector} of the current arrow batch corresponding to {@code index}.
     */
    public abstract FieldVector getVector(int index);

    @Override
    public final void close() throws Exception {
        this.specificClose();
        this.tableAllocator.close();
    }

    /**
     * Method to be implemented by all descendants of {@link ArrowTableReader} to close their own
     * members when necessary.
     * @throws Exception when an exception occurs during closing of some member.
     */
    protected abstract void specificClose() throws Exception;

    /**
     * Method to get the Arrow {@link File} which will be read by {@code this}.
     * @return The Arrow {@link File} which will be read by {@code this}.
     */
    public File getArrowFile() {
        return this.arrowFile;
    }

    /**
     * Method to check if the {@link ArrowTableReader} represented by {@code this} projects columns.
     * @return Whether the {@link ArrowTableReader} represented by {@code this} projects columns.
     */
    public boolean projectsColumns() {
        return this.useProjectingArrowReader;
    }

    /**
     * Method to obtain the projected columns of the {@link ArrowTableReader} represented by {@code this}.
     * @return The projected columns of the {@link ArrowTableReader} represented by {@code this}.
     */
    public int[] getColumnsToProject() {
        return this.columnsToProject;
    }

}
