package evaluation.codegen.infrastructure.data;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.calcite.util.ImmutableIntList;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * An {@link ArrowTableReader} specialisation which performs the reading of data on a separate thread.
 * It then maps the methods of {@link ArrowTableReader} to the buffered data from the other thread.
 * Buffering the data is done using an {@link ArrayBlockingQueue}.
 */
public class ABQArrowTableReader extends ArrowTableReader {

    /**
     * The amount of batches of the arrow file that are cached by this reader.
     */
    public static int QUEUE_CAPACITY = 16;

    /**
     * The buffer indicating whether there are more vectors to be processed.
     */
    private final ArrayBlockingQueue<Boolean> loadNextBatchResultQueue;

    /**
     * The buffer containing the Arrow vectors that have already been read.
     */
    private final ArrayBlockingQueue<FieldVector[]> fieldVectorQueue;

    /**
     * Reference to the thread that performs the actual reading of the table.
     */
    private ReaderThread readerThread;

    /**
     * Boolean indicating whether the reader thread has already been spawned or not.
     */
    private boolean readerThreadActive = false;

    /**
     * The batch which is currently available via the {@code getVector} method.
     */
    private FieldVector[] currentBatch;

    /**
     * Creates a new {@link ABQArrowTableReader} instance.
     * @param arrowFile The Arrow IPC file representing the table.
     * @param rootAllocator The {@link RootAllocator} used for Arrow operations.
     * @param columnsToProject The columns of the {@code arrowFile} to actually project out.
     * @throws FileNotFoundException When the specified Arrow file cannot be found.
     */
    public ABQArrowTableReader(File arrowFile, RootAllocator rootAllocator, ImmutableIntList columnsToProject) throws Exception {
        super(arrowFile, rootAllocator, columnsToProject);
        this.loadNextBatchResultQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.fieldVectorQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.readerThread = new ReaderThread(this.arrowFile, this.tableAllocator, this.columnsToProject, this.loadNextBatchResultQueue, this.fieldVectorQueue);
    }

    @Override
    public void reset() throws Exception {
        // First close the previous reader thread
        this.readerThread.close();

        // Reset the queues
        this.loadNextBatchResultQueue.clear();
        this.fieldVectorQueue.clear();

        // Then create a new one
        this.readerThread = new ReaderThread(
                this.arrowFile,
                this.tableAllocator,
                this.columnsToProject,
                this.loadNextBatchResultQueue,
                this.fieldVectorQueue);
        this.readerThreadActive = false;
    }

    @Override
    public boolean loadNextBatch() {
        if (!this.readerThreadActive) {
            // Launch the reader thread if necessary
            this.readerThread.start();
            this.readerThreadActive = true;

        } else {
            // Otherwise the currentBatch is already populated and should be closed
            // Note that only the "projected" columns are valid entries, so only those need to be closed
            for (int projectedColumnIndex : this.columnsToProject) {
                this.currentBatch[projectedColumnIndex].close();
            }
        }

        // Get the next batch
        try {
            // Check if the next batch exists (blocking)
            if (!this.loadNextBatchResultQueue.take())
                return false;

            // If so, buffer it (blocking)
            this.currentBatch = this.fieldVectorQueue.take();
            return true;

        } catch (InterruptedException e) {
            throw new RuntimeException("ABQArrowTableReader.loadNextBatch InterruptedException occurred: ", e);
        }

    }

    @Override
    public FieldVector getVector(int index) {
        return this.currentBatch[index];
    }

    @Override
    protected void specificClose() throws Exception {
        // Close the reader thread
        this.readerThread.close();
    }

    /**
     * Definition of the class that actually performs the reading of the table file into the buffer.
     */
    private static class ReaderThread extends Thread implements Closeable {

        /**
         * The {@link BufferAllocator} used for reading the input file.
         */
        private final BufferAllocator tableAllocator;

        /**
         * The {@link FileInputStream} used for reading the input file.
         */
        private final FileInputStream tableInputStream;

        /**
         * The {@link AethraArrowFileReader} used for reading the input file.
         */
        private final AethraArrowFileReader tableFileReader;

        /**
         * The {@link VectorSchemaRoot} of the input table.
         */
        private final VectorSchemaRoot schemaRoot;

        /**
         * The number of columns in the input table file.
         */
        private final int columnCount;

        /**
         * The columns of the input file to project out. (i.e. to read from the file)
         */
        private final int[] columnsToProject;

        /**
         * The {@link ArrayBlockingQueue} to store the result for {@code loadNextBatch} calls.
         */
        private final ArrayBlockingQueue<Boolean> loadNextBatchTargetQueue;

        /**
         * The {@link ArrayBlockingQueue} to cache the read {@link FieldVector}s into.
         */
        private final ArrayBlockingQueue<FieldVector[]> fieldVectorTargetQueue;

        /**
         * Creates a new instance of the {@link ReaderThread} class.
         * @param arrowFile The table file to be read by the created instance.
         * @param tableAllocator The {@link BufferAllocator} to use for reading the table.
         * @param columnsToProject The actual columns to project out.
         * @param loadNextBatchTargetQueue  The queue into which to buffer the result for {@code loadNextBatch} calls.
         * @param fieldVectorTargetQueue The queue into which to buffer the {@link FieldVector}s that have been read.
         * @throws FileNotFoundException If the {@code arrowFile} cannot be found.
         * @throws IOException If the input table cannot be read correctly.
         */
        public ReaderThread(
                File arrowFile,
                BufferAllocator tableAllocator,
                ImmutableIntList columnsToProject,
                ArrayBlockingQueue<Boolean> loadNextBatchTargetQueue,
                ArrayBlockingQueue<FieldVector[]> fieldVectorTargetQueue
        ) throws IOException {
            this.tableAllocator = tableAllocator;
            this.tableInputStream = new FileInputStream(arrowFile);
            this.tableFileReader = new AethraArrowFileReader(this.tableInputStream.getChannel(), this.tableAllocator, columnsToProject);
            this.schemaRoot = this.tableFileReader.getVectorSchemaRoot();
            this.columnCount = schemaRoot.getFieldVectors().size();
            this.columnsToProject = columnsToProject.toIntArray();
            this.loadNextBatchTargetQueue = loadNextBatchTargetQueue;
            this.fieldVectorTargetQueue = fieldVectorTargetQueue;
        }

        @Override
        public void run() {
            try {
                while (tableFileReader.loadNextBatch()) {
                    FieldVector[] vectorBatch = new FieldVector[columnCount];

                    // Buffer the actual batch as indicated by the columns to project
                    for (int i : this.columnsToProject) {
                        FieldVector fv_cvi_i = schemaRoot.getVector(i);
                        if (fv_cvi_i instanceof IntVector int_fv_cvi_i) {
                            vectorBatch[i] = new IntVector(int_fv_cvi_i.getField(), this.tableAllocator);
                            int_fv_cvi_i.transferTo((IntVector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof FixedSizeBinaryVector fsbv_fv_cvi_i) {
                            vectorBatch[i] = new FixedSizeBinaryVector(fsbv_fv_cvi_i.getField(), this.tableAllocator);
                            fsbv_fv_cvi_i.transferTo((FixedSizeBinaryVector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof Float8Vector f8_fv_cvi_i) {
                            vectorBatch[i] = new Float8Vector(f8_fv_cvi_i.getField(), this.tableAllocator);
                            f8_fv_cvi_i.transferTo((Float8Vector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof LargeVarCharVector lvc_fv_cvi_i) {
                            vectorBatch[i] = new LargeVarCharVector(lvc_fv_cvi_i.getField(), this.tableAllocator);
                            lvc_fv_cvi_i.transferTo((LargeVarCharVector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof DateDayVector dd_fv_cvi_i) {
                            vectorBatch[i] = new DateDayVector(dd_fv_cvi_i.getField(), this.tableAllocator);
                            dd_fv_cvi_i.transferTo((DateDayVector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof DecimalVector dd_fv_cvi_i) {
                            vectorBatch[i] = new DecimalVector(dd_fv_cvi_i.getField(), this.tableAllocator);
                            dd_fv_cvi_i.transferTo((DecimalVector) vectorBatch[i]);

                        } else if (fv_cvi_i instanceof VarCharVector vc_fv_cvi_i) {
                            vectorBatch[i] = new VarCharVector(vc_fv_cvi_i.getField(), this.tableAllocator);
                            vc_fv_cvi_i.transferTo((VarCharVector) vectorBatch[i]);

                        } else {
                            throw new UnsupportedOperationException(
                                    "ABQArrowTableReader.ReaderThread could not buffer the current field vector type: " + fv_cvi_i.getClass());
                        }
                    }

                    // Write it into the loadNextBatch queue (blocking)
                    this.loadNextBatchTargetQueue.put(true);

                    // Write the actual batch into its queue (blocking)
                    this.fieldVectorTargetQueue.put(vectorBatch);

                }

                // Final batch has been read, so only communicate this via the loadNextBatch queue (blocking)
                this.loadNextBatchTargetQueue.put(false);

            } catch (IOException e) {
                throw new RuntimeException("ABQArrowTableReader.ReaderThread IOException occurred: ", e);
            } catch (InterruptedException e) {
                throw new RuntimeException("ABQArrowTableReader.ReaderThread InterruptedException occurred: ", e);
            }
        }

        @Override
        public void close() throws IOException {
            this.schemaRoot.close();
            this.tableFileReader.close();
            this.tableInputStream.close();
        }
    }

}
