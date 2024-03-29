package org.apache.arrow.vector.ipc;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.vector.ipc.ArrowMagic.MAGIC_LENGTH;
import static org.apache.arrow.vector.ipc.message.MessageSerializer.IPC_CONTINUATION_TOKEN;

/**
 * AethraDB specific class.
 * Class which performs reading of Arrow IPC files, but while reducing overhead compared to the
 * standard {@link ArrowFileReader}.
 *
 * However, to do its work, this reader makes some important assumptions:
 * - It assumes that no compression is used in the input file
 * - It assumes that no vector has a child vector
 * - It assumes that field vectors are always aligned to 8-byte boundaries
 */
public class AethraArrowFileReader extends ArrowReader {

    private final SeekableReadChannel in;
    private final int[] columnsToRead;
    private boolean[] columnEnabled;
    private ArrowFooter footer;
    private int currentDictionaryBatch = 0;
    private int currentRecordBatch = 0;

    private AethraArrowFileReader(
            SeekableReadChannel in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory, int[] columnsToRead) {
        super(allocator, compressionFactory);
        this.in = in;
        this.columnsToRead = columnsToRead;
        this.columnEnabled = null;
    }

    private AethraArrowFileReader(
            SeekableByteChannel in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory, int[] columnsToRead) {
        this(new SeekableReadChannel(in), allocator, compressionFactory, columnsToRead);
    }

    private AethraArrowFileReader(SeekableReadChannel in, BufferAllocator allocator, int[] columnsToRead) {
        this(in, allocator, NoCompressionCodec.Factory.INSTANCE, columnsToRead);
    }

    public AethraArrowFileReader(SeekableByteChannel in, BufferAllocator allocator, int[] columnsToRead) {
        this(new SeekableReadChannel(in), allocator, columnsToRead);
    }

    @Override
    public long bytesRead() {
        return in.bytesRead();
    }

    @Override
    protected void closeReadSource() throws IOException {
        in.close();
    }

    @Override
    public Schema readSchema() throws IOException {
        if (footer == null) {
            if (in.size() <= (MAGIC_LENGTH * 2 + 4)) {
                throw new InvalidArrowFileException("file too small: " + in.size());
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + MAGIC_LENGTH);
            long footerLengthOffset = in.size() - buffer.remaining();
            in.setPosition(footerLengthOffset);
            in.readFully(buffer);
            buffer.flip();
            byte[] array = buffer.array();
            if (!ArrowMagic.validateMagic(Arrays.copyOfRange(array, 4, array.length))) {
                throw new InvalidArrowFileException("missing Magic number " + Arrays.toString(buffer.array()));
            }
            int footerLength = MessageSerializer.bytesToInt(array);
            if (footerLength <= 0 || footerLength + MAGIC_LENGTH * 2 + 4 > in.size() ||
                    footerLength > footerLengthOffset) {
                throw new InvalidArrowFileException("invalid footer length: " + footerLength);
            }
            long footerOffset = footerLengthOffset - footerLength;
            ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
            in.setPosition(footerOffset);
            in.readFully(footerBuffer);
            footerBuffer.flip();
            Footer footerFB = Footer.getRootAsFooter(footerBuffer);
            this.footer = new ArrowFooter(footerFB);

            // Initialise the "columnEnabled" field
            int columnCount = this.footer.getSchema().getFields().size();
            this.columnEnabled = new boolean[columnCount];
            Arrays.fill(this.columnEnabled, false);
            for (int i : this.columnsToRead)
                this.columnEnabled[i] = true;
        }
        MetadataV4UnionChecker.checkRead(footer.getSchema(), footer.getMetadataVersion());
        return footer.getSchema();
    }

    @Override
    public void initialize() throws IOException {
        super.initialize();

        // empty stream, has no dictionaries in IPC.
        if (footer.getRecordBatches().size() == 0) {
            return;
        }
        // Read and load all dictionaries from schema
        for (int i = 0; i < dictionaries.size(); i++) {
            ArrowDictionaryBatch dictionaryBatch = readDictionary();
            loadDictionary(dictionaryBatch);
        }
    }

    /**
     * Get custom metadata.
     */
    public Map<String, String> getMetaData() {
        if (footer != null) {
            return footer.getMetaData();
        }
        return new HashMap<>();
    }

    /**
     * Read a dictionary batch from the source, will be invoked after the schema has been read and
     * called N times, where N is the number of dictionaries indicated by the schema Fields.
     *
     * @return the read ArrowDictionaryBatch
     * @throws IOException on error
     */
    public ArrowDictionaryBatch readDictionary() throws IOException {
        if (currentDictionaryBatch >= footer.getDictionaries().size()) {
            throw new IOException("Requested more dictionaries than defined in footer: " + currentDictionaryBatch);
        }
        ArrowBlock block = footer.getDictionaries().get(currentDictionaryBatch++);
        return readDictionaryBatch(in, block, allocator);
    }

    /** Returns true if a batch was read, false if no more batches. */
    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();

        if (currentRecordBatch < footer.getRecordBatches().size()) {
            ArrowBlock block = footer.getRecordBatches().get(currentRecordBatch++);
            readAndLoadRecordBatch(in, block, allocator);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the {@link ArrowBlock} metadata from the file.
     */
    public List<ArrowBlock> getRecordBlocks() throws IOException {
        ensureInitialized();
        return footer.getRecordBatches();
    }

    /**
     * Loads record batch for the given block.
     */
    public boolean loadRecordBatch(ArrowBlock block) throws IOException {
        ensureInitialized();
        int blockIndex = footer.getRecordBatches().indexOf(block);
        if (blockIndex == -1) {
            throw new IllegalArgumentException("Arrow block does not exist in record batches: " + block);
        }
        currentRecordBatch = blockIndex;
        return loadNextBatch();
    }

    @VisibleForTesting
    ArrowFooter getFooter() {
        return footer;
    }

    private ArrowDictionaryBatch readDictionaryBatch(SeekableReadChannel in,
                                                     ArrowBlock block,
                                                     BufferAllocator allocator) throws IOException {
        in.setPosition(block.getOffset());
        ArrowDictionaryBatch batch = MessageSerializer.deserializeDictionaryBatch(in, block, allocator);
        if (batch == null) {
            throw new IOException("Invalid file. No batch at offset: " + block.getOffset());
        }
        return batch;
    }

    private void readAndLoadRecordBatch(SeekableReadChannel in, ArrowBlock block, BufferAllocator allocator) throws IOException {
        // Move the reader to the correct position and initialise a variable to keep track of the current position
        long currentInPosition = block.getOffset();
        in.setPosition(currentInPosition);

        // Start by only reading metadata: metadata length contains prefix_size bytes plus byte padding
        long metadataLen = block.getMetadataLength();
        ArrowBuf metadataPrefixBuffer = allocator.buffer(metadataLen);
        if (in.readFully(metadataPrefixBuffer, metadataLen) != metadataLen) {
            throw new IOException("Unexpected end of input trying to read batch metadata.");
        }
        currentInPosition += metadataLen;

        // Extract the metadata without the prefix
        int prefixSize = metadataPrefixBuffer.getInt(0) == IPC_CONTINUATION_TOKEN ? 8 : 4;
        ArrowBuf metadataBuffer = metadataPrefixBuffer.slice(prefixSize, metadataLen - prefixSize);

        // Parse the metadata
        Message messageFB = Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());
        RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());

        // Parse information about the column nodes
        int nodesLength = recordBatchFB.nodesLength();
        List<ArrowFieldNode> nodes = new ArrayList<>();
        for (int i = 0; i < nodesLength; ++i) {
            FieldNode node = recordBatchFB.nodes(i);
            if ((int) node.length() != node.length() || (int) node.nullCount() != node.nullCount()) {
                throw new IOException("Cannot currently deserialize record batches with node length larger than INT_MAX records.");
            }
            nodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
        }

        // Now read the vectors of the body that we actually need
        VectorSchemaRoot root = this.getVectorSchemaRoot();
        List<FieldVector> fieldVectors = root.getFieldVectors();
        int currentRecordBatchFBBufferIndex = 0;
        for (int columnIndex = 0; columnIndex < fieldVectors.size(); columnIndex++) {

            // Get the layout of the current column
            ArrowFieldNode columnFieldNode = nodes.get(columnIndex);
            FieldVector columnFieldVector = fieldVectors.get(columnIndex);
            Field columnField = columnFieldVector.getField();
            int bufferLayoutCount = TypeLayout.getTypeBufferCount(columnField.getType());

            List<Buffer> columnsBufferDefinitions = new ArrayList<>(bufferLayoutCount);
            for (int i = 0; i < bufferLayoutCount; i++)
                columnsBufferDefinitions.add(i, recordBatchFB.buffers(currentRecordBatchFBBufferIndex++));

            // If a column is not enabled, we need to skip its buffers in the input
            if (!this.columnEnabled[columnIndex]) {
                for (Buffer bufferToSkip : columnsBufferDefinitions) {
                    currentInPosition += bufferToSkip.length();
                    currentInPosition = DataSizeRoundingUtil.roundUpTo8Multiple(currentInPosition); // Align to 8-byte boundaries
                }
                continue;
            }

            // Otherwise, we need to load the data into memory (assumption: no decompression necessary)
            List<ArrowBuf> columnBuffers = new ArrayList<>(bufferLayoutCount);
            for (Buffer cbd : columnsBufferDefinitions) {
                long bufferLength = cbd.length();
                in.setPosition(currentInPosition);
                ArrowBuf actualColumnBuffer = allocator.buffer(bufferLength);
                if (in.readFully(actualColumnBuffer, bufferLength) != bufferLength) {
                    throw new IOException("Unexpected end of input trying to read batch column buffer.");
                }
                currentInPosition += bufferLength;
                currentInPosition = DataSizeRoundingUtil.roundUpTo8Multiple(currentInPosition);     // Align to 8-byte boundaries
                columnBuffers.add(actualColumnBuffer);
            }

            // Try to assign the buffers to the field vector
            try {
                columnFieldVector.loadFieldBuffers(columnFieldNode, columnBuffers);

            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Could not load buffers for field " +
                        columnFieldVector + ". error message: " + e.getMessage(), e);
            }

            // Check that there are no children to be loaded too
            if (columnField.getChildren().size() > 0)
                throw new IllegalStateException("AethraArrowFileReader does not support child fields");

            // Make sure reference counting is up-to-date
            for (ArrowBuf cb : columnBuffers)
                cb.getReferenceManager().release();

        }

        // Update the row count of the root (as all *initialised* field vectors have already been set
        // this specific call to set the root count only saves initialisation of unused vectors)
        root.setRowCountRootOnly(checkedCastToInt(recordBatchFB.length()));

        // Release the metadata buffer
        metadataPrefixBuffer.getReferenceManager().release();

    }

}
