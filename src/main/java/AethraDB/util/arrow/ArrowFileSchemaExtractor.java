package AethraDB.util.arrow;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.vector.ipc.InvalidArrowFileException;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.arrow.vector.types.pojo.Field.convertField;

/**
 * Helper class which optimises extracting just the fields of the schema from an arrow file.
 */
public final class ArrowFileSchemaExtractor {

    private static final int MAGIC_LENGTH;
    private static final java.lang.reflect.Method VALIDATE_MAGIC_METHOD;

    static {
        try {
            Class<?> arrowMagicClass = Class.forName("org.apache.arrow.vector.ipc.ArrowMagic");

            java.lang.reflect.Field magicLengthField = arrowMagicClass.getDeclaredField("MAGIC_LENGTH");
            magicLengthField.setAccessible(true);
            MAGIC_LENGTH = magicLengthField.getInt(null);

            VALIDATE_MAGIC_METHOD = arrowMagicClass.getDeclaredMethod("validateMagic", byte[].class);
            VALIDATE_MAGIC_METHOD.setAccessible(true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayList<Field> getFieldDescriptionFromTableFile(File arrowTable) throws IOException {
        try (
            FileInputStream arrowTableStream = new FileInputStream(arrowTable);
            SeekableReadChannel in = new SeekableReadChannel(arrowTableStream.getChannel());
        ) {
            // Read the raw footer data into memory
            if (in.size() <= (MAGIC_LENGTH * 2 + 4)) {
                throw new InvalidArrowFileException("file too small: " + in.size());
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + MAGIC_LENGTH);
            long footerLengthOffset = in.size() - buffer.remaining();
            in.setPosition(footerLengthOffset);
            in.readFully(buffer);
            buffer.flip();
            byte[] array = buffer.array();
            boolean validMagic;
            try {
                byte[] magicToValidate = Arrays.copyOfRange(array, 4, array.length);
                validMagic = (boolean) VALIDATE_MAGIC_METHOD.invoke(null, magicToValidate);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!validMagic) {
                throw new InvalidArrowFileException("missing Magic number " + Arrays.toString(buffer.array()));
            }
            int footerLength = MessageSerializer.bytesToInt(array);
            if (footerLength <= 0 || footerLength + MAGIC_LENGTH * 2 + 4 > in.size() || footerLength > footerLengthOffset) {
                throw new InvalidArrowFileException("invalid footer length: " + footerLength);
            }
            long footerOffset = footerLengthOffset - footerLength;
            ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
            in.setPosition(footerOffset);
            in.readFully(footerBuffer);
            footerBuffer.flip();
            Footer footerFB = Footer.getRootAsFooter(footerBuffer);

            // And return exactly the schema from the footer
            org.apache.arrow.flatbuf.Schema schema = footerFB.schema();
            ArrayList<Field> fields = new ArrayList<>();
            for (int i = 0; i < schema.fieldsLength(); i++) {
                fields.add(convertField(schema.fields(i)));
            }
            return fields;
        }

    }

}
