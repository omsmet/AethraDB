/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This class has been edited in the AethraDB context to remove overhead from the implementation.
 * Specifically JSON related functionality has been removed from this edited class.
 */

package org.apache.arrow.vector.types.pojo;


import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Endianness;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.ipc.message.FBSerializables;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.arrow.vector.types.pojo.Field.convertField;

/**
 * An Arrow Schema.
 */
public class Schema {

  /**
   * Search for a field by name in given the list of fields.
   *
   * @param fields the list of the fields
   * @param name   the name of the field to return
   * @return the corresponding field
   * @throws IllegalArgumentException if the field was not found
   */
  public static Field findField(List<Field> fields, String name) {
    for (Field field : fields) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    throw new IllegalArgumentException(String.format("field %s not found in %s", name, fields));
  }

  static final String METADATA_KEY = "key";
  static final String METADATA_VALUE = "value";

// Removed since AethraDB does not use this functionality
//  private static final ObjectMapper mapper = new ObjectMapper();
//  private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
//  private static final ObjectReader reader = mapper.readerFor(Schema.class);
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

//  Removed since AethraDB does not use this functionality
//  public static Schema fromJSON(String json) throws IOException {
//    return reader.readValue(Preconditions.checkNotNull(json));
//  }

  public static Schema deserialize(ByteBuffer buffer) {
    return convertSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(buffer));
  }

  /** Converts a flatbuffer schema to its POJO representation. */
  public static Schema convertSchema(org.apache.arrow.flatbuf.Schema schema) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < schema.fieldsLength(); i++) {
      fields.add(convertField(schema.fields(i)));
    }
    Map<String, String> metadata = new HashMap<>();
    for (int i = 0; i < schema.customMetadataLength(); i++) {
      KeyValue kv = schema.customMetadata(i);
      String key = kv.key();
      String value = kv.value();
      metadata.put(key == null ? "" : key, value == null ? "" : value);
    }
    return new Schema(true, Collections.unmodifiableList(fields), Collections.unmodifiableMap(metadata));
  }

  private final List<Field> fields;
  private final Map<String, String> metadata;

  public Schema(Iterable<Field> fields) {
    this(fields, (Map<String, String>) null);
  }

  /**
   * Constructor with metadata.
   */
  public Schema(Iterable<Field> fields,
                Map<String, String> metadata) {
    this(true,
        Collections2.toImmutableList(fields),
        metadata == null ? Collections.emptyMap() : Collections2.immutableMapCopy(metadata));
  }

// Removed since AethraDB does not use this functionality
//  /**
//   * Constructor used for JSON deserialization.
//   */
//  @JsonCreator
//  private Schema(@JsonProperty("fields") Iterable<Field> fields,
//                @JsonProperty("metadata") List<Map<String, String>> metadata) {
//    this(fields, convertMetadata(metadata));
//  }


  /**
   * Private constructor to bypass automatic collection copy.
   * @param unsafe a ignored argument. Its only purpose is to prevent using the constructor
   *     by accident because of type collisions (List vs Iterable).
   */
  private Schema(boolean unsafe, List<Field> fields, Map<String, String> metadata) {
    this.fields = fields;
    this.metadata = metadata;
  }

  static Map<String, String> convertMetadata(List<Map<String, String>> metadata) {
    return (metadata == null) ? null : metadata.stream()
        .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.get(METADATA_KEY), e.get(METADATA_VALUE)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static List<Map<String, String>> convertMetadata(Map<String, String> metadata) {
    return (metadata == null) ? null : metadata.entrySet()
        .stream()
        .map(Schema::convertEntryToKeyValueMap)
        .collect(Collectors.toList());
  }

  private static Map<String, String> convertEntryToKeyValueMap(Map.Entry<String, String> entry) {
    Map<String, String> map = new HashMap<>(2);
    map.put(METADATA_KEY, entry.getKey());
    map.put(METADATA_VALUE, entry.getValue());
    return Collections.unmodifiableMap(map);
  }

  public List<Field> getFields() {
    return fields;
  }

// Removed since AethraDB does not use this functionality
//  @JsonIgnore
  public Map<String, String> getCustomMetadata() {
    return metadata;
  }

// Removed since AethraDB does not use this functionality
//  @JsonProperty("metadata")
//  @JsonInclude(Include.NON_EMPTY)
//  List<Map<String, String>> getCustomMetadataForJson() {
//    return convertMetadata(getCustomMetadata());
//  }

  /**
   * Search for a field by name in this Schema.
   *
   * @param name the name of the field to return
   * @return the corresponding field
   * @throws IllegalArgumentException if the field was not found
   */
  public Field findField(String name) {
    return findField(getFields(), name);
  }

// Removed since AethraDB does not use this functionality
//  /**
//   * Returns the JSON string representation of this schema.
//   */
//  public String toJson() {
//    try {
//      return writer.writeValueAsString(this);
//    } catch (JsonProcessingException e) {
//      // this should not happen
//      throw new RuntimeException(e);
//    }
//  }

  /**
   *  Adds this schema to the builder returning the size of the builder after adding.
   */
  public int getSchema(FlatBufferBuilder builder) {
    int[] fieldOffsets = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldOffsets[i] = fields.get(i).getField(builder);
    }
    int fieldsOffset = org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    int metadataOffset = FBSerializables.writeKeyValues(builder, metadata);
    org.apache.arrow.flatbuf.Schema.startSchema(builder);
    org.apache.arrow.flatbuf.Schema.addEndianness(builder,
        (LITTLE_ENDIAN ? Endianness.Little : Endianness.Big));
    org.apache.arrow.flatbuf.Schema.addFields(builder, fieldsOffset);
    org.apache.arrow.flatbuf.Schema.addCustomMetadata(builder, metadataOffset);
    return org.apache.arrow.flatbuf.Schema.endSchema(builder);
  }

  /**
   * Returns the serialized flatbuffer representation of this schema.
   */
  public byte[] toByteArray() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = this.getSchema(builder);
    builder.finish(schemaOffset);
    ByteBuffer bb = builder.dataBuffer();
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    return bytes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, metadata);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Schema)) {
      return false;
    }
    return Objects.equals(this.fields, ((Schema) obj).fields) &&
        Objects.equals(this.metadata, ((Schema) obj).metadata);
  }

  @Override
  public String toString() {
    String meta = metadata.isEmpty() ? "" : "(metadata: " + metadata.toString() + ")";
    return "Schema<" + fields.stream().map(t -> t.toString()).collect(Collectors.joining(", ")) + ">" + meta;
  }
}
