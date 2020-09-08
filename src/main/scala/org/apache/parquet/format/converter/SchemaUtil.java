package org.apache.parquet.format.converter;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;

/**
 * Workaround to use conversion methods for MessageType.
 */
public class SchemaUtil {
  private static final ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private static final Method fromParquetSchema;

  static {
    try {
      fromParquetSchema = ParquetMetadataConverter.class
        .getDeclaredMethod("fromParquetSchema", List.class, List.class);
    } catch (Exception err) {
      throw new RuntimeException(err);
    }
    fromParquetSchema.setAccessible(true);
  }

  private SchemaUtil() {
  }

  /** Converts message type into Thrift schema element */
  @SuppressWarnings("unchecked")
  public static MessageType fromParquetSchema(List<SchemaElement> schema, List<ColumnOrder> order) {
    try {
      return (MessageType) fromParquetSchema.invoke(converter, schema, order);
    } catch (Exception err) {
      throw new RuntimeException(err);
    }
  }
}
