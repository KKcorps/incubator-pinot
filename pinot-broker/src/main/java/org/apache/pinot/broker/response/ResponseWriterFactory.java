package org.apache.pinot.broker.response;

import org.apache.pinot.broker.response.impl.AvroResponseWriter;
import org.apache.pinot.broker.response.impl.JSONResponseWriter;


public class ResponseWriterFactory {

  public static ResponseWriter getResponseWriterForFormat(String format) {
    switch (ResponseTypes.valueOf(format.toUpperCase())){
      case JSON: return new JSONResponseWriter();
      case AVRO: AvroResponseWriter.get();
      default: return new JSONResponseWriter();
    }
  }
}
