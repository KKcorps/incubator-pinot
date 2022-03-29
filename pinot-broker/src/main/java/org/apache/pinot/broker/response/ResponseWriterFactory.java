package org.apache.pinot.broker.response;

import org.apache.pinot.broker.response.impl.JSONResponseWriter;


public class ResponseWriterFactory {

  public static ResponseWriter getResponseWriterForFormat(String format) {
    switch (ResponseTypes.valueOf(format)){
      case JSON: return new JSONResponseWriter();
      default: return new JSONResponseWriter();
    }
  }
}
