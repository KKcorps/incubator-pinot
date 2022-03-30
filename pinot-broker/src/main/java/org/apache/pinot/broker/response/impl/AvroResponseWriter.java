package org.apache.pinot.broker.response.impl;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.pinot.broker.response.ResponseWriter;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroResponseWriter implements ResponseWriter {
  public static final Logger LOG = LoggerFactory.getLogger(AvroResponseWriter.class);
  // How to keep this schema updated with the broker response change in future??
  private Schema _brokerResponseSchema;
  private final DatumWriter<BrokerResponse> _writer;
  public static AvroResponseWriter _instance;

  public AvroResponseWriter(){
    _brokerResponseSchema = ReflectData.AllowNull.get().getSchema(BrokerResponseNative.class);
    _writer = new ReflectDatumWriter<>(_brokerResponseSchema);
  }

  public static AvroResponseWriter get() {
    if(_instance == null) {
      return new AvroResponseWriter();
    }
    return _instance;
  }

  @Override
  public Object formatBrokerResponse(BrokerResponse brokerResponse) {
    try {
      final ByteArrayOutputStream bout = new ByteArrayOutputStream();
      final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
      _writer.write(brokerResponse, binEncoder);
      binEncoder.flush();
      return bout.toByteArray();
    } catch (Exception e) {
      LOG.error("Caught exception while creating broker avro schema: " + brokerResponse);
    }
    return null;
  }
}
