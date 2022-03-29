package org.apache.pinot.broker.response.impl;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.pinot.broker.response.ResponseWriter;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;


public class AvroResponseWriter implements ResponseWriter {
  // How to keep this schema updated with the broker response change in future??
  private Schema _brokerResponseSchema;

  public AvroResponseWriter(){
    _brokerResponseSchema = ReflectData.get().getSchema(BrokerResponseNative.class);
  }

  @Override
  public Object formatBrokerResponse(BrokerResponse brokerResponse) {
    return _brokerResponseSchema;
  }
}
