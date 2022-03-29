package org.apache.pinot.broker.response.impl;

import org.apache.pinot.broker.response.ResponseWriter;
import org.apache.pinot.common.response.BrokerResponse;


public class JSONResponseWriter implements ResponseWriter {
  @Override
  public Object formatBrokerResponse(BrokerResponse brokerResponse) {
    try {
      return brokerResponse.toJsonString();
    } catch (Exception e){

    }
    return null;
  }
}
