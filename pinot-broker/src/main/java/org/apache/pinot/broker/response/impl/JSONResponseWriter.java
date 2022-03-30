package org.apache.pinot.broker.response.impl;

import org.apache.pinot.broker.response.ResponseWriter;
import org.apache.pinot.common.response.BrokerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONResponseWriter implements ResponseWriter {
  public static final Logger LOG = LoggerFactory.getLogger(JSONResponseWriter.class);

  @Override
  public Object formatBrokerResponse(BrokerResponse brokerResponse) {
    try {
      return brokerResponse.toJsonString();
    } catch (Exception e){
      LOG.error("Caught exception while creating broker JSON: " + brokerResponse);
    }
    return null;
  }
}
