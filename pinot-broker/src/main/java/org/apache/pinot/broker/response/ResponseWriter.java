package org.apache.pinot.broker.response;

import org.apache.pinot.common.response.BrokerResponse;


public interface ResponseWriter {

  Object formatBrokerResponse(BrokerResponse brokerResponse);
}
