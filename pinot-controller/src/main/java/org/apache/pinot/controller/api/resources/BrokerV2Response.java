package org.apache.pinot.controller.api.resources;

public class BrokerV2Response {
  private String _name;
  private String _host;
  private Integer _port;

  public BrokerV2Response(String name, String host, Integer port) {
    _name = name;
    _host = host;
    _port = port;
  }

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    _name = name;
  }

  public String getHost() {
    return _host;
  }

  public void setHost(String host) {
    _host = host;
  }

  public Integer getPort() {
    return _port;
  }

  public void setPort(Integer port) {
    _port = port;
  }
}
