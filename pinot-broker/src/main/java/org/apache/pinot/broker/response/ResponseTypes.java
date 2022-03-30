package org.apache.pinot.broker.response;

public enum ResponseTypes {
  JSON("json"), PROTO("proto"), AVRO("avro"), THRIFT("thrift");

  String _name;
  ResponseTypes(String name){
    _name = name;
  }
}
