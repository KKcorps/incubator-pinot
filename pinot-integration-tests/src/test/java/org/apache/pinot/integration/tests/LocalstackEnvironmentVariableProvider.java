package org.apache.pinot.integration.tests;

import cloud.localstack.docker.annotation.IEnvironmentVariableProvider;
import java.util.HashMap;
import java.util.Map;


public class LocalstackEnvironmentVariableProvider implements IEnvironmentVariableProvider {
  @Override
  public Map<String, String> getEnvironmentVariables() {
    Map<String, String> mp = new HashMap<>();
    mp.put("KINESIS_PROVIDER", "kinesalite");
    return mp;
  }
}
