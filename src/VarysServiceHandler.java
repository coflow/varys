package varys;

import java.util.*;

import org.apache.thrift.TException;

public class VarysServiceHandler implements VarysService.Iface {

  Map<String, MachineStat> clusterStat;

  public VarysServiceHandler() {
    clusterStat = Collections.synchronizedMap(new HashMap<String, MachineStat>());
  }

  @Override
  public void putOne(String hostname, MachineStat machineStat) throws TException {
    clusterStat.put(hostname, machineStat);
  }
  
  @Override
  public Map<String, MachineStat> getAll() {
    return clusterStat;
  }
}
