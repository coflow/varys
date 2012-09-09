package varys;

import java.util.*;

import org.apache.thrift.TException;

public class VarysServiceHandler implements VarysService.Iface {

  Map<String, MachineStat> clusterStat;
  double oldFactor = 0.2;

  public VarysServiceHandler() {
    clusterStat = Collections.synchronizedMap(new HashMap<String, MachineStat>());
  }

  @Override
  public synchronized void putOne(String hostname, MachineStat newMs) throws TException {
    MachineStat oldMs = (clusterStat.containsKey(hostname)) ? clusterStat.get(hostname) : new MachineStat(hostname, 0.0, 0.0);
    double rx_bps = (1.0 - oldFactor) * newMs.rx_bps + oldFactor * oldMs.rx_bps;
    double tx_bps = (1.0 - oldFactor) * newMs.tx_bps + oldFactor * oldMs.tx_bps;
    clusterStat.put(hostname, new MachineStat(hostname, rx_bps, tx_bps));
  }
  
  @Override
  public Map<String, MachineStat> getAll() {
    return clusterStat;
  }
  
  @Override
  public synchronized List<String> getMachines(int numMachines, double avgTxBytes) {
    ArrayList<String> retVal = new ArrayList<String>();
    if (avgTxBytes < 0.0) {
      // Random
      // TODO:
    } else {
      // Find best machines
      ArrayList<MachineStat> machineStats = new ArrayList<MachineStat>(clusterStat.values());
      Collections.sort(machineStats, new Comparator<MachineStat>(){
           public int compare(MachineStat o1, MachineStat o2){
               if (o1.tx_bps == o2.tx_bps) {
                   return 0;
               }
               return o1.tx_bps < o2.tx_bps ? -1 : 1;
           }
      });
      for (int i = 0; i < numMachines && i < machineStats.size(); i++) {
        retVal.add(machineStats.get(i).hostname);
        adjustTxBps(machineStats.get(i).hostname, avgTxBytes);
      }
    }
    return retVal;
  }
  
  private void adjustTxBps(String hostname, double txBytes) {
    MachineStat oldMs = (clusterStat.containsKey(hostname)) ? clusterStat.get(hostname) : new MachineStat(hostname, 0.0, 0.0);
    double toAdd = txBytes * 0.5; // Default blocksize is 256MB
    double newTxBps = oldMs.tx_bps + toAdd;
    double nicSpeed = 128.0 * 1024 * 1024;  // 1Gbps == 128MBps
    clusterStat.put(hostname, new MachineStat(hostname, oldMs.rx_bps, (newTxBps > nicSpeed) ? nicSpeed : newTxBps));
  }
}
