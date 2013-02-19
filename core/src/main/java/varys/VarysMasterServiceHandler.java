package varys;

import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysMasterServiceHandler implements VarysMasterService.Iface {

  public VarysMasterServiceHandler() {
  }

  /* private void keepPrintingNetworkStats() {
    Thread t = new Thread (
      new Runnable() {
        @Override 
        public void run() {
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (Exception e) {
              e.printStackTrace();
            }
            
            double rx[] = new double[nameToRxBpsMap.size()];
            double tx[] = new double[nameToTxBpsMap.size()];
            int index = 0;
            for (String name: nameToRxBpsMap.keySet()) {
              rx[index] = nameToRxBpsMap.getBps(name);
              tx[index] = nameToTxBpsMap.getBps(name);
              index++;
            }
            System.out.printf("%03d |RX| AVG= %12.2f STDEV= %12.2f COVAR= %12.2f |TX| AVG= %12.2f STDEV= %12.2f COVAR= %12.2f\n", index,
              VarysCommon.average(rx), VarysCommon.stdev(rx), VarysCommon.covar(rx),
              VarysCommon.average(tx), VarysCommon.stdev(tx), VarysCommon.covar(tx));
          }
        }
      }
    );
    t.setDaemon(true);
    t.start();
  } */

  @Override
  public synchronized List<String> getMachines(int numMachines, long avgTxBytes) {
    /* List<String> retVal = null;
    String placementPolicy = VarysCommon.varysProperties.getProperty("varys.placementPolicy", "Random");
    if (placementPolicy.startsWith("Random")) {
      retVal = nameToTxBpsMap.getRandomN(numMachines, avgTxBytes);
    } else if (placementPolicy.startsWith("NetworkAware")) {
      retVal = nameToTxBpsMap.getTopN(numMachines, avgTxBytes);
    } */
    return null;
  }
}
