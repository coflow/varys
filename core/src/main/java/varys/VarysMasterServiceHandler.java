package varys;

import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysMasterServiceHandler implements VarysMasterService.Iface {

  private class BpsInfo {
    double bps = 0.0;
    double tempBps = 0.0;
    boolean isTemp = false;
    long lastUpdateTime = now();
    
    void resetToNormal(double bps) {  
      this.bps = bps;
      this.tempBps = bps;
      this.isTemp = false;
      this.lastUpdateTime = now();
    }
    
    void moveToTemp(long blockSize) {
      // Into the temporary zone
      this.isTemp = true;

      // 1Gbps == 128MBps
      double nicSpeed = 128.0 * 1024 * 1024;
      
      // Aim to increase by the remaining capacity of the link
      double incVal = nicSpeed - this.tempBps;
      if (incVal < 0.0) {
        incVal = 0.0;
      }
      
      // Calculate the expected time till the next update
      double secElapsed = (now() - lastUpdateTime) / 1000.0;
      double timeTillUpdate = 1.0 * VarysCommon.HEARTBEAT_INTERVAL_SEC - secElapsed;

      // Bound incVal by blockSize
      if (timeTillUpdate > 0.0) {
        double temp = blockSize / timeTillUpdate;
        if (temp < incVal) {
          incVal = temp;
        }
      }
      
      this.tempBps = this.tempBps + incVal;
    }
    
    private long now() {
      return System.currentTimeMillis();
    }
  }
  
  double oldFactor = 0.2;
  
  NameToBpsMap nameToRxBpsMap = null;
  NameToBpsMap nameToTxBpsMap = null;
  
  private class NameToBpsMap {
    
    private Map<String, BpsInfo> nameToBpsMap = Collections.synchronizedMap(new HashMap<String, BpsInfo>());
    
    public int size() {
      return nameToBpsMap.size();
    }
    
    public Set<String> keySet() {
      return nameToBpsMap.keySet();
    }
    
    public Set<Map.Entry<String, BpsInfo>> entrySet() {
      return nameToBpsMap.entrySet();
    }
    
    public Collection<BpsInfo> values() {
      return nameToBpsMap.values();
    }
    
    public void updateNetworkInfo(String name, double newBps) {
      BpsInfo bpsInfo = (nameToBpsMap.containsKey(name)) ? nameToBpsMap.get(name) : new BpsInfo();
      double bps = (1.0 - oldFactor) * newBps + oldFactor * bpsInfo.bps;
      bpsInfo.resetToNormal(bps);
      nameToBpsMap.put(name, bpsInfo);
    }

    public void adjustBps(String name, long blockSize) {
      BpsInfo bpsInfo = (nameToBpsMap.containsKey(name)) ? nameToBpsMap.get(name) : new BpsInfo();
      bpsInfo.moveToTemp(blockSize);
      nameToBpsMap.put(name, bpsInfo);
    }

    public double getBps(String name) {
      BpsInfo bpsInfo = nameToBpsMap.containsKey(name) ? nameToBpsMap.get(name) : new BpsInfo();
      return bpsInfo.isTemp ? bpsInfo.tempBps : bpsInfo.bps;
    }

    public synchronized String getRandom(long adjustBytes) {
      List<String> ret = getRandomN(1, adjustBytes);
      if (ret == null || ret.size() == 0) {
        return null;
      }
      return ret.get(0);
    }
    
    public synchronized List<String> getRandomN(int numMachines, long adjustBytes) {
      ArrayList<String> retVal = new ArrayList<String>();
      ArrayList<String> machines = new ArrayList(nameToRxBpsMap.keySet());
      assert(numMachines <= machines.size());
      
      boolean[] wasSelected = new boolean[machines.size()];
      Arrays.fill(wasSelected, false);
      
      while (numMachines-- > 0) {
        int toAdd = -1;
        while (toAdd == -1) {
          toAdd = writeBlockRanGen.nextInt(machines.size());
          if (wasSelected[toAdd]) {
            toAdd = -1;
          }
        }
        retVal.add(machines.get(toAdd));
        adjustBps(machines.get(toAdd), adjustBytes);
        wasSelected[toAdd] = true;
      }
      return retVal;
    }
    
    public synchronized String getTop(long adjustBytes) {
      List<String> ret = getTopN(1, adjustBytes);
      if (ret == null || ret.size() == 0) {
        return null;
      }
      return ret.get(0);
    }
    
    public synchronized List<String> getTopN(int numMachines, long adjustBytes) {
      ArrayList<String> retVal = new ArrayList<String>();
      assert(adjustBytes >= 0);

      ArrayList<String> machines = new ArrayList<String>(nameToBpsMap.keySet());
      Collections.sort(machines, new Comparator<String>(){
         public int compare(String o1, String o2){
           double o1bps = getBps(o1);
           double o2bps = getBps(o2);
           if (o1bps == o2bps) {
               return 0;
           }
           return o1bps < o2bps ? -1 : 1;
         }
      });
      for (int i = 0; i < numMachines && i < machines.size(); i++) {
        retVal.add(machines.get(i));
        adjustBps(machines.get(i), adjustBytes);
      }
      
      return retVal;
    }
    
  }

  Random writeBlockRanGen = new Random(13);
  
  public VarysMasterServiceHandler() {
    nameToRxBpsMap = new NameToBpsMap();
    nameToTxBpsMap = new NameToBpsMap();
    keepPrintingNetworkStats();
  }

  private void keepPrintingNetworkStats() {
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
  }

  @Override
  public synchronized void putOne(String hostname, MachineStat newMs) throws TException {
    nameToRxBpsMap.updateNetworkInfo(hostname, newMs.rx_bps);
    nameToTxBpsMap.updateNetworkInfo(hostname, newMs.tx_bps);
  }
  
  @Override
  public Map<String, MachineStat> getAll() {
    return null;
  }
  
  @Override
  public synchronized void writeBlock(long blockSize, EndPoint listenFrom) {
    // Decide where to put.
    
    String destSlave = null;
    
    String placementPolicy = VarysCommon.varysProperties.getProperty("varys.placementPolicy", "Random");
    if (placementPolicy.startsWith("Random")) {
      destSlave = nameToRxBpsMap.getRandom(blockSize);
    } else if (placementPolicy.startsWith("NetworkAware")) {
      destSlave = nameToRxBpsMap.getTop(blockSize);
    }
    
    // Start receiver(s)
    assert(destSlave != null);
    
    TTransport transport = null;
    try {
      transport = new TSocket(destSlave, VarysCommon.SLAVE_PORT);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysSlaveService.Client client = new VarysSlaveService.Client(protocol);
      
      client.writeBlock(blockSize, listenFrom);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }
  
  @Override
  public synchronized List<String> getMachines(int numMachines, long avgTxBytes) {
    List<String> retVal = null;
    
    String placementPolicy = VarysCommon.varysProperties.getProperty("varys.placementPolicy", "Random");
    if (placementPolicy.startsWith("Random")) {
      retVal = nameToTxBpsMap.getRandomN(numMachines, avgTxBytes);
    } else if (placementPolicy.startsWith("NetworkAware")) {
      retVal = nameToTxBpsMap.getTopN(numMachines, avgTxBytes);
    }
    
    return retVal;
  }
}
