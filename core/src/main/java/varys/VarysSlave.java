package varys;

import java.io.*;
import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysSlave {

  String masterURL = "";
  String masterHostname = null;
  int masterPort = -1;

  double lastRxBytes = -1;
  double lastTxBytes = -1;
  
  String commandToGetRxBytes = null;
  String commandToGetTxBytes = null;
  
  public VarysSlave(String masterURL) {
    // Load properties
    Properties props = VarysCommon.loadProperties();

    commandToGetRxBytes = props.getProperty("varys.command.getRxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $7}'");
    commandToGetTxBytes = props.getProperty("varys.command.getTxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $10}'");
    
    // Retrieve master information
    masterHostname = VarysCommon.getMasterHostname(masterURL);
    masterPort = VarysCommon.getMasterPort(masterURL);
    
    // Set initial values for rxBytes and txBytes
    lastRxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetRxBytes));
    lastTxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetTxBytes));
    
    try {
      Thread.sleep(VarysCommon.SLEEP_INTERVAL_SEC * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
    
  public void start() {
    TTransport transport = null;
    try {
      transport = new TSocket(masterHostname, masterPort);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysService.Client client = new VarysService.Client(protocol);
      
      while (true) {
        client.putOne(VarysCommon.getLocalHostname(), getMachineStat());
        // System.out.println(client.getAll());
        
        Thread.sleep(VarysCommon.SLEEP_INTERVAL_SEC * 1000);
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  private MachineStat getMachineStat() {
    double curRxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetRxBytes));
    double curTxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetTxBytes));

    double rxBps = (curRxBytes - lastRxBytes) / VarysCommon.SLEEP_INTERVAL_SEC;
    double txBps = (curTxBytes - lastTxBytes) / VarysCommon.SLEEP_INTERVAL_SEC;
    
    lastRxBytes = curRxBytes;
    lastTxBytes = curTxBytes;
    
    return new MachineStat(VarysCommon.getLocalHostname(), rxBps, txBps);
  }
  
  public static void main(String[] args) {
    
    // Check for master URL
    if (args.length == 0) {
      System.err.println("Missing master URL.");
      System.exit(1);
    }
    String masterURL = args[0];
    VarysSlave vc = new VarysSlave(masterURL);
    vc.start();
  }
  
}
