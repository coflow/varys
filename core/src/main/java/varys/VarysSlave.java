package varys;

import java.io.*;
import java.util.*;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TThreadPoolServer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysSlave {

  String masterHostname = null;

  double lastRxBytes = -1;
  double lastTxBytes = -1;
  
  String commandToGetRxBytes = null;
  String commandToGetTxBytes = null;
  
  public VarysSlave() {
    // Load properties
    
    commandToGetRxBytes = VarysCommon.varysProperties.getProperty("varys.command.getRxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $7}'");
    commandToGetTxBytes = VarysCommon.varysProperties.getProperty("varys.command.getTxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $10}'");
    
    // Retrieve master information
    masterHostname = null;
    try {
      masterHostname = VarysCommon.getMasterHostname();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    // Set initial values for rxBytes and txBytes
    lastRxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetRxBytes));
    lastTxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetTxBytes));
    
    try {
      Thread.sleep(VarysCommon.HEARTBEAT_INTERVAL_SEC * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
    
  public void start() {
    TTransport transport = null;
    try {
      transport = new TSocket(masterHostname, VarysCommon.MASTER_PORT);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysMasterService.Client client = new VarysMasterService.Client(protocol);
      
      while (true) {
        client.putOne(VarysCommon.getLocalHostname(), getMachineStat());
        
        Thread.sleep(VarysCommon.HEARTBEAT_INTERVAL_SEC * 1000);
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

    double rxBps = (curRxBytes - lastRxBytes) / VarysCommon.HEARTBEAT_INTERVAL_SEC;
    double txBps = (curTxBytes - lastTxBytes) / VarysCommon.HEARTBEAT_INTERVAL_SEC;
    
    lastRxBytes = curRxBytes;
    lastTxBytes = curTxBytes;
    
    return new MachineStat(VarysCommon.getLocalHostname(), rxBps, txBps);
  }
  
  public static void StartThreadedServer(VarysSlaveService.Processor<VarysSlaveServiceHandler> processor) {
    try {
      TServerTransport serverTransport = new TServerSocket(VarysCommon.SLAVE_PORT);
      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    
    // First start the Thrift Server
    (new Thread("Thrift Server @ Slave:" + VarysCommon.getLocalHostname()) {
      public void run() {
        VarysSlaveServiceHandler varysHandler = new VarysSlaveServiceHandler();
        StartThreadedServer(new VarysSlaveService.Processor<VarysSlaveServiceHandler>(varysHandler));
      }
    }).start();
    
    // Do normal business
    VarysSlave vc = new VarysSlave();
    vc.start();
  }
  
}
