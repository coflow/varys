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

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.NetInterfaceStat;

public class VarysSlave {

  Sigar sigar = null;

  String masterHostname = null;

  double lastRxBytes = 0;
  double lastTxBytes = 0;
  
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

    sigar = new Sigar();
    
    // Set initial values
    initialize();
    
    try {
      Thread.sleep(VarysCommon.HEARTBEAT_INTERVAL_SEC * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void initialize() {
    // Initialize the prev* values
    getMachineStat(true);
    
    
  }
  
  public void start() {
    TTransport transport = null;
    try {
      transport = new TSocket(masterHostname, VarysCommon.MASTER_PORT);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysMasterService.Client client = new VarysMasterService.Client(protocol);
      
      while (true) {
        client.putOne(VarysCommon.getLocalHostname(), getMachineStat(false));
        
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

  private MachineStat getMachineStat(boolean firstTime) {
    double curRxBytes = 0;
    double curTxBytes = 0;
    
    // Collect stats using Sigar
    String[] netIfs = sigar.getNetInterfaceList();
    for (int i = 0; i < netIf.length; i++) {
      NetInterfaceStat net = sigar.getNetInterfaceStat(netIf[i]);
      
      double r = net.getRxBytes();
      curRxBytes += (r >= 0) ? r : 0;
      
      double t = net.getTxBytes();
      curTxBytes += (t >= 0) ? t : 0;
    }
    
    // Collect stats from the command line
    // curRxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetRxBytes));
    // curTxBytes = Double.parseDouble(VarysCommon.getValueFromCommandLine(commandToGetTxBytes));

    double rxBps = 0;
    double txBps = 0;

    if (!firstTime) {
      rxBps = (curRxBytes - lastRxBytes) / VarysCommon.HEARTBEAT_INTERVAL_SEC;
      txBps = (curTxBytes - lastTxBytes) / VarysCommon.HEARTBEAT_INTERVAL_SEC;
    }
    
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
