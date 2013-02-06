package varys;

import java.io.*;
import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysGetMachines {

  String masterURL = "";
  int masterPort = -1;
  String masterHostname = null;

  public VarysGetMachines(String masterURL) {
    // Load properties
    Properties props = VarysCommon.loadProperties();

    // Retrieve master information
    masterHostname = VarysCommon.getMasterHostname(masterURL);
    masterPort = VarysCommon.getMasterPort(masterURL);
  }
  
  public List<String> getMachines(int numMachines, double avgTxBytes) {
    TTransport transport = null;
    try {
      transport = new TSocket(masterHostname, masterPort);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysService.Client client = new VarysService.Client(protocol);
      
      return client.getMachines(numMachines, avgTxBytes);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
    return null;
  }

  public static void main(String[] args) {
    
    if (args.length < 3) {
      System.err.println("Usage: VarysGetMachines <masterURL> <numMachines> <avgTxMegaBytes>");
      System.exit(1);
    }
    
    String masterURL = args[0];
    int numMachines = Integer.parseInt(args[1]);
    double avgTxBytes = Double.parseDouble(args[2]) * 1024.0 * 1024.0;
    
    VarysGetMachines gm = new VarysGetMachines(masterURL);
    List<String> machines = gm.getMachines(numMachines, avgTxBytes);
    
    System.out.print("X");
    for (int i = 0; i < machines.size(); i++) {
      System.out.print(machines.get(i));
      if (i + 1 < machines.size()) {
        System.out.print("/");
      }
    }
    System.out.println("X");
  }
  
}
