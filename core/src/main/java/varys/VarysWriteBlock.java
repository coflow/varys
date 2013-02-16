package varys;

import java.io.*;
import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class VarysWriteBlock {

  String masterHostname = null;

  public VarysWriteBlock() {
    // Retrieve master information
    masterHostname = null;
    try {
      masterHostname = VarysCommon.getMasterHostname();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  
  public void writeBlock(long blockSize, EndPoint listenFrom) {
    TTransport transport = null;
    try {
      transport = new TSocket(masterHostname, VarysCommon.MASTER_PORT);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      VarysMasterService.Client client = new VarysMasterService.Client(protocol);
      client.writeBlock(blockSize, listenFrom);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  public static void main(String[] args) {
    
    if (args.length < 3) {
      System.err.println("Usage: VarysWriteBlock <blockSize> <srcHost> <srcPort>");
      System.exit(1);
    }
    
    long blockSize = Long.parseLong(args[0]);
    EndPoint listenFrom = new EndPoint(args[1], Integer.parseInt(args[2]));
    
    VarysWriteBlock wb = new VarysWriteBlock();
    wb.writeBlock(blockSize, listenFrom);
  }
  
}
