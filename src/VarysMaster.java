package varys;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TThreadPoolServer;

public class VarysMaster {

  public static void StartThreadedServer(VarysMasterService.Processor<VarysMasterServiceHandler> processor) {
    try {
      TServerTransport serverTransport = new TServerSocket(VarysCommon.MASTER_PORT);
      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    VarysMasterServiceHandler varysHandler = new VarysMasterServiceHandler();
    StartThreadedServer(new VarysMasterService.Processor<VarysMasterServiceHandler>(varysHandler));
  }
  
}
