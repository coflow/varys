package varys;

import java.io.*;
import java.net.*;

public class VarysSlaveServiceHandler implements VarysSlaveService.Iface {

  public VarysSlaveServiceHandler() {
  }

  @Override
  public void writeBlock(final long blockSize, final EndPoint listenFrom) {
    (new Thread("writeBlock receiver") {
      public void run() {
        try {
          Socket socket = null;
          socket = new Socket(listenFrom.hostname, listenFrom.port);
          InputStream in = socket.getInputStream();
          byte[] buf = new byte[65536];
          long bytesReceived = 0;
          while (bytesReceived < blockSize) {
            long n = in.read(buf);
            if (n == -1) {
              throw new Exception("EOF reached for node " + listenFrom.hostname + " after " + bytesReceived + " bytes");
            } else {
              bytesReceived += n;
            }
          }
          socket.close();
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }).start();
  }
  
}
