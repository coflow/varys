package varys;

import java.io.*;
import java.net.*;
import java.util.*;

public class VarysCommon {
  public static final String CONFIG_DIR = "conf";
  
  public static final String MASTERS_FILENAME = CONFIG_DIR + "/masters";
  public static final int MASTER_PORT = 1606;
  
  public static final long SLEEP_INTERVAL_SEC = 1;
  
  public static final String PATH_TO_PROPERTIES_FILE = CONFIG_DIR + "/varys.properties";

  public static String cmdToGetPublicName = "curl http://169.254.169.254/latest/meta-data/public-hostname";

  public static String getMasterHostname() throws Exception{
    FileInputStream fstream = new FileInputStream(MASTERS_FILENAME);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));

    String strLine = br.readLine();
    if (strLine == null) {
      throw new Exception("Master Not Found!");
    }
    in.close();
    
    return strLine;
  }
  
  public static int getMasterPort() {
    return MASTER_PORT;
  }
  
  public static String getLocalHostname() {
    String retVal = null;
    try {
      // retVal = InetAddress.getLocalHost().getHostName();
      retVal = getValueFromCommandLine(cmdToGetPublicName);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retVal;
  }
  
  public static String getValueFromCommandLine(String commandToRun) {
    String retVal = null;
    try {
      ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", commandToRun);
      Process p = pb.start();

      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

      while ((retVal = stdInput.readLine()) != null) {
        break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    // System.out.println(retVal);
    return retVal;
  }
  
  public static Properties loadProperties() {
    Properties properties = new Properties();
    try {
      FileInputStream in = new FileInputStream(PATH_TO_PROPERTIES_FILE);
      properties.load(in);
      in.close();
    } catch (Exception e) {
    }
    return properties;
  }
  
}
