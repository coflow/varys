package varys;

import java.io.*;
import java.net.*;
import java.util.*;

public class VarysCommon {
  public static final String cmdToGetPublicName = "curl http://169.254.169.254/latest/meta-data/public-hostname";
  
  public static final String MASTER_IP = (System.getenv("VARYS_MASTER_IP") != null) ? System.getenv("VARYS_MASTER_IP") : getValueFromCommandLine(cmdToGetPublicName); // InetAddress.getLocalHost().getHostName();

  public static final int MASTER_PORT = (System.getenv("VARYS_MASTER_PORT") != null) ? Integer.parseInt(System.getenv("VARYS_MASTER_PORT")) : 1606;

  public static final int SLAVE_PORT = (System.getenv("VARYS_SLAVE_PORT") != null) ? Integer.parseInt(System.getenv("VARYS_SLAVE_PORT")) : 1607;

  public static final int WEBUI_PORT = (System.getenv("SPARK_MASTER_WEBUI_PORT") != null) ? Integer.parseInt(System.getenv("VARYS_MASTER_WEBUI_PORT")) : 16016;
    
  public static final String CONFIG_DIR = (System.getenv("VARYS_CONF_DIR") != null) ? System.getenv("VARYS_CONF_DIR") : "conf";

  public static final String MASTERS_FILENAME = CONFIG_DIR + "/masters";
    
  public static final long HEARTBEAT_INTERVAL_SEC = 1;
  
  public static final String PATH_TO_PROPERTIES_FILE = CONFIG_DIR + "/varys.properties";
  
  public static final Properties varysProperties = loadProperties();

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
  
  private static Properties loadProperties() {
    Properties properties = new Properties();
    try {
      FileInputStream in = new FileInputStream(PATH_TO_PROPERTIES_FILE);
      properties.load(in);
      in.close();
    } catch (Exception e) {
    }
    return properties;
  }
  
  public static double sum(double[] array) {
    double sum = 0.0;
    for (int i = 0; i < array.length; i++) {
      sum += array[i];
    }
    return sum;
  }
  
  public static double average(double[] arr) {
    return sum(arr) /arr.length;
  }
  
  public static double stdev(double[] arr) {
    double std = 0.0;
    double avg = average(arr);
    for (double d: arr) {
      std += (d * d);
    }
    return Math.sqrt((std / arr.length) - (avg * avg));
  }
  
  public static double covar(double[] arr) {
    double avg = average(arr);
    return (avg > 0) ? (stdev(arr) / average(arr)) : 0;
  }
}
