#!/usr/local/bin/thrift -java

/**
 * Available types in Thrift:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 */

namespace java varys

struct MachineStat {
  1: double rx_bps,
  2: double tx_bps
}

service VarysService {
  void putOne(1:string hostname, 2:MachineStat machineStat),
  map<string, MachineStat> getAll(),
}

