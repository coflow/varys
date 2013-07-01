/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package varys.util

import java.io.IOException
import java.io.InputStream

import varys.Logging

/**
 * The ThrottleInputStream provides bandwidth throttling on a specified
 * InputStream. It is implemented as a wrapper on top of another InputStream
 * instance.
 * The throttling works by examining the number of bytes read from the underlying
 * InputStream from the beginning, and sleep()ing for a time interval if
 * the byte-transfer is found exceed the specified tolerable maximum.
 * (Thus, while the read-rate might exceed the maximum for a given short interval,
 * the average tends towards the specified maximum, overall.)
 */
private[varys] class ThrottledInputStream(
    val rawStream: InputStream,
    val ownerName: String,
    val initBitPerSec: Double = 0.0)
  extends InputStream() with Logging {

  val startTime = System.currentTimeMillis()

  val mBPSLock = new Object

  var maxBytesPerSec = (initBitPerSec / 8).toLong
  var bytesRead = 0L
  var totalSleepTime = 0L

  val SLEEP_DURATION_MS = 50L

  if (maxBytesPerSec < 0) {
    throw new IOException("Bandwidth " + maxBytesPerSec + " is invalid")
  }
  
  override def read(): Int = {
    throttle()
    val data = rawStream.read()
    if (data != -1) {
      bytesRead += 1
    }
    data
  }

  override def read(b: Array[Byte]): Int = {
    throttle()
    val readLen = rawStream.read(b)
    if (readLen != -1) {
      bytesRead += readLen
    }
    readLen
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    throttle()
    val readLen = rawStream.read(b, off, len)
    if (readLen != -1) {
      bytesRead += readLen
    }
    readLen
  }

  private def throttle() {
    while (maxBytesPerSec <= 0.0) {
      mBPSLock.synchronized {
        logTrace(this + " maxBytesPerSec <= 0.0. Sleeping.")
        mBPSLock.wait()
      }
    }

    // NEVER exceed the specified rate
    while (getBytesPerSec > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS)
        totalSleepTime += SLEEP_DURATION_MS
      } catch {
        case ie: InterruptedException => throw new IOException("Thread aborted", ie)
      }
    }
  }

  def setNewRate(newMaxBitPerSec: Double) {
    maxBytesPerSec = (newMaxBitPerSec / 8).toLong
    mBPSLock.synchronized {
      logTrace(this + " newMaxBitPerSec = " + newMaxBitPerSec)
      mBPSLock.notifyAll()
    }
  }

  def getTotalBytesRead() = bytesRead

  def getBytesPerSec(): Long = {
    val elapsed = (System.currentTimeMillis() - startTime) / 1000
    if (elapsed == 0) {
      bytesRead 
    } else {
      bytesRead / elapsed
    }
  }

  def getTotalSleepTime() = totalSleepTime

  override def toString(): String = {
    "ThrottledInputStream{" +
      "ownerName=" + ownerName +
      ", bytesRead=" + bytesRead +
      ", maxBytesPerSec=" + maxBytesPerSec +
      ", bytesPerSec=" + getBytesPerSec +
      ", totalSleepTime=" + totalSleepTime +
      '}';
  }
}