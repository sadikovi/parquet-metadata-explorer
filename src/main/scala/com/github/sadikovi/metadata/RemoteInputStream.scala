package com.github.sadikovi.metadata

import java.io.{EOFException, IOException, InputStream}
import org.apache.hadoop.fs.Seekable

/**
 * Buffered remote input stream that buffers large chunks of data from remote file systems or
 * blob stores to reduce the amount of slow and expensive requests.
 *
 * The stream is designed for sequential reads and it is encouraged to provide a large buffer size
 * in order of tens or hundreds of megabytes, e.g. 128 MB (typical parquet row group size)
 *
 * The input stream takes ownership and management of the passed `in` parent stream
 * (also referred to as "remote" stream).
 */
class RemoteInputStream(
    private var in: InputStream with Seekable,
    val bufferSize: Int) extends InputStream with Seekable {
  assert(in != null, "Input stream is null")
  assert(bufferSize > 0, s"Expected positive buffer size, found $bufferSize")

  // Whether or not the stream is closed, if it is, any method calls with result in error
  private var isClosed: Boolean = false
  // Global position in the stream
  private var pos: Long = 0
  // Temporary array for reading 1 byte of data
  private val tmp = new Array[Byte](1)
  // Temporary buffer for reading from remote
  private var buf = new Array[Byte](bufferSize)
  // Temporary buffer offset
  private var bufoff: Int = 0
  // Temporary buffer length
  private var buflen: Int = 0

  // Metrics
  private var numRemoteReads: Int = 0
  private var numRemoteSeeks: Int = 0

  override def read(): Int = if (read(tmp) > 0) tmp(0) else -1

  override def read(arr: Array[Byte]): Int = read(arr, 0, arr.length)

  override def read(arr: Array[Byte], off: Int, len: Int): Int = {
    assertOpen()
    if (getBufLen() == 0 && fillBuf()) return -1
    val bytesRead = copyFromBuf(arr, off, len)
    pos += bytesRead
    bytesRead
  }

  /** Naive implementation of readFully method */
  def readFully(arr: Array[Byte], off: Int, len: Int): Unit = {
    var bytesRead = len
    var currOffset = off
    while (bytesRead > 0) {
      val bytes = read(arr, currOffset, bytesRead)
      if (bytes < 0) {
        throw new EOFException(s"Failed to read bytes at off $off and len $len: $bytes")
      }
      bytesRead -= bytes
      currOffset += bytes
    }
  }

  override def skip(bytes: Long): Long = {
    assertOpen()
    seek(pos + bytes)
    bytes
  }

  override def close(): Unit = {
    super.close()
    isClosed = true
    if (in != null) {
      in.close()
      in = null
    }
    buf = null
  }

  // Seekable API

  override def getPos(): Long = {
    assertOpen()
    pos
  }

  /** Seeks to a new position `newpos`. Backward seeks are not allowed */
  override def seek(newpos: Long): Unit = {
    assertOpen()
    if (newpos - pos < 0) throw new IOException("Cannot seek backwards")
    if (newpos - pos < getBufLen()) {
      skipBuf((newpos - pos).toInt)
    } else {
      seekRemote(newpos)
      skipBuf(getBufLen()) // resets the buffer
    }
    pos = newpos
  }

  override def seekToNewSource(newpos: Long): Boolean = {
    false // not implemented
  }

  /** Checks if the input stream is open */
  private def assertOpen(): Unit = {
    if (isClosed) throw new AssertionError("Input stream is closed")
  }

  // Remote functions

  /**
   * Reads bytes from the remote stream and returns the amount of bytes read.
   * Has similar semantics with `InputStream::read(arr, off, len)` method.
   */
  private def readRemote(arr: Array[Byte], off: Int, len: Int): Int = {
    numRemoteReads += 1
    in.read(arr, off, len)
  }

  /**
   * Seeks within the remote stream.
   * Has similar semantics as `Seekable::seek(pos)`.
   */
  private def seekRemote(pos: Long): Unit = {
    numRemoteSeeks += 1
    in.seek(pos)
  }

  // Metrics

  /** Metric for the number of remote reads */
  def getNumRemoteReads(): Int = numRemoteReads

  /** Metric for the number of remote seeks */
  def getNumRemoteSeeks(): Int = numRemoteSeeks

  // Temporary buffer internal methods

  /** Returns the number of remaining bytes in the buffer */
  private def getBufLen(): Int = {
    if (bufoff >= buflen) 0 else (buflen - bufoff)
  }

  /** Fills the buffer */
  private def fillBuf(): Boolean = {
    bufoff = 0
    buflen = readRemote(buf, 0, buf.length) // can return -1 to indicate EOF
    buflen < 0
  }

  /** Copies bytes from buffer into the array, can copy fewer bytes */
  private def copyFromBuf(arr: Array[Byte], off: Int, len: Int): Int = {
    val bytesToCopy = Math.min(len, getBufLen())
    System.arraycopy(buf, bufoff, arr, off, bytesToCopy)
    bufoff += bytesToCopy
    bytesToCopy
  }

  /** Skips the provided len bytes in the buffer */
  private def skipBuf(len: Int): Unit = {
    if (len > getBufLen()) {
      throw new IOException("Not enough bytes to skip, " +
        s"bufoff: $bufoff, len: $len, buflen: $buflen")
    }
    bufoff += len
  }
}
