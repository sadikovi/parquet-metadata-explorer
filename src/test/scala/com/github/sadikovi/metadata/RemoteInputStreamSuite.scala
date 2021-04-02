package com.github.sadikovi.metadata

import java.io.{ByteArrayInputStream, EOFException, IOException, InputStream}

import org.apache.hadoop.fs.{Path, Seekable}

class RemoteInputStreamSuite extends UnitTestSuite {
  /** Creates remote input stream */
  private def remote(arr: Array[Byte], bufferSize: Int) = {
    val in = new TestInputStream(arr)
    new RemoteInputStream(in, bufferSize)
  }

  /** Reads data from input stream and fills up the array */
  private def readFully(in: InputStream, arr: Array[Byte]): Int = {
    var off = 0
    var eof = false
    while (!eof && off < arr.length) {
      val bytes = in.read(arr, off, arr.length - off)
      eof = bytes < 0
      if (bytes >= 0) off += bytes
    }
    off
  }

  test("fail on non-positive buffer size") {
    intercept[AssertionError] {
      remote(new Array[Byte](1), -1)
    }

    intercept[AssertionError] {
      remote(new Array[Byte](1), 0)
    }
  }

  test("negative seek") {
    val in = remote(new Array[Byte](10), 2)
    intercept[IOException] {
      in.seek(-1)
    }

    in.seek(5)
    intercept[IOException] {
      in.seek(1)
    }
  }

  test("no use after close") {
    val in = remote(new Array[Byte](10), 2)
    in.close()

    intercept[AssertionError] {
      in.read()
    }

    intercept[AssertionError] {
      in.read(new Array[Byte](2))
    }

    intercept[AssertionError] {
      in.read(new Array[Byte](2), 0, 2)
    }

    intercept[AssertionError] {
      in.skip(3)
    }

    intercept[AssertionError] {
      in.seek(8)
    }

    intercept[AssertionError] {
      in.getPos()
    }
  }

  test("subsequent close is no-op") {
    val in = remote(new Array[Byte](10), 2)
    in.close()
    in.close()
    in.close()
  }

  test("partial read") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5), 3)
    val res = new Array[Byte](6)
    assert(in.read(res) === 3)
    assert(res === Seq(1, 2, 3, 0, 0, 0))
  }

  test("read fully") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5), 3)
    val res = new Array[Byte](6)
    assert(readFully(in, res) === 5)
    assert(res === Seq(1, 2, 3, 4, 5, 0))
  }

  test("read fully, buffer boundary") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 2)
    val res = new Array[Byte](6)
    assert(readFully(in, res) === 6)
    assert(res === Seq(1, 2, 3, 4, 5, 6))
  }

  test("read fully, single byte buffer") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 1)
    val res = new Array[Byte](6)
    assert(readFully(in, res) === 6)
    assert(res === Seq(1, 2, 3, 4, 5, 6))
  }

  test("read from a file with different buffer sizes") {
    withTempDir { path =>
      val file = new Path(path, "test-file")
      val out = fs.create(file)
      try {
        out.write(new Array[Byte](1 * 1024 * 1024))
      } finally {
        out.close()
      }

      for (size <- Seq(1, 4, 17, 128, 1024, 53472, 1 * 1024 * 1024, 2 * 1024 * 1024)) {
        val in = new RemoteInputStream(fs.open(file), size)
        try {
          assert(readFully(in, new Array[Byte](5 * 1024 * 1024)) === 1 * 1024 * 1024)
        } finally {
          in.close()
        }
      }
    }
  }

  test("readFully") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6, 7), 2)
    val res = new Array[Byte](3)

    in.readFully(res, 0, res.length)
    assert(res === Seq(1, 2, 3))

    in.readFully(res, 0, res.length)
    assert(res === Seq(4, 5, 6))

    in.readFully(res, 0, 1)
    assert(res === Seq(7, 5, 6))
  }

  test("readFully with EOF") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6, 7), 2)
    val res = new Array[Byte](8)

    val err = intercept[EOFException] {
      in.readFully(res, 0, res.length)
    }
    assert(err.getMessage.contains("Failed to read bytes at off 0 and len 8: -1"))
  }

  test("seek within buffer") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 4)
    assert(in.read() === 1)
    in.seek(3)
    assert(in.getPos() === 3)
    assert(in.read() === 4)
    assert(in.getNumRemoteReads === 1)
    assert(in.getNumRemoteSeeks === 0)
  }

  test("seek outside buffer") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 2)
    assert(in.read() === 1)
    in.seek(4)
    assert(in.getPos() === 4)
    assert(in.read() === 5)
    assert(in.getNumRemoteReads === 2)
    assert(in.getNumRemoteSeeks === 1)
  }

  test("seek to the last byte") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 3)
    in.seek(5)
    assert(in.getPos() === 5)
    assert(in.read() === 6)
    assert(in.getPos() === 6)
  }

  test("seek to the end") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 3)
    in.seek(6)
    assert(in.getPos() === 6)
    assert(in.read() === -1)
  }

  test("read + seek") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 4)
    val res = new Array[Byte](4)
    assert(in.read(res, 0, 2) === 2)
    in.seek(4)
    assert(in.read(res, 2, 2) === 2)
    assert(res === Seq(1, 2, 5, 6))
  }

  test("metric numRemoteReads") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 1)
    val res = new Array[Byte](6)
    assert(readFully(in, res) === 6)
    assert(in.getNumRemoteReads === 6)
    assert(in.getNumRemoteSeeks === 0)
  }

  test("metric numRemoteSeeks") {
    val in = remote(Array[Byte](1, 2, 3, 4, 5, 6), 1)
    val res = new Array[Byte](4)
    for (i <- 0 until 6) {
      in.seek(i)
    }
    assert(in.getNumRemoteReads === 0)
    assert(in.getNumRemoteSeeks === 6)
  }
}

class TestInputStream(arr: Array[Byte])
    extends ByteArrayInputStream(arr) with Seekable {
  var numReadCalls: Int = 0
  var numSeekCalls: Int = 0

  override def read(arr: Array[Byte], off: Int, len: Int): Int = {
    numReadCalls += 1
    super.read(arr, off, len)
  }

  override def getPos(): Long = pos

  override def seek(newpos: Long): Unit = {
    numSeekCalls += 1
    super.skip(newpos - pos)
  }

  override def seekToNewSource(newpos: Long): Boolean = false
}
