package com.github.sadikovi.metadata

import org.apache.hadoop.fs.{FileStatus, Path}

class SerializableFileStatusSuite extends UnitTestSuite {
  test("invoke DBR methods") {
    class MyFileStatus {
      def getLen: Long = 123
      def getModificationTime: Long = 1000
      def getPath: Path = new Path("/test")
    }

    val status = new SerializableFileStatus(new MyFileStatus())
    assert(status.getPath === new Path("/test"))
    assert(status.getLen === 123)
    assert(status.getModificationTime === 1000)
  }

  test("invoke OSS methods") {
    val status = new SerializableFileStatus(
      new FileStatus(123, false, 0, 0, 1000, new Path("/test")))
    assert(status.getPath === new Path("/test"))
    assert(status.getLen === 123)
    assert(status.getModificationTime === 1000)
  }

  test("fail to invoke a method") {
    class MyFileStatus { }
    val status = new SerializableFileStatus(new MyFileStatus())

    var err = intercept[RuntimeException] { status.getPath }
    assert(err.getMessage.contains("Failed to invoke method 'getPath()'"))

    err = intercept[RuntimeException] { status.getLen }
    assert(err.getMessage.contains("Failed to invoke method 'getLen()'"))

    err = intercept[RuntimeException] { status.getModificationTime }
    assert(err.getMessage.contains("Failed to invoke method 'getModificationTime()'"))
  }
}
