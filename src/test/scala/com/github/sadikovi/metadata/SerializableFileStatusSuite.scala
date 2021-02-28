package com.github.sadikovi.metadata

import org.apache.hadoop.fs.Path

class SerializableFileStatusSuite extends UnitTestSuite {
  test("SerializableFileStatus, isStatus") {
    class MyFileStatus { }
    class SerializedFileStatus { }
    class MyStatus { }
    assert(SerializableFileStatus.isStatus(new MyFileStatus()))
    assert(SerializableFileStatus.isStatus(new SerializedFileStatus()))
    assert(!SerializableFileStatus.isStatus(new MyStatus()))
  }

  test("SerializedFileStatus, call methods") {
    class MyFileStatus {
      def getLen: Long = 1
      def getModificationTime: Long = 2
      def getPath: Path = new Path("/test")
    }

    val status = new SerializableFileStatus(new MyFileStatus())
    assert(status.getPath === new Path("/test"))
    assert(status.getLen === 1)
    assert(status.getModificationTime === 2)
  }
}
