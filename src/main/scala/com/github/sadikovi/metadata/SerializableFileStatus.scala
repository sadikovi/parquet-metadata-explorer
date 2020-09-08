package com.github.sadikovi.metadata

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * Hack to extract values regardless of FileStatus class.
 * This is done to make the code work in OSS and Databricks.
 */
class SerializableFileStatus(@transient val status: Any) {
  import SerializableFileStatus._

  def getPath(): Path = status match {
    case oss: FileStatus => oss.getPath()
    case dbr if isStatus(dbr) => invoke[Path](dbr, "getPath")
  }

  def getLen(): Long = status match {
    case oss: FileStatus => oss.getLen()
    case dbr if isStatus(dbr) => invoke[Long](dbr, "getLen")
  }

  def getModificationTime(): Long = status match {
    case oss: FileStatus => oss.getModificationTime()
    case dbr if isStatus(dbr) => invoke[Long](dbr, "getModificationTime")
  }
}

object SerializableFileStatus {
  /** Returns true, if object is a FileSTatus or SerializableFileStatus */
  def isStatus(obj: Any): Boolean = {
    obj.getClass.getName.contains("FileStatus")
  }

  /** Calls method with no arguments on the object */
  def invoke[T](obj: Any, method: String): T = {
    val m = obj.getClass.getDeclaredMethod(method)
    m.setAccessible(true)
    m.invoke(obj).asInstanceOf[T]
  }
}
