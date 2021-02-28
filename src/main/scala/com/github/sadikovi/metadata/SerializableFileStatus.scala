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
    case dbr => invoke[Path](dbr, "getPath")
  }

  def getLen(): Long = status match {
    case oss: FileStatus => oss.getLen()
    case dbr => invoke[Long](dbr, "getLen")
  }

  def getModificationTime(): Long = status match {
    case oss: FileStatus => oss.getModificationTime()
    case dbr => invoke[Long](dbr, "getModificationTime")
  }
}

object SerializableFileStatus {
  /** Calls method with no arguments on the object */
  def invoke[T](obj: Any, methodName: String): T = {
    val clazz = obj.getClass
    try {
      val method = clazz.getDeclaredMethod(methodName)
      method.setAccessible(true)
      method.invoke(obj).asInstanceOf[T]
    } catch {
      case err: Exception =>
        throw new RuntimeException(
          s"Failed to invoke method '${methodName}()' in ${clazz}, reason: $err", err)
    }
  }
}
