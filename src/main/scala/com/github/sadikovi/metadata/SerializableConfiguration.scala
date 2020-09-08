package com.github.sadikovi.metadata

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/**
 * Workaround for serializable Hadoop configuration.
 * Copied from Apache Spark repository.
 */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}
