/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * An RDD partition used to recover checkpointed data.
 */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
 * An RDD that recovers checkpointed data from storage.
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](@transient sc: SparkContext)
  extends RDD[T](sc, Nil) {

<<<<<<< HEAD
  val broadcastedConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

  @transient val fs = new Path(checkpointPath).getFileSystem(sc.hadoopConfiguration)

  override def getPartitions: Array[Partition] = {
    val cpath = new Path(checkpointPath)
    val numPartitions =
    // listStatus can throw exception if path does not exist.
    if (fs.exists(cpath)) {
      val dirContents = fs.listStatus(cpath).map(_.getPath)
      val partitionFiles = dirContents.filter(_.getName.startsWith("part-")).map(_.toString).sorted
      val numPart = partitionFiles.length
      if (numPart > 0 && (! partitionFiles(0).endsWith(CheckpointRDD.splitIdToFile(0)) ||
          ! partitionFiles(numPart-1).endsWith(CheckpointRDD.splitIdToFile(numPart-1)))) {
        throw new SparkException("Invalid checkpoint directory: " + checkpointPath)
      }
      numPart
    } else 0

    Array.tabulate(numPartitions)(i => new CheckpointRDDPartition(i))
  }

  checkpointData = Some(new RDDCheckpointData[T](this))
  checkpointData.get.cpFile = Some(checkpointPath)

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(new Path(checkpointPath,
      CheckpointRDD.splitIdToFile(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, CheckpointRDD.splitIdToFile(split.index))
    CheckpointRDD.readFromFile(file, broadcastedConf, context)
  }

  override def checkpoint() {
    // Do nothing. CheckpointRDD should not be checkpointed.
  }
}

private[spark] object CheckpointRDD extends Logging {
  def splitIdToFile(splitId: Int): String = {
    "part-%05d".format(splitId)
  }

  def writeToFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableWritable[Configuration]],
      blockSize: Int = -1
    )(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    val finalOutputName = splitIdToFile(ctx.partitionId)
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath =
      new Path(outputDir, "." + finalOutputName + "-attempt-" + ctx.attemptNumber)

    if (fs.exists(tempOutputPath)) {
      throw new IOException("Checkpoint failed: temporary path " +
        tempOutputPath + " already exists")
    }
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)

    val fileOutputStream = if (blockSize < 0) {
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    Utils.tryWithSafeFinally {
      serializeStream.writeAll(iterator)
    } {
      serializeStream.close()
    }

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo("Deleting tempOutputPath " + tempOutputPath)
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: "
          + ctx.attemptNumber + " and final output path does not exist")
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo("Final output path " + finalOutputPath + " already exists; not overwriting it")
        fs.delete(tempOutputPath, false)
      }
    }
  }

  def readFromFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableWritable[Configuration]],
      context: TaskContext
    ): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = fs.open(path, bufferSize)
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }
=======
  // CheckpointRDD should not be checkpointed again
  override def doCheckpoint(): Unit = { }
  override def checkpoint(): Unit = { }
  override def localCheckpoint(): this.type = this
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  // Note: There is a bug in MiMa that complains about `AbstractMethodProblem`s in the
  // base [[org.apache.spark.rdd.RDD]] class if we do not override the following methods.
  // scalastyle:off
  protected override def getPartitions: Array[Partition] = ???
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
  // scalastyle:on

}
