package litchiware.spark.input

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

class VariableLengthBinaryRecordReader
  extends RecordReader[NullWritable, BytesWritable] {

  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var fileInputStream: FSDataInputStream = null
  private val recordKey: NullWritable = null
  private var recordValue: BytesWritable = null

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: NullWritable = {
    recordKey
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = splitStart + fileSplit.getLength

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // job configuration
    val conf = context.getConfiguration
    // check compression
    val codec = new CompressionCodecFactory(conf).getCodec(file)
    if (codec != null) {
      throw new IOException("VariableLengthRecordReader does not support reading compressed files")
    }
    // get the filesystem
    val fs = file.getFileSystem(conf)
    // open the File
    fileInputStream = fs.open(file)
    // seek to the splitStart position
    fileInputStream.seek(splitStart)
    // set our current position
    currentPosition = splitStart
  }

  override def nextKeyValue(): Boolean = {
    if (currentPosition < splitEnd) {
      // setup a buffer to store the record
      val lengthBuffer = new Array[Byte](4)
      fileInputStream.readFully(lengthBuffer)
      val byteBuf = ByteBuffer.wrap(lengthBuffer)
      byteBuf.order(ByteOrder.LITTLE_ENDIAN)
      val recordLength = byteBuf.getInt
      recordValue = new BytesWritable(new Array[Byte](recordLength))
      val recordBuffer = recordValue.getBytes
      fileInputStream.readFully(recordBuffer)
      // update our current position
      currentPosition = currentPosition + 4 + recordLength
      // return true
      return true
    }
    false
  }
}
