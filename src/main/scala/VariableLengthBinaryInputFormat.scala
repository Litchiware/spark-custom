package litchiware.spark.input

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

object VariableLengthBinaryInputFormat {
}

class VariableLengthBinaryInputFormat
  extends FileInputFormat[NullWritable, BytesWritable] {

  /**
    * Override of isSplitable to ensure initial computation of the record length
    */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    false
  }

  /**
    * Create a VariableLengthBinaryRecordReader
    */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[NullWritable, BytesWritable] = {
    new VariableLengthBinaryRecordReader
  }
}
