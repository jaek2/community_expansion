/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

import com.google.common.base.Strings;

/**
 * The UDF is useful for splitting the output data into a bunch of directories
 * and files dynamically based on user specified key field in the output tuple.
 *
 * Sample usage: <code>
 * A = LOAD 'mydata' USING PigStorage() as (a, b, c);
 * STORE A INTO '/my/home/output' USING MultiStorage('/my/home/output','0', 'bz2', '\\t');
 * </code> Parameter details:- ========== <b>/my/home/output </b>(Required) :
 * The DFS path where output directories and files will be created. <b> 0
 * </b>(Required) : Index of field whose values should be used to create
 * directories and files( field 'a' in this case). <b>'bz2' </b>(Optional) : The
 * compression type. Default is 'none'. Supported types are:- 'none', 'gz' and
 * 'bz2' <b> '\\t' </b>(Optional) : Output field separator.
 *
 * Let 'a1', 'a2' be the unique values of field 'a'. Then output may look like
 * this
 *
 * /my/home/output/a1/a1-0000 /my/home/output/a1/a1-0001
 * /my/home/output/a1/a1-0002 ... /my/home/output/a2/a2-0000
 * /my/home/output/a2/a2-0001 /my/home/output/a2/a2-0002
 *
 * The prefix '0000*' is the task-id of the mapper/reducer task executing this
 * store. In case user does a GROUP BY on the field followed by MultiStorage(),
 * then its imperative that all tuples for a particular group will go exactly to
 * 1 reducer. So in the above case for e.g. there will be only 1 file each under
 * 'a1' and 'a2' directories.
 *
 * If the output is compressed,then the sub directories and the output files will
 * be having the extension. Say for example in the above case if bz2 is used one file
 * will look like ;/my/home/output.bz2/a1.bz2/a1-0000.bz2
 *
 * Key field can also be a comma separated list of indices e.g. '0,1' - in this case
 * storage will be multi-level:
 * /my/home/output/a1/b1/a1-b1-0000
 * /my/home/output/a1/b2/a1-b2-0000
 * There is also an option to leave key values out of storage, see isRemoveKeys.
 */
public class MultiStorage extends StoreFunc {

  private static final String KEYFIELD_DELIMETER = ",";
  private Path outputPath; // User specified output Path
  private final List<Integer> splitFieldIndices= new ArrayList<Integer>(); // Indices of the key fields
  private String fieldDel; // delimiter of the output record.
  private Compression comp; // Compression type of output data.
  private boolean isRemoveKeys = false;

  // Compression types supported by this store
  enum Compression {
    none, bz2, bz, gz;
  };

  public MultiStorage(String parentPathStr, String splitFieldIndex) {
    this(parentPathStr, splitFieldIndex, "none");
  }

  public MultiStorage(String parentPathStr, String splitFieldIndex,
      String compression) {
    this(parentPathStr, splitFieldIndex, compression, "\\t");
  }

  public MultiStorage(String parentPathStr, String splitFieldIndex,
      String compression, String fieldDel) {
    this(parentPathStr, splitFieldIndex, compression, fieldDel, "false");
  }

  /**
   * Constructor
   *
   * @param parentPathStr
   *          Parent output dir path (this will be specified in store statement,
   *            so MultiStorage don't use this parameter in reality. However, we don't
   *            want to change the construct to break backward compatibility)
   * @param splitFieldIndex
   *          key field index
   * @param compression
   *          'bz2', 'bz', 'gz' or 'none'
   * @param fieldDel
   *          Output record field delimiter.
   * @param isRemoveKeys
   *          Removes key columns from result during write.
   */
  public MultiStorage(String parentPathStr, String splitFieldIndex,
                      String compression, String fieldDel, String isRemoveKeys) {
    this.isRemoveKeys = Boolean.parseBoolean(isRemoveKeys);
    this.outputPath = new Path(parentPathStr);

    String[] splitFieldIndices = splitFieldIndex.split(KEYFIELD_DELIMETER);
    for (String splitFieldIndexString : splitFieldIndices){
      this.splitFieldIndices.add(Integer.parseInt(splitFieldIndexString));
    }

    this.fieldDel = fieldDel;
    try {
      this.comp = (compression == null) ? Compression.none : Compression
              .valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Exception when converting compression string: "
              + compression + " to enum. No compression will be used");
      this.comp = Compression.none;
    }
  }

  //--------------------------------------------------------------------------
  // Implementation of StoreFunc

  private RecordWriter<List<String>, Tuple> writer;

  @Override
  public void putNext(Tuple tuple) throws IOException {
    for (int splitFieldIndex : this.splitFieldIndices) {
      if (tuple.size() <= splitFieldIndex) {
        throw new IOException("split field index:" + splitFieldIndex
                + " >= tuple size:" + tuple.size());
      }
    }
    List<String> fields = new ArrayList<String>();
    for (int splitFieldIndex : this.splitFieldIndices){
      try {
        fields.add(String.valueOf(tuple.get(splitFieldIndex)));
      } catch (ExecException exec) {
        throw new IOException(exec);
      }
    }
    try {
      writer.write(fields, tuple);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
      MultiStorageOutputFormat format = new MultiStorageOutputFormat();
      format.setKeyValueSeparator(fieldDel);
      if (this.isRemoveKeys){
        format.setSkipIndices(this.splitFieldIndices);
      }
      return format;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
      this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set(MRConfiguration.TEXTOUTPUTFORMAT_SEPARATOR, "");
    FileOutputFormat.setOutputPath(job, new Path(location));
    if (comp == Compression.bz2 || comp == Compression.bz) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
    } else if (comp == Compression.gz) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    }
  }

  @Override
  public Boolean supportsParallelWriteToStoreLocation() {
    return false;
  }

  //--------------------------------------------------------------------------
  // Implementation of OutputFormat

  public static class MultiStorageOutputFormat extends
  TextOutputFormat<List<String>, Tuple> {

    private String keyValueSeparator = "\\t";
    private byte fieldDel = '\t';
    private List<Integer> skipIndices = null;

    @Override
    public RecordWriter<List<String>, Tuple>
    getRecordWriter(TaskAttemptContext context
                ) throws IOException, InterruptedException {

      final TaskAttemptContext ctx = context;

      return new RecordWriter<List<String>, Tuple>() {

        private Map<List<String>, MyLineRecordWriter> storeMap =
              new HashMap<List<String>, MyLineRecordWriter>();

        private static final int BUFFER_SIZE = 1024;

        private ByteArrayOutputStream mOut =
              new ByteArrayOutputStream(BUFFER_SIZE);

        @Override
        public void write(List<String> key, Tuple val) throws IOException {
          int sz = val.size();
          for (int i = 0; i < sz; i++) {
            Object field;
            try {
              field = val.get(i);
            } catch (ExecException ee) {
              throw ee;
            }

            boolean skipCurrentField = skipIndices != null && skipIndices.contains(i);

            if (!skipCurrentField) {
              StorageUtil.putField(mOut, field);
            }

            if (i != sz - 1 && !skipCurrentField) {
              mOut.write(fieldDel);
            }
          }

          getStore(key).write(null, new Text(mOut.toByteArray()));

          mOut.reset();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
          for (MyLineRecordWriter out : storeMap.values()) {
            out.close(context);
          }
        }

        private MyLineRecordWriter getStore(List<String> fieldValues) throws IOException {
          MyLineRecordWriter store = storeMap.get(fieldValues);
          if (store == null) {
            DataOutputStream os = createOutputStream(fieldValues);
            store = new MyLineRecordWriter(os, keyValueSeparator);
            storeMap.put(fieldValues, store);
          }
          return store;
        }

        private DataOutputStream createOutputStream(List<String> fieldValues) throws IOException {
          Configuration conf = ctx.getConfiguration();
          TaskID taskId = ctx.getTaskAttemptID().getTaskID();

          // Check whether compression is enabled, if so get the extension and add them to the path
          boolean isCompressed = getCompressOutput(ctx);
          CompressionCodec codec = null;
          String extension = "";
          if (isCompressed) {
             Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(ctx, GzipCodec.class);
             codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
             extension = codec.getDefaultExtension();
          }

          NumberFormat nf = NumberFormat.getInstance();
          nf.setMinimumIntegerDigits(4);

          StringBuffer pathStringBuffer = new StringBuffer();
          for (String fieldValue : fieldValues){
            String safeFieldValue = fieldValue.replaceAll("\\/","-");
            pathStringBuffer.append(safeFieldValue);
            pathStringBuffer.append("/");
          }
          pathStringBuffer.deleteCharAt(pathStringBuffer.length()-1);
          String pathString = pathStringBuffer.toString();
          String idString = pathString.replaceAll("\\/","-");

          if (!Strings.isNullOrEmpty(extension)){
            pathString = pathString.replaceAll("\\/",extension+"\\/");
          }

          Path path = new Path(pathString+extension, idString + '-'
                + nf.format(taskId.getId())+extension);
          Path workOutputPath = ((FileOutputCommitter)getOutputCommitter(ctx)).getWorkPath();
          Path file = new Path(workOutputPath, path);
          FileSystem fs = file.getFileSystem(conf);
          FSDataOutputStream fileOut = fs.create(file, false);

          if (isCompressed)
             return new DataOutputStream(codec.createOutputStream(fileOut));
          else
             return fileOut;
        }

      };
    }

    public void setKeyValueSeparator(String sep) {
      keyValueSeparator = sep;
      fieldDel = StorageUtil.parseFieldDel(keyValueSeparator);
    }

    public void setSkipIndices(List<Integer> skipIndices) {
      this.skipIndices = skipIndices;
    }

    //------------------------------------------------------------------------
  //

    protected static class MyLineRecordWriter
    extends TextOutputFormat.LineRecordWriter<WritableComparable, Text> {

      public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
        super(out, keyValueSeparator);
      }
    }
  }

}
