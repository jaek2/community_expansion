import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
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
public class VWMultiStorage extends StoreFunc {

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

  public VWMultiStorage(String parentPathStr, String splitFieldIndex) {
    this(parentPathStr, splitFieldIndex, "none");
  }

  public VWMultiStorage(String parentPathStr, String splitFieldIndex,
      String compression) {
    this(parentPathStr, splitFieldIndex, compression, "\\t");
  }

  public VWMultiStorage(String parentPathStr, String splitFieldIndex,
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
  public VWMultiStorage(String parentPathStr, String splitFieldIndex,
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

        /*
         * This write function is customized to store output spefically for my pig script
         */
        @Override
        public void write(List<String> key, Tuple val) throws IOException {
          String UTF8 = "UTF-8";
          // I actually know that the size is 8 and we have to skip the very last one
          // demo, device, app_activities, geo, keywords, yamp_events.clicks, mx3_events.mx3_click_events, TOP_SEGMENTS::id

          // default label and importance
          int label = -1;
          long importance = 1;

          // get the click numbers
          long num_click = 0;

          Object field = val.get(7);
          if (DataType.findType(field) != DataType.NULL){
            num_click = (Long)field;
          }

          // update if there is any click
          if (num_click != 0){
            importance = num_click;
            label = 1;
          }

          mOut.write(((Integer)label).toString().getBytes());
          mOut.write(' ');
          mOut.write(((Long)importance).toString().getBytes());

          /* Start extracting data from other fields */
          /* note the limited size of the buffer*/

          // demo tuple
          field = val.get(0);
          if (DataType.findType(field) != DataType.NULL){
            String namespace = new String(" |demo");
            mOut.write(namespace.getBytes(UTF8));

            Tuple demo_t = (Tuple)field;

            // gender
            Object temp_field = demo_t.get(0);
            if (DataType.findType(temp_field) != DataType.NULL){
              String gender = (String)temp_field;

              if (!gender.equals("UNKNOWN") && !gender.equals("NULL")){
                mOut.write(' ');
                mOut.write(gender.getBytes(UTF8));
              }
            }

            // age_bucket
            temp_field = demo_t.get(3);
            if (DataType.findType(temp_field) != DataType.NULL){
              String age = (String)temp_field;

              if (!age.equals("Unknown") && !age.equals("NULL")){
                mOut.write(' ');
                mOut.write(age.getBytes(UTF8));
              }
            }
          }

          // device tuple
          field = val.get(1);
          if (DataType.findType(field) != DataType.NULL){
            String namespace = new String(" |device");
            mOut.write(namespace.getBytes(UTF8));

            Tuple device_t = (Tuple)field;

            // extract mobile tuple
            Object temp_field = device_t.get(0);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple mobile_t = (Tuple)temp_field;

              // select model info
              Object temp_model = mobile_t.get(4);
              if (DataType.findType(temp_model) != DataType.NULL){
                String model = (String)temp_model;
                model = model.replaceAll("\\s+", "_");

                mOut.write(' ');
                mOut.write(model.getBytes(UTF8));
              }
            }

            // extract desktop bag
            temp_field = device_t.get(1);
            if (DataType.findType(temp_field) != DataType.NULL){
              DataBag desktop_bag = (DataBag)temp_field;
              Set<String> desktop_os = new HashSet<String>();
              Set<String> desktop_browser = new HashSet<String>();

              for (Tuple descktop_t : desktop_bag){
                Object temp_os = descktop_t.get(0);
                if (DataType.findType(temp_os) != DataType.NULL){
                  String os = (String)temp_os;
                  desktop_os.add(os);
                }

                Object temp_browser = descktop_t.get(2);
                if (DataType.findType(temp_browser) != DataType.NULL){
                  String browser = (String)temp_browser;
                  desktop_browser.add(browser);
                }
              }

              for (String os : desktop_os){
                mOut.write(' ');
                mOut.write(os.getBytes(UTF8));
              }

              for (String browser : desktop_browser){
                mOut.write(' ');
                mOut.write(browser.getBytes(UTF8));
              }
            }
          }

          // app_activities
          field = val.get(2);
          if (DataType.findType(field) != DataType.NULL){
            String namespace = new String(" |app_activities");
            mOut.write(namespace.getBytes(UTF8));

            DataBag app_bag = (DataBag)field;
            Set<String> app_category = new HashSet<String>();

            for (Tuple app_t : app_bag){
              Object temp_category = app_t.get(4);
              if (DataType.findType(temp_category) != DataType.NULL){
                String category = (String)temp_category;
                category = category.replaceAll("\\s+", "_");

                app_category.add(category);
              }
            }

            for (String category : app_category){
              mOut.write(' ');
              mOut.write(category.getBytes(UTF8));
            }
          }

          // geo
          field = val.get(3);
          if (DataType.findType(field) != DataType.NULL){
            String namespace = new String(" |geo");
            mOut.write(namespace.getBytes(UTF8));

            Tuple geo_t = (Tuple)field;

            // city
            Object temp_field = geo_t.get(0);
            if (DataType.findType(temp_field) != DataType.NULL){
              DataBag geo_bag = (DataBag)temp_field;
              Set<String> geo_city = new HashSet<String>();

              for (Tuple city_t : geo_bag){
                // woe_id
                Object temp_city = city_t.get(0);
                if (DataType.findType(temp_city) != DataType.NULL){
                    String city = ((Long)temp_city).toString();
                    geo_city.add(city);
                }
              }

              for (String city : geo_city){
                mOut.write(' ');
                mOut.write(city.getBytes(UTF8));
              }
            }
          }

          // keywords tuple
          field = val.get(4);
          if (DataType.findType(field) != DataType.NULL){
            String namespace = new String(" |keywords");
            mOut.write(namespace.getBytes(UTF8));

            Tuple keywords_t = (Tuple)field;
            Set<String> keywords = new HashSet<String>();

            // page_view
            Object temp_field = keywords_t.get(0);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple page_view_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = page_view_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            // page_click
            temp_field = keywords_t.get(1);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple page_click_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = page_click_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            // ad_click
            temp_field = keywords_t.get(2);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple ad_click_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = ad_click_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            // ad_conv
            temp_field = keywords_t.get(3);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple ad_conv_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = ad_conv_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            // search_click
            temp_field = keywords_t.get(4);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple search_click_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = search_click_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            // search_query
            temp_field = keywords_t.get(5);
            if (DataType.findType(temp_field) != DataType.NULL){
              Tuple search_query_t = (Tuple)temp_field;

              // wiki
              Object temp_wiki = search_query_t.get(0);
              if (DataType.findType(temp_wiki) != DataType.NULL){
                DataBag wiki_bag = (DataBag)temp_wiki;

                for (Tuple wiki_t : wiki_bag){
                  // word
                  Object temp_word = wiki_t.get(0);
                  if (DataType.findType(temp_word) != DataType.NULL){
                    String word = (String)temp_word;
                    word = word.replaceAll(":|\\||\\n+|\\s+", "");
                    word = word.toLowerCase();

                    keywords.add(word);
                  }
                }
              }
            }

            for (String keyword : keywords){
              mOut.write(' ');
              mOut.write(keyword.getBytes(UTF8));
            }
          }

          /*
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
          */

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
