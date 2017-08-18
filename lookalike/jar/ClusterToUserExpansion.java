package com.yahoo.adw.closed_loop.lift_analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import com.yahoo.adw.closed_loop.mr.UserExpansionPayload;
import com.yahoo.adw.closed_loop.mr.CohortKey;
import com.yahoo.adw.closed_loop.mr.ClusterExpansionPayload;
import com.yahoo.adw.closed_loop.common.ClosedLoopConstants;
import com.yahoo.adw.closed_loop.common.Constructors;
import com.yahoo.adw.closed_loop.common.MRUtils;
//import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.yahoo.adw.closed_loop.mr.LiftCohort;

//@Slf4j
public class ClusterToUserExpansion extends Configured implements Tool {
    public static class ClusterMapper extends org.apache.hadoop.mapreduce.Mapper<AvroKey<ClusterExpansionPayload>, NullWritable, LongWritable, AvroValue<UserExpansionPayload>> {
        /** Default counter group. */
        public static String COUNTER_GROUP = "ClusterMapper";

        public static UserExpansionPayload UEPL = UserExpansionPayload.newBuilder()
                                                                .setCohort(LiftCohort.newBuilder().build())
                                                                .build();

        /**
         * Set up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * Map function, generate key/value pairs, key is cluster id, value is
         *
         * @param key Avro key ClusterExpansionPayload.
         * @param value NullWritable.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(AvroKey<ClusterExpansionPayload> key, NullWritable value, Context context) throws IOException, InterruptedException {
            /*
            ClusterExpansionPayload datum = key.datum();
            if (null == datum.getClusterIds() || datum.getClusterIds().isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyClusterIdList").increment(1);
                return;
            }
            UserExpansionPayload outValue = UserExpansionPayload.newBuilder()
                                                                .setCohort(datum.getCohort())
                                                                .build();
           for (Long clusterId : datum.getClusterIds()) {
                context.write(new LongWritable(clusterId), new AvroValue<>(outValue));
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
            */

            ClusterExpansionPayload datum = key.datum();
            if (null == datum.getClusterIds() || datum.getClusterIds().isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyClusterIdList").increment(1);
                return;
            }
            // UserExpansionPayload outValue = UserExpansionPayload.newBuilder()
            //                                                     .setCohort(datum.getCohort())
            //                                                     .build();

            for (Long clusterId : datum.getClusterIds()) {
                context.write(new LongWritable(clusterId), new AvroValue<>(UEPL));
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
        }

        /**
         * Clean up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class SigMatrixMapper extends org.apache.hadoop.mapreduce.Mapper<AvroKey, NullWritable, LongWritable,  AvroValue<UserExpansionPayload>> {

        /** Default counter group. */
        public static String COUNTER_GROUP = "SigMatrixMapper";

        /**
         * Set up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * Map function, read reverse signature matrix data, generate key/value pairs, key is cluster id, payload contains list of sids.
         *
         * @param mapKey AvroKey.
         * @param mapValue NullWritable.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(AvroKey mapKey, NullWritable mapValue, Context context) throws IOException, InterruptedException {
            GenericRecord key = (GenericRecord)mapKey.datum();
            GenericArray rawIdList = (GenericArray)key.get("user_raw_ids");
            if (null == rawIdList || rawIdList.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyRawIds").increment(1);
                return;
            }
            List<String> sidList = new ArrayList<>();
            for (int i = 0; i < rawIdList.size(); i++) {
                GenericRecord userRawId = (GenericRecord)rawIdList.get(i);
                if ("SID".equals((String)userRawId.get("id_type"))) {
                    sidList.add((String)userRawId.get("raw_id"));
                }
            }
            if (sidList.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptySidList").increment(1);
                return;
            }
            long clusterId = (long)key.get("cluster_id");
            UserExpansionPayload outValue = UserExpansionPayload.newBuilder()
                                                                .setSids(sidList)
                                                                .build();
            context.write(new LongWritable(clusterId), new AvroValue<>(outValue));
            context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
        }

        /**
         * Clean up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, AvroValue<UserExpansionPayload>, Text, NullWritable > {

        /** Default counter group. */
        public static String COUNTER_GROUP = "ClusterToUserExpansionReducer";

        /**
         * Set up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }


        /**
         * Reduce function, to join on cluster id, and populate lookalike user ids into every agg key.
         *
         * @param key Cluster id.
         * @param values List of UserExpansionPayload.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(LongWritable key, Iterable<AvroValue<UserExpansionPayload>> values, Context context) throws IOException, InterruptedException {
            //Reducer side cache cohort information, including ad system id, order id, panel, start ts and end ts.
            /*
            Map<CohortKey, Integer> cohortMap = new HashMap<>();
            ArrayList<String> sidList =  new ArrayList<>();
            for (AvroValue<UserExpansionPayload> value : values) {
                UserExpansionPayload datum = value.datum();
                if (null == datum.getSids()) {
                    //From event mapper.
                    CohortKey cohortKey = Constructors.newCohortKeyFrom(datum.getCohort());
                    cohortMap.put(cohortKey, cohortMap.getOrDefault(cohortKey, 0) + 1);
                } else {
                    //From signature matrix mapper.
                    sidList.addAll(datum.getSids());
                }
            }
            if (cohortMap.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "NoEventsInClusterId").increment(1);
                return;
            }
            if (sidList.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "NoMatchClusterId").increment(1);
                return;
            }
            for (Map.Entry<CohortKey, Integer> entry : cohortMap.entrySet()) {
                UserExpansionPayload outValue = UserExpansionPayload.newBuilder()
                                                                    .setCohort(Constructors.newLiftCohortFrom(entry.getKey()))
                                                                    .setScore(entry.getValue())
                                                                    .setSids(sidList)
                                                                    .build();
                context.write(new AvroKey<>(outValue), NullWritable.get());
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
            */

            boolean isThereEvent = false;
            Set<String> sidList = new HashSet<>();
            for (AvroValue<UserExpansionPayload> value : values) {
                UserExpansionPayload datum = value.datum();
                if (null == datum.getSids()) {
                    //From event mapper.
                    isThereEvent = true;
                } else {
                    //From signature matrix mapper.
                    sidList.addAll(datum.getSids());
                }
            }

            if (!isThereEvent) {
                context.getCounter(COUNTER_GROUP, "NoEventsInClusterId").increment(1);
                return;
            }

            if (sidList.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "NoMatchClusterId").increment(1);
                return;
            }

            for (String sid : sidList){
                context.write(new Text(sid), NullWritable.get());
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
        }

        /**
         * Clean up.
         *
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        for (String arg : args) {
            MRUtils.UpdateConfiguration(conf, arg);
        }

        String runDate = conf.get(ClosedLoopConstants.RUN_DATE);
        if (null == runDate) {
            throw new RuntimeException(ClosedLoopConstants.RUN_DATE + " is not set!. -D" + ClosedLoopConstants.RUN_DATE + "=YYYYMMDD");
        }

        Job job = Job.getInstance(conf, conf.get("mapreduce.job.name", "ClusterToUserExpansion"));
        job.setJarByClass(ClusterToUserExpansion.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(conf.getInt("mapreduce.reduce.tasks", 128));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputValueSchema(job, UserExpansionPayload.getClassSchema());

        String clusterDataPath = conf.get(ClosedLoopConstants.CLUSTER_DATA_PATH);
        if (null == clusterDataPath) {
            throw new RuntimeException(ClosedLoopConstants.CLUSTER_DATA_PATH + " is not set!. -D" + ClosedLoopConstants.CLUSTER_DATA_PATH);
        }
        String sigMatrixPath = conf.get(ClosedLoopConstants.SIG_MATRIX_DATA_PATH);
        if (null == sigMatrixPath) {
            throw new RuntimeException(ClosedLoopConstants.SIG_MATRIX_DATA_PATH + " is not set!. -D" + ClosedLoopConstants.SIG_MATRIX_DATA_PATH);
        }
        MultipleInputs.addInputPath(job, new Path(clusterDataPath), AvroKeyInputFormat.class, ClusterMapper.class);
        MultipleInputs.addInputPath(job, new Path(sigMatrixPath), AvroKeyInputFormat.class, SigMatrixMapper.class);

        AvroJob.setOutputKeySchema(job, UserExpansionPayload.getClassSchema());
        job.setOutputFormatClass(TextOutputFormat.class);

        job.submit();
        System.out.println("Job tracking URL: " + job.getTrackingURL());
        boolean status = job.waitForCompletion(true);
        if (status) {
            System.out.println("Job history URL: " + job.getHistoryUrl());
            return 0;
        } else {
            throw new RuntimeException(job.getJobName() + " status: " + job.isSuccessful());
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode =ToolRunner.run(new ClusterToUserExpansion(), args);
        System.exit(exitCode);
    }
}
