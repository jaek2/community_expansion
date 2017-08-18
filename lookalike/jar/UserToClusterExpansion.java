package com.yahoo.adw.closed_loop.lift_analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import com.yahoo.adw.closed_loop.mr.SidURNRecord;
import com.yahoo.adw.closed_loop.mr.CohortKey;
import com.yahoo.adw.closed_loop.mr.LiftRecord;
import com.yahoo.adw.closed_loop.mr.ClusterExpansionPayload;
import com.yahoo.adw.closed_loop.mr.SidKey;
import com.yahoo.adw.closed_loop.common.ClosedLoopConstants;
import com.yahoo.adw.closed_loop.common.MRUtils;
import com.yahoo.adw.closed_loop.common.Constructors;
//import lombok.extern.slf4j.Slf4j;

import java.lang.String;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import com.yahoo.adw.closed_loop.mr.LiftCohort;

//@Slf4j
public class UserToClusterExpansion extends Configured implements Tool {
    public static class UserMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, SidKey, AvroValue<ClusterExpansionPayload>> {

        /** Default counter group. */
        public static String COUNTER_GROUP = "UserMapper";

        public static LiftCohort COHORT = LiftCohort.newBuilder().build();
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
         * Map function
         * Generate key/value using sid/lift agg key.
         *
         * @param key LiftRecord.
         * @param value NullWritable.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            LiftRecord datum = key.datum();
            if (null == datum.getCohort().getPanelGridName() || datum.getCohort().getPanelGridName().equals(ClosedLoopConstants.DUMMY_PANEL)) {
                context.getCounter(COUNTER_GROUP, "AllPanel").increment(1);
                return;
            }
            if (null ==  datum.getSids() ||  datum.getSids().isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptySidList").increment(1);
                return;
            }
            ClusterExpansionPayload outValue = ClusterExpansionPayload.newBuilder()
                                                                      .setCohort(datum.getCohort())
                                                                      .build();
            SidKey outKey = new SidKey();
            for (SidURNRecord record : datum.getSids()) {
                outKey.setSid(record.getSid());
                context.write(outKey, new AvroValue<>(outValue));
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
            */

            // have to pass defaut LiftCohort object to determine if reduce has to generate output for this sid or not
            ClusterExpansionPayload outValue = ClusterExpansionPayload.newBuilder()
                                                                      .setCohort(COHORT)
                                                                      .build();

            SidKey outKey = new SidKey();
            outKey.setSid((value.toString()).trim());
            context.write(outKey, new AvroValue<>(outValue));
            context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
        }

        /**
         * Clean up.
         *
         * @param context Job context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class ClusterMapper extends org.apache.hadoop.mapreduce.Mapper<AvroKey<GenericRecord>, NullWritable, SidKey, AvroValue<ClusterExpansionPayload>> {

        /** Default counter group. */
        public static String COUNTER_GROUP = "ClusterMapper";

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
         * Map function.
         * Read signature matrix, generate key/value pairs using sid/cluster id.
         *
         * @param key GenericRecord.
         * @param value NullWritable.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord datum = key.datum();
            if (!("SID".equals((String)datum.get("id_type")))) {
                context.getCounter(COUNTER_GROUP, "NonSidId").increment(1);
                return;
            }
            SidKey outKey = new SidKey((String)datum.get("raw_id"));
            GenericArray clusterIdList = (GenericArray)datum.get("cluster_ids");
            if (null == clusterIdList || clusterIdList.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyClusterIdList").increment(1);
                return;
            }
            ClusterExpansionPayload.Builder builder = ClusterExpansionPayload.newBuilder()
                                                                             .setClusterIds(new ArrayList<>());
            for (int i = 0; i < clusterIdList.size(); i++) {
                GenericRecord clusterId = (GenericRecord)clusterIdList.get(i);
                builder.getClusterIds().add((long)clusterId.get("id"));
            }
            context.write(outKey, new AvroValue<>(builder.build()));
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

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<SidKey, AvroValue<ClusterExpansionPayload>, AvroKey<ClusterExpansionPayload>, NullWritable > {

        /** Default counter group. */
        public static String COUNTER_GROUP = "UserToClusterExpansionReducer";

        public static LiftCohort COHORT = LiftCohort.newBuilder().build();

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
         * Reduce function
         * Join by sid, to get a list of cluster ids belonging to the agg key.
         *
         * @param key Sid.
         * @param values List of cluster expansion payload.
         * @param context Job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(SidKey key, Iterable<AvroValue<ClusterExpansionPayload>> values, Context context) throws IOException, InterruptedException {
            /*
            Set<CohortKey> set = new HashSet<>();
            ArrayList<Long> list =  new ArrayList<>();
            for (AvroValue<ClusterExpansionPayload> value : values) {
                ClusterExpansionPayload datum = value.datum();
                //Ideally every sid should have only one list of cluster ids.
                if (null == datum.getClusterIds()) {
                    if (datum.getCohort() != null) {
                        set.add(Constructors.newCohortKeyFrom(datum.getCohort()));
                    }
                } else {
                    context.getCounter(COUNTER_GROUP, "NumOfClusterIdList").increment(1);
                    list.addAll(datum.getClusterIds());
                }
            }
            if (set.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyUserId").increment(1);
                return;
            }
            if (list.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyClusterId").increment(1);
                return;
            }
            for (CohortKey outKey : set) {
                ClusterExpansionPayload outValue = ClusterExpansionPayload.newBuilder()
                                                                          .setCohort(Constructors.newLiftCohortFrom(outKey))
                                                                          .setClusterIds(list)
                                                                          .build();
                context.write(new AvroKey<>(outValue), NullWritable.get());
                context.getCounter(COUNTER_GROUP, "ValidRecord").increment(1);
            }
            */

            boolean isThereUserId = false;
            Set<Long> cluster_set =  new HashSet<>();
            for (AvroValue<ClusterExpansionPayload> value : values) {
                ClusterExpansionPayload datum = value.datum();
                //Ideally every sid should have only one list of cluster ids.
                if (null == datum.getClusterIds()) {
                    if (datum.getCohort() != null) {
                        isThereUserId = true;
                    }
                } else {
                    context.getCounter(COUNTER_GROUP, "NumOfClusterIdList").increment(1);
                    cluster_set.addAll(datum.getClusterIds());
                }
            }

            if (!isThereUserId) {
                context.getCounter(COUNTER_GROUP, "EmptyUserId").increment(1);
                return;
            }

            if (cluster_set.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EmptyClusterId").increment(1);
                return;
            }

            ArrayList<Long> list =  new ArrayList<>(cluster_set);
            ClusterExpansionPayload outValue = ClusterExpansionPayload.newBuilder()
                                                                      .setCohort(COHORT)
                                                                      .setClusterIds(list)
                                                                      .build();
            context.write(new AvroKey<>(outValue), NullWritable.get());
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

        Job job = Job.getInstance(conf, conf.get("mapreduce.job.name", "UserToClusterExpansion"));
        job.setJarByClass(UserToClusterExpansion.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(conf.getInt("mapreduce.reduce.tasks", 128));

        job.setMapOutputKeyClass(SidKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputValueSchema(job, ClusterExpansionPayload.getClassSchema());

        String userDataPath = conf.get(ClosedLoopConstants.USER_DATA_PATH);
        if (null == userDataPath) {
            throw new RuntimeException(ClosedLoopConstants.USER_DATA_PATH + " is not set!. -D" + ClosedLoopConstants.USER_DATA_PATH);
        }
        String clusterDataPath = conf.get(ClosedLoopConstants.CLUSTER_DATA_PATH);
        if (null == clusterDataPath) {
            throw new RuntimeException(ClosedLoopConstants.CLUSTER_DATA_PATH + " is not set!. -D" + ClosedLoopConstants.CLUSTER_DATA_PATH);
        }
        MultipleInputs.addInputPath(job, new Path(userDataPath), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(clusterDataPath), AvroKeyInputFormat.class, ClusterMapper.class);

        job.setOutputFormatClass(LazyOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, ClusterExpansionPayload.getClassSchema());

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
        int exitCode =ToolRunner.run(new UserToClusterExpansion(), args);
        System.exit(exitCode);
    }
}
