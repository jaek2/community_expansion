%DEFAULT SEGMENT '20702705'
%DEFAULT NUM_FEATURE '100'

%DEFAULT MATRIX_DATE '20170721'
%DEFAULT SLASH '/'

%DEFAULT ORIG_PATH_PREFIX '/tmp/jaek/output_sid/'
%DEFAULT LOOKALIKE_PATH_PREFIX '/user/adwstg/jaek/closed_loop/la_cluster_to_user_expansion/'
%DEFAULT FINAL_PATH_PREFIX '/tmp/jaek/final_output/'
%DEFAULT UNDER_SCORE '_'
%DEFAULT FINAL_PATH_SUFFIX 'f_expanded_sid_list'
%DEFAULT OUTPUT_PATH_PREFIX '/tmp/jaek/performance_evaluation/'
%DEFAULT OUTPUT_PATH_SUFFIX '_result_week_limited/'

%DEFAULT NUM_L_SID 'num_lsid'
%DEFAULT NUM_F_SID 'num_fsid'
%DEFAULT L_REVENUE 'lrevenue'
%DEFAULT F_REVENUE 'frevenue'

SET default_parallel 20;

DSP_AGG = LOAD 'adw.dsp_agg' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- join by sid first and then split by target vs split by target and then join

-- probably we can just join and split
DSP_AGG = FILTER DSP_AGG BY s_id IS NOT NULL AND advertiser_cost IS NOT NULL AND advertiser_cost != 0.0 AND '2017070700' < hour_id AND hour_id < '2017071400' AND source == 'regular'; -- hour_id (monthly), source
DSP_AGG = FOREACH DSP_AGG GENERATE s_id, advertiser_cost, hour_id;

-- Final list
FINAL_SID = LOAD '$FINAL_PATH_PREFIX$SEGMENT$UNDER_SCORE$NUM_FEATURE$FINAL_PATH_SUFFIX' USING PigStorage() AS (fsid:chararray);

-- Get the data that belong the to users in the lists
FINAL_DATA = JOIN DSP_AGG BY s_id, FINAL_SID BY fsid USING 'replicated';
FINAL_DATA = FOREACH FINAL_DATA GENERATE advertiser_cost, hour_id;

-- get counts of the unique sids in both relations (they are already lists of unique SIDs)
FINAL_GRP = GROUP FINAL_SID ALL;
NUM_FINAL_SID = FOREACH FINAL_GRP GENERATE COUNT(FINAL_SID.fsid);

STORE NUM_FINAL_SID INTO '$OUTPUT_PATH_PREFIX$SEGMENT$OUTPUT_PATH_SUFFIX$NUM_F_SID' USING PigStorage();

-- get total revenue
FINAL_GRP = GROUP FINAL_DATA BY hour_id;
PARTIAL_FINAL_REVENUE = FOREACH FINAL_GRP GENERATE SUM(FINAL_DATA.advertiser_cost) AS sum;
FINAL_GRP = GROUP PARTIAL_FINAL_REVENUE ALL;
FINAL_REVENUE = FOREACH FINAL_GRP GENERATE SUM(PARTIAL_FINAL_REVENUE.sum);

STORE FINAL_REVENUE INTO '$OUTPUT_PATH_PREFIX$SEGMENT$OUTPUT_PATH_SUFFIX$F_REVENUE' USING PigStorage();

-- Lookalike list
LOOKALIKE_SID = LOAD '$LOOKALIKE_PATH_PREFIX$SEGMENT$SLASH$MATRIX_DATE' USING PigStorage() AS (lsid:chararray);

-- Get the data that belong the to users in the lists
LOOKALIKE_DATA = JOIN LOOKALIKE_SID BY lsid, DSP_AGG BY s_id;
LOOKALIKE_DATA = FOREACH LOOKALIKE_DATA GENERATE advertiser_cost, hour_id;

-- get counts of the unique sids in both relations (they are already lists of unique SIDs)
LOOKALIKE_GRP = GROUP LOOKALIKE_SID ALL;
NUM_LOOKALIKE_SID = FOREACH LOOKALIKE_GRP GENERATE COUNT(LOOKALIKE_SID.lsid);

STORE NUM_LOOKALIKE_SID INTO '$OUTPUT_PATH_PREFIX$SEGMENT$OUTPUT_PATH_SUFFIX$NUM_L_SID' USING PigStorage();

-- get total revenue
LOOKALIKE_GRP = GROUP LOOKALIKE_DATA BY hour_id PARALLEL 36;
PARTIAL_LOOKALIKE_REVENUE = FOREACH LOOKALIKE_GRP GENERATE SUM(LOOKALIKE_DATA.advertiser_cost) AS sum;
LOOKALIKE_GRP = GROUP PARTIAL_LOOKALIKE_REVENUE ALL;
LOOKALIKE_REVENUE = FOREACH LOOKALIKE_GRP GENERATE SUM(PARTIAL_LOOKALIKE_REVENUE.sum);

STORE LOOKALIKE_REVENUE INTO '$OUTPUT_PATH_PREFIX$SEGMENT$OUTPUT_PATH_SUFFIX$L_REVENUE' USING PigStorage();

-- use the above results to measure the performance of the audience expansion
