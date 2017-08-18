REGISTER /homes/adwstg/jaek/pipeline_dev/bin/multistorage.jar;
REGISTER /homes/adwstg/jaek/pipeline_dev/bin/vwmultistorage.jar;
REGISTER /homes/adwstg/jaek/pipeline_dev/bin/graphmultistorage.jar;

/* Part 1: Get a list of segments from awd_stg.fa_segment_revenue which are the most profitable */

-- Load the data and apply a filter (change dt for different paritions)
TOP_SEGMENTS = LOAD 'adw_stg.fa_segment_revenue' USING org.apache.hive.hcatalog.pig.HCatLoader();
TOP_SEGMENTS = FILTER TOP_SEGMENTS BY usertype == 'sid' AND dt == '20170514' AND source == 'dsp';
TOP_SEGMENTS = FOREACH TOP_SEGMENTS GENERATE id, revenue/userscount AS profitability:double;

-- Sort and limit the top N data (change the number as you need)
TOP_SEGMENTS = ORDER TOP_SEGMENTS BY profitability DESC;
TOP_SEGMENTS = LIMIT TOP_SEGMENTS 10;
TOP_SEGMENTS = FOREACH TOP_SEGMENTS GENERATE id;

/* Part 2: For each segment in the list get a list of SIDs */

-- Flatten segments information
GUP_DATA = LOAD 'urs_prd.tgt_gup' USING org.apache.hive.hcatalog.pig.HCatLoader();
GUP_DATA = FILTER GUP_DATA BY segments IS NOT NULL AND yamp_events.clicks IS NOT NULL AND dt == '20170701' AND id_type == 'SID' AND status == 'active';
GUP_DATA = FOREACH GUP_DATA GENERATE raw_id, demo, device, app_activities, geo, keywords, yamp_events.clicks, mx3_events.mx3_click_events, FLATTEN(segments);

-- Join to filter through the segments list
RESULT = JOIN GUP_DATA BY segment_id, TOP_SEGMENTS BY id USING 'replicated';
SID_LIST = FOREACH RESULT GENERATE GUP_DATA::raw_id, TOP_SEGMENTS::id;
TRAINING_DATA = FOREACH RESULT GENERATE demo, device, app_activities, geo, keywords, clicks, mx3_click_events, TOP_SEGMENTS::id;
GRAPH_DATA = FOREACH RESULT GENERATE GUP_DATA::raw_id, demo, device, app_activities, geo, keywords, TOP_SEGMENTS::id;

-- Store in directories based on segment_id
--STORE SID_LIST INTO '/tmp/jaek/output_sid' USING MultiStorage('/tmp/jaek/output_sid', '1', 'none', '\\t', 'true');
--STORE TRAINING_DATA INTO '/tmp/jaek/vw_train' USING VWMultiStorage('/tmp/jaek/vw_train', '7', 'none', '\\t', 'true');
STORE GRAPH_DATA INTO '/tmp/jaek/graph_input' USING GraphMultiStorage('/tmp/jaek/graph_input', '6', 'none', '\\t', 'true');
