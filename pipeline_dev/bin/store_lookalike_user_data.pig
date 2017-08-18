%DEFAULT SEGMENT '50123900'
%DEFAULT MATRIX_DATE '20170721'
%DEFAULT GUP_DATE '20170721'
%DEFAULT SLASH '/'

REGISTER /homes/adwstg/jaek/pipeline_dev/bin/graphmultistorage.jar;

ORIG_SID = LOAD '/tmp/jaek/output_sid/20033178' USING PigStorage() AS (osid:chararray);

NEW_SID = LOAD '/user/adwstg/jaek/closed_loop/la_cluster_to_user_expansion/$SEGMENT$SLASH$MATRIX_DATE' USING PigStorage() AS (sid:chararray);
NEW_SID = DISTINCT NEW_SID;

JOINED = JOIN NEW_SID BY sid LEFT OUTER, ORIG_SID BY osid;
SID = FILTER JOINED BY ORIG_SID::osid IS NULL;
SID = FOREACH SID GENERATE NEW_SID::sid;

GUP_DATA = LOAD 'urs_prd.tgt_gup' USING org.apache.hive.hcatalog.pig.HCatLoader();
GUP_DATA = FILTER GUP_DATA BY raw_id IS NOT NULL AND dt == '$GUP_DATE' AND id_type == 'SID' AND status == 'active';
GUP_DATA = FOREACH GUP_DATA GENERATE raw_id, demo, device, app_activities, geo, keywords, dt;

-- Join to filter through the segments list
RESULT = JOIN GUP_DATA BY raw_id, SID BY sid;

SID_DATA = FOREACH RESULT GENERATE GUP_DATA::raw_id;
GRAPH_DATA = FOREACH RESULT GENERATE GUP_DATA::raw_id, demo, device, app_activities, geo, keywords, dt;

STORE SID_DATA INTO '/tmp/jaek/lookalike_sid/$SEGMENT' USING PigStorage();
STORE GRAPH_DATA INTO '/tmp/jaek/lookalike_graph_input/$SEGMENT' USING GraphMultiStorage('/tmp/jaek/lookalike_graph_input/$SEGMENT', '6', 'none', '\\t', 'true');
