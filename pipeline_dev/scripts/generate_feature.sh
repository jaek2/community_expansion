#SEGMENT=20103634
#vw/bin64/vw -d /grid/0/tmp/jaek/vw_train/$SEGMENT --loss_function=squared --noconstant --invert_hash feature_vector/$SEGMENT.feature -b 24

#SEGMENT=20852739
#vw/bin64/vw -d /grid/0/tmp/jaek/vw_train/$SEGMENT --loss_function=squared --noconstant --invert_hash feature_vector/$SEGMENT.feature -b 24

SEGMENT=50122352
vw/bin64/vw -d /grid/0/tmp/jaek/vw_train/$SEGMENT --loss_function=squared --noconstant --invert_hash feature_vector/$SEGMENT.feature -b 24

SEGMENT=20028775
vw/bin64/vw -d /grid/0/tmp/jaek/vw_train/$SEGMENT --loss_function=squared --noconstant --invert_hash feature_vector/$SEGMENT.feature -b 24

