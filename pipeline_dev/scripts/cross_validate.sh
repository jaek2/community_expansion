# to extract feature weight run the following
# vw/bin64/vw -d /grid/0/tmp/jaek/vw_train/20702705 --loss_function=squared --noconstant --invert_hash feature_vector/20702705.feature

# sh cross_validate.sh -s 20702705 -l squared -d /grid/0/tmp/jaek/vw_train -r /homes/adwstg/jaek/pipeline_dev

# take input arguments
while getopts "s:l:d:r:" OPTION; do
    case $OPTION in
        s)  # segment id
            SEGMENT=$OPTARG     # ex) 20702705
            ;;
        l)  # type of loss function used for training
            LOSS_FUNCTION=$OPTARG   # ex) squared, hinge, logistic, quantile
            ;;
        d)  # directoy for vowpal wabbit input train data
            INPUT_PATH=$OPTARG  # ex) /grid/0/tmp/jaek/vw_train
            ;;
        r)  # root directory of the project (to find the library folders)
            PROJ_ROOT=$OPTARG   # ex) /homes/adwstg/jaek/pipeline_dev
            ;;
        \?)
            echo "You must feed segment id (-s), type of loss function (-l) and path of the input train data (-d)"
            exit
            ;;
    esac
done

# shuffle the lines of the training data of a certain segment
mkdir $INPUT_PATH/cross_validation_$SEGMENT
mkdir $INPUT_PATH/cross_validation_$SEGMENT/shuffled/
mkdir $INPUT_PATH/cross_validation_$SEGMENT/split/
mkdir $INPUT_PATH/cross_validation_$SEGMENT/train
mkdir $INPUT_PATH/cross_validation_$SEGMENT/test
mkdir $INPUT_PATH/cross_validation_$SEGMENT/tmp

#
shuf $INPUT_PATH/$SEGMENT -o $INPUT_PATH/cross_validation_$SEGMENT/shuffled/shuffled_$SEGMENT

# get the number of lines N
NUM_LINE=$(wc -l < $INPUT_PATH/cross_validation_$SEGMENT/shuffled/shuffled_$SEGMENT)
NUM_SPLIT=$(($NUM_LINE / 10))

if (($NUM_LINE % 10)); then
    # $NUM_LINE is not divisible by 10

    NUM_SPLIT=$(($NUM_SPLIT + 1))
fi

# split the training data into eqaul pieces of N/10 lines
split -l $NUM_SPLIT $INPUT_PATH/cross_validation_$SEGMENT/shuffled/shuffled_$SEGMENT $INPUT_PATH/cross_validation_$SEGMENT/split/part_

# repeat follwing in 10 times : pick a piece, merge the rest, train on the merged one and test on the piece, save the accuracy, and remove the merged one
VW_ACCURACY=0.0

for part in $INPUT_PATH/cross_validation_$SEGMENT/split/*
do
    # move the testing set and create the training set by merging the rest
    mv $part $INPUT_PATH/cross_validation_$SEGMENT/test/
    cat $INPUT_PATH/cross_validation_$SEGMENT/split/* >> $INPUT_PATH/cross_validation_$SEGMENT/train/data_train

    # run vw and get the accuracy
    $PROJ_ROOT/vw/bin64/./vw -b 30 -f $INPUT_PATH/cross_validation_$SEGMENT/tmp/$SEGMENT.model -d $INPUT_PATH/cross_validation_$SEGMENT/train/data_train --loss_function=$LOSS_FUNCTION --noconstant --passes 10 --cache_file $INPUT_PATH/cross_validation_$SEGMENT/tmp/$SEGMENT.cache
    $PROJ_ROOT/vw/bin64/./vw -d $INPUT_PATH/cross_validation_$SEGMENT/test/* -t -i $INPUT_PATH/cross_validation_$SEGMENT/tmp/$SEGMENT.model -p $INPUT_PATH/cross_validation_$SEGMENT/tmp/pred_$SEGMENT --binary
    VW_ACCURACY=$(echo "$VW_ACCURACY + $(/homes/adwstg/jaek/Python2.7.10/bin/python $PROJ_ROOT/scripts/accuracy_test.py $INPUT_PATH/cross_validation_$SEGMENT/test/* $INPUT_PATH/cross_validation_$SEGMENT/tmp/pred_$SEGMENT)"|bc -l)

    # remove training set, prediction model, prediction result
    rm $INPUT_PATH/cross_validation_$SEGMENT/train/data_train
    rm $INPUT_PATH/cross_validation_$SEGMENT/tmp/$SEGMENT.model
    rm $INPUT_PATH/cross_validation_$SEGMENT/tmp/pred_$SEGMENT
    rm $INPUT_PATH/cross_validation_$SEGMENT/tmp/$SEGMENT.cache
    # put the testing set back
    mv $INPUT_PATH/cross_validation_$SEGMENT/test/* $INPUT_PATH/cross_validation_$SEGMENT/split/
done

# take the average of the accuracy and report it
echo $(echo "$VW_ACCURACY/10"|bc -l)

# clean the directories
rm -rf $INPUT_PATH/cross_validation_$SEGMENT
