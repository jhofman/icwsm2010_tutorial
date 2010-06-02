#!/bin/bash
#
# file: run_wordcount
#
# description: runs the wordcount example in wordcount.py, either
# locally or with hadoop. note: this is an overly verbose driver
# script; running the code will show the actual command executed,
# which is all that's required to run either locally or with hadoop.
#
# usage:
#  locally: ./run_wordcount
#  hadoop: ./run_wordcount -h
#
#  note: if running with hadoop, $HADOOP_HOME should be set to your
#  hadoop install, e.g. 
#    export HADOOP_HOME=/usr/lib/hadoop
#    export HADOOP_HOME=~/hadoop
#  or similar
#
# author: jake hofman (gmail: jhofman)
#

# run locally unless "-h" switch is provided to run with hadoop
mode=${1-"-l"}

INPUT="input.txt"
OUTPUT="wc_output"
MAPPER="./wordcount.py -m"
REDUCER="./wordcount.py -r"

# display the input
echo "input:"
cat $INPUT
echo

# remove existing output
rm -rf $OUTPUT

echo -n "running "

# locally
if [ $mode != '-h' ]
    then

    echo "locally:"
    cmd="cat $INPUT | $MAPPER | sort -k1 | $REDUCER > $OUTPUT"
    # or equivalently: cmd="./wordcount.py -l < $INPUT > $OUTPUT

else
# hadoop

    echo "with hadoop:"
    cmd="$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop*streaming*.jar \
	-input $INPUT \
	-output $OUTPUT \
	-mapper \"$MAPPER\" \
	-reducer \"$REDUCER\" \
	-file wordcount.py \
	-file hstream.py"

    OUTPUT="$OUTPUT/part-*"
fi


# display the command
echo $cmd
echo

# execute the command
eval $cmd

# display the output
echo "output:"
[ $? == 0 ] && cat $OUTPUT
