#
# Unit test scripts for tmstat_eval functionality.
#
# $Id$
#
name=$0
obj_dir=$1
tmctl="$obj_dir/tmctl --base=$obj_dir/core_data -d private"
tmstat_test="$obj_dir/tmstat_test --base=$obj_dir/core_data -d private"

# Exit upon any failure
set -e

# Setup directory structure
rm -f core.*
rm -fr $obj_dir/core_data
mkdir -p $obj_dir/core_data

# Generate a sample core file
ulimit -c unlimited
echo 0x0f > /proc/self/coredump_filter
set +e
$obj_dir/tmstat_test --base=$obj_dir/core_data --test=core
set -e

# Verify that 
for f in core.*
do
    $obj_dir/tmctl -f $f 2>&1
    $obj_dir/tmctl -f $f -x $obj_dir/core_data 2>&1
done

SEGMENTS=$obj_dir/core_data/segment*
if ["$SEGMENTS" -eq "$obj_dir/core_data/segment*"]
then
    echo "No segments written"
    exit 1
else
    for f in $SEGMENTS
    do
        $obj_dir/tmctl -f $f 2>&1
    done
fi

# Cleanup test files
rm -f core.*
rm -fr $obj_dir/core_data
