#!/bin/bash


# CONSTANTS

composite=PartitionSample
namespace=com.ibm.streamsx.parquet.sample
SAMPLE_HOME="../../.."
projectDir=$( cd ${SAMPLE_HOME} ; pwd )
outputDir="${projectDir}/output/com.ibm.streamsx.parquet.sample.$composite/Standalone"
logDir="${projectDir}/logs"


traceLevel=5 # ... 0 for off, 1 for error, 2 for warn, 3 for info, 4 for debug, 5 for trace
logLevel=2 # ... 0 for error, 1 for warn, 2 for info
traceLevelEnum=(off error warn info debug trace)

submitParameterList=(
hdfsRootPath="/user/bigsql/ptest"
hdfsUser="bigsql"
hdfsUri="hdfs://108.168.234.156:8020"
)

logFile=$logDir/RawSampleRun.log

# FUNCTIONS

die() { echo ; echo -e "\e[1;31m$*\e[0m" >&2 ; exit 1 ; }
step() { echo ; echo -e "\e[1;34m$*\e[0m" ; }

# MAIN

cd $projectDir || die "Failed to change to $projectDir, $?"
echo "Using project Dir '$projectDir'"

step "configuration for composite '$namespace::$composite' ..."
( IFS=$'\n' ; echo -e "\n$composite submission-time parameters:\n${submitParameterList[*]}" )
echo -e "\ntrace level: ${traceLevelEnum[$traceLevel]}"

echo "LD_LIBRARY_PATH:${LD_LIBRARY_PATH}"

step "executing application '$namespace::$composite' in $runMode mode ..."
executable=$outputDir/bin/$namespace.$composite	
$executable -t $traceLevel -l $logLevel ${submitParameterList[*]} 2>&1 | tee $logFile || die "$composite interrupted." 

step "done"

exit 0
