#!/bin/bash

# CONSTANTS
composite=PartitionSample
namespace=com.ibm.streamsx.parquet.sample

SAMPLE_HOME="../../.."
projectDir=$( cd ${SAMPLE_HOME} ; pwd )
outputDir="${projectDir}/output/com.ibm.streamsx.parquet.sample.$composite/Standalone"
dataDir="${projectDir}/data"
logDir="${projectDir}/logs/"
PARQUET_TOOLKIT_HOME="${projectDir}/../.."


#FUNCTIONS
step() { echo ; echo -e "\e[1;34m$*\e[0m" ; }
die() { echo ; echo -e "\e[1;31m$*\e[0m" >&2 ; exit 1 ; }

toolkitList=(
${PARQUET_TOOLKIT_HOME}/com.ibm.streamsx.parquet
)


compilerOptionsList=(
--verbose-mode
--rebuild-toolkits
--spl-path=$( IFS=: ; echo "${toolkitList[*]}" )
--standalone
--optimized-code-generation
--cxx-flags=-g3
--rebuild-toolkits
--main-composite=$namespace::$composite
--output-directory=$outputDir
--data-directory=$dataDir
--num-make-threads=$coreCount
)

if [ ! -d "$DIRECTORY" ]; then
   mkdir -p $logDir
fi

logFile=$logDir/RawSampleCompile.log


step "configuration for composite '$namespace::$composite' ..."
( IFS=$'\n' ; echo -e "\nStreams toolkits:\n${toolkitList[*]}" )
( IFS=$'\n' ; echo -e "\nStreams compiler options:\n${compilerOptionsList[*]}" )

step "compiling application '$namespace::$composite' ..."
cd $projectDir
sc ${compilerOptionsList[*]} || die "Sorry, could not build '$namespace::$composite', $?" |& tee -a $logFile 

step "done"


