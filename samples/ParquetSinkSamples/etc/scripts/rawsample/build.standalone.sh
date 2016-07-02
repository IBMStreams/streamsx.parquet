#!/bin/bash
#set -o xtrace

# /opt/ibm/InfoSphereStreams/4.0.1.0/bin/sc -M com.ibm.streamsx.parquet.sample::RawSample --output-directory=output/com.ibm.streamsx.parquet.sample.RawSample/Standalone --data-directory=data -T -a --rebuild-toolkits -t /homes/apyasic/github/streamsx.parquet/com.ibm.streamsx.parquet: --no-toolkit-indexing --no-mixed-mode-preprocessing 

# CONSTANTS
composite=RawSample
namespace=com.ibm.streamsx.parquet.sample

SAMPLE_HOME="../../.."
projectDir=$( cd ${SAMPLE_HOME} ; pwd )
outputDir="${projectDir}/output/com.ibm.streamsx.parquet.sample.RawSample/Standalone"
dataDir="${projectDir}/data"
logDir="${projectDir}/logs/"
PARQUET_TOOLKIT_HOME="${projectDir}/../.."


#FUNCTIONS
step() { echo ; echo -e "\e[1;34m$*\e[0m" ; }

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


