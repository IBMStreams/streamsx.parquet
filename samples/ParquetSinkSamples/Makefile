# Licensed Materials - Property of IBM
# Text Toolkit for IBM Streams
# (c) Copyright IBM Corp. 2020
# All rights reserved.                                 
.PHONY: build all clean distributed

SPLC_FLAGS = -a
OUTPUT_DIR = output
TO_BE_DELETED = 
ifeq ($(STREAMS_STUDIO_BUILDING), 1)
  # Building from Streams Studio, use env vars set by studio
  SPLC = $(STREAMS_STUDIO_SC_PATH)
  SPL_PATH = $(STREAMS_STUDIO_SPL_PATH)
  OUTPUT_DIR = $(STREAMS_STUDIO_OUTPUT_DIRECTORY)
else
  # Building outside of Streams Studio...
  ifeq ($(STREAMS_INSTALL),)
    $(error error: environment variable STREAMS_INSTALL has to be set)
  endif
  # Set this variable if streamsx.parquet sample or toolkit location changed in your environment.
  STREAMSX_PARQUET_TOOLKIT ?= ../../com.ibm.streamsx.parquet
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(STREAMSX_PARQUET_TOOLKIT)
  # delete directories and files generated and required by StreamsStudio
  TO_BE_DELETED += bin impl .toolkitList .classpath
endif
TO_BE_DELETED += $(OUTPUT_DIR)

SPLC_FLAGS += -t $(SPL_PATH)

SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.parquet.sample::PartitionCloseOnPunctSample
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.parquet.sample::CompressionSampleRunner
SPL_MAIN_COMPOSITE3 = com.ibm.streamsx.parquet.sample::NullValuesSample
SPL_MAIN_COMPOSITE4 = com.ibm.streamsx.parquet.sample::PartitionCloseOnCountSample
SPL_MAIN_COMPOSITE5 = com.ibm.streamsx.parquet.sample::RawSample

SPL_CMD_ARGS ?=

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --output-directory=./output/PartitionCloseOnPunctSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS) --output-directory=./output/CompressionSampleRunner
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE3) $(SPL_CMD_ARGS) --output-directory=./output/NullValuesSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE4) $(SPL_CMD_ARGS) --output-directory=./output/PartitionCloseOnCountSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE5) $(SPL_CMD_ARGS) --output-directory=./output/RawSample

clean:
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE1)
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE2)
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE3)
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE4)
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE5)
	rm -rf $(TO_BE_DELETED)
