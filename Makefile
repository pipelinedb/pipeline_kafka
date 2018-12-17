MODULE_big = pipeline_kafka
OBJS = pipeline_kafka.o

EXTENSION = pipeline_kafka
DATA = pipeline_kafka--1.0.0.sql

REGRESS = pipeline_kafka

LIB_RDKAFKA_STATIC ?= /usr/lib/librdkafka.a

PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir) -I$(shell $(PG_CONFIG) --includedir-server)/../pipelinedb

SHLIB_LINK += $(LIB_RDKAFKA_STATIC)
SHLIB_LINK += -lz -lpthread -lssl

ifndef NO_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pipeline_kafka
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: test

test:
	make -C test
