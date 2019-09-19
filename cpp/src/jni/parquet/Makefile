.PHONY:all
CC=g++

INCLUDE=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++11 -fPIC -g
LFLAGS=-L/usr/java/jdk1.8.0_221-amd64/jre/lib/amd64/server
LIBS=-lhdfs3 -lparquet -larrow -larrow_dataset
MAIN=libarrow_parquet_jni.so


all: clean $(MAIN) install

test:
	rm -f small_test
	$(CC) $(CFLAGS) $(INCLUDE) -o small_test main.c /usr/lib64/libarrow_parquet_jni.so ${LIBS}

$(MAIN):
	$(CC) $(CFLAGS) $(INCLUDE) -shared -o $(MAIN) HdfsConnector.cpp ParquetReader.cpp ParquetWriter.cpp jni_wrapper.cpp $(LIBS)

install:
	cp -f libarrow_parquet_jni.so /usr/lib64/libarrow_parquet_jni.so

clean:
	rm -f libarrow_parquet_jni.so
	rm -f /usr/lib64/libarrow_parquet_jni.so
