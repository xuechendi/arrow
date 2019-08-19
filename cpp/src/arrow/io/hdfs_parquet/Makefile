.PHONY:all
CC=g++

INCLUDE=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++11 -fPIC -g
LFLAGS=-L/usr/java/jdk1.8.0_221-amd64/jre/lib/amd64/server
#LIBS=-lhdfs -lparquet -larrow -larrow_dataset -ljvm
LIBS=-lhdfs3 -lparquet -larrow -larrow_dataset
MAIN=libarrow_parquet.so
EXEC=test


all: clean $(MAIN)

$(MAIN):
	$(CC) $(CFLAGS) $(LFLAGS) $(INCLUDE) -shared -o $(MAIN) ParquetHdfsReader.cpp ParquetHdfsRecordBatchReader.cpp $(LIBS)
	cp -f libarrow_parquet.so /usr/lib64/libarrow_parquet.so
	cp -f ParquetHdfsReader.h ParquetHdfsRecordBatchReader.h /usr/local/include/arrow/io/hdfs_parquet/

$(EXEC):
	$(CC) $(CFLAGS) $(LFLAGS) $(INCLUDE) -o $(EXEC) main.c -larrow_parquet $(LIBS)

clean:
	rm -f libarrow_parquet.so
	rm -f test
