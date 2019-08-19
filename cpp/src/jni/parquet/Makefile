.PHONY:all
CC=g++

INCLUDE=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++11 -fPIC -g
LFLAGS=-L./
#LIBS=-lhdfs -lparquet -larrow -larrow_dataset -larrow_parquet
LIBS=-lhdfs3 -lparquet -larrow -larrow_dataset -larrow_parquet
MAIN=libarrow_parquet_jni.so


all: clean $(MAIN) install

$(MAIN):
	$(CC) $(CFLAGS) $(INCLUDE) -shared -o $(MAIN) jni_wrapper.cpp $(LIBS)

install:
	cp -f libarrow_parquet_jni.so /usr/lib64/libarrow_parquet_jni.so

clean:
	rm -f libarrow_parquet_jni.so
