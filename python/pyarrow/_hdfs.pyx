# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

from pyarrow.lib cimport check_status
from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem


cdef class HadoopFileSystem(FileSystem):
    """
    HDFS backed FileSystem implementation

    Parameters
    ----------
    endpoint : tuple of (host, port) pair
        For example ('localhost', 8020).
    driver : {'libhdfs', 'libhdfs3'}, default 'libhdfs'
        Connect using libhdfs (JNI-based) or libhdfs3 (3rd-party C++ library
        from Apache HAWQ (incubating)). Prefer libhdfs because libhdfs3 project
        is not maintained anymore.
    replication : int, default 3
        Number of copies each block will have.
    buffer_size : int, default 0
        If 0, no buffering will happen otherwise the size of the temporary read
        and write buffer.
    default_block_size : int, default None
        None means the default configuration for HDFS, a typical block size is
        128 MB.
    """

    cdef:
        CHadoopFileSystem* hdfs

    def __init__(self, str host, int port=8020, str user=None,
                 int replication=3, int buffer_size=0,
                 default_block_size=None):
        cdef:
            CHdfsOptions options
            shared_ptr[CHadoopFileSystem] wrapped

        if not host.startswith(('hdfs://', 'viewfs://')):
            # TODO(kszucs): do more sanitization
            host = 'hdfs://{}'.format(host)

        options.ConfigureEndPoint(tobytes(host), int(port))
        options.ConfigureHdfsReplication(replication)
        options.ConfigureHdfsBufferSize(buffer_size)

    def __init__(self, endpoint=None, driver=None, replication=None,
                 user=None, buffer_size=None, default_block_size=None):
        if endpoint is not None:
            self.endpoint = endpoint
        if driver is not None:
            self.driver = driver
        if replication is not None:
            self.replication = replication
        if user is not None:
            options.ConfigureHdfsUser(tobytes(user))
        if default_block_size is not None:
            options.ConfigureHdfsBlockSize(default_block_size)

        with nogil:
            wrapped = GetResultValue(CHadoopFileSystem.Make(options))
        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.hdfs = <CHadoopFileSystem*> wrapped.get()

    @staticmethod
    def from_uri(uri):
        cdef CResult[CHdfsOptions] result
        result = CHdfsOptions.FromUriString(tobytes(uri))
        return HdfsOptions.wrap(GetResultValue(result))

    @property
    def endpoint(self):
        return (
            frombytes(self.options.connection_config.host),
            self.options.connection_config.port
        )

    @endpoint.setter
    def endpoint(self, value):
        if not isinstance(value, tuple) or len(value) != 2:
            raise TypeError('Endpoint must be a tuple of host port pair')
        self.options.connection_config.host = tobytes(value[0])
        self.options.connection_config.port = int(value[1])

    @property
    def driver(self):
        if self.options.connection_config.driver == HdfsDriver_LIBHDFS3:
            return 'libhdfs3'
        else:
            return 'libhdfs'

    @driver.setter
    def driver(self, value):
        if value == 'libhdfs3':
            self.options.connection_config.driver = HdfsDriver_LIBHDFS3
        elif value == 'libhdfs':
            self.options.connection_config.driver = HdfsDriver_LIBHDFS
        else:
            raise ValueError('Choose either libhdfs of libhdfs3')

    @property
    def user(self):
        return frombytes(self.options.connection_config.user)

    @user.setter
    def user(self, value):
        self.options.connection_config.user = tobytes(value)

        The following two calls are equivalent:

        HadoopFileSystem.from_uri(
            'hdfs://localhost:8020/?user=test&replication=1'
        )

        HadoopFileSystem('localhost', port=8020, user='test', replication=1)

        Parameters
        ----------
        uri : str
            A string URI describing the connection to HDFS.
            In order to change the user, replication, buffer_size or
            default_block_size pass the values as query parts.

        Returns
        -------
        HadoopFileSystem
        """
        cdef:
            HadoopFileSystem self = HadoopFileSystem.__new__(HadoopFileSystem)
            shared_ptr[CHadoopFileSystem] wrapped
            CHdfsOptions options

        options = GetResultValue(CHdfsOptions.FromUriString(tobytes(uri)))
        with nogil:
            wrapped = GetResultValue(CHadoopFileSystem.Make(options))

        self.init(<shared_ptr[CFileSystem]> wrapped)
        return self

    def __reduce__(self):
        cdef CHdfsOptions opts = self.hdfs.options()
        return (
            HadoopFileSystem, (
                frombytes(opts.connection_config.host),
                opts.connection_config.port,
                frombytes(opts.connection_config.user),
                opts.replication,
                opts.buffer_size,
                opts.default_block_size,
            )
        )
