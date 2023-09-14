import asyncio
#==============================ASYNC UNIX SOCKET SERVER==========================================================
# This is an asynchronous unix socket server implementation.
# Usage:
#       async def data_consumer():
#           server = AsyncUnixSocketServer("/tmp/my_unix_socket")
#           async for data_chunk in serverd.open():
#               print("Received:", data_chunk)
#           await server.close()
class AsyncUnixSocketServer():
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path

    async def send_str(self, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg = str(msg)
        message_length = len(msg)
        self.writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        self.writer.write(msg.encode('utf-8'))
        await self.writer.drain()

    async def receive(self):
        while True:
                msg_length_prefix = await self.reader.read(self.MSG_LENGTH_PREFIX_BYTES)
                if not msg_length_prefix:
                    break
                msg_length = int.from_bytes(msg_length_prefix, byteorder='big')
                msg = await self.reader.read(msg_length)
                if not msg:
                    break
                msg = msg.decode('utf-8')
                yield msg
        
    async def client_handler(self, reader, writer):
        try:
            self.reader = reader
            self.writer = writer
            await self.receive()
        finally:
            self.writer.close()
            await self.writer.wait_closed()

    async def open(self):
        self.server = await asyncio.start_unix_server(self.client_handler, self.unix_socket_path)

    async def close(self):
        self.server.close()
        await self.server.wait_closed()



#==============================ASYNC UNIX SOCKET CLIENT==========================================================
# This is an asynchronous unix socket client implementation.
# Usage: 
#   async def send_data_to_server():
#       client = UnixSocketClient("/tmp/my_unix_socket")
#       try:
#           await client.connect()
#           while True:
#               data_to_send = input("Enter data to send (or 'exit' to quit): ").encode("utf-8")
#               if data_to_send == b'exit':
#                   break
#               await client.send_str(data_to_send)
#       except KeyboardInterrupt:
#           pass
#       finally:
#           client.close()
class AsyncUnixSocketClient():
    MSG_LENGTH_PREFIX_BYTES = 4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path

    async def send_str(self, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to server closing")
        msg = str(msg)
        message_length = len(msg)
        self.writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        self.writer.write(msg.encode('utf-8'))
        await self.writer.drain()

    async def receive(self):
        while True:
            msg_length_prefix = await self.reader.read(self.MSG_LENGTH_PREFIX_BYTES)
            if not msg_length_prefix:
                break
            msg_length = int.from_bytes(msg_length_prefix, byteorder='big')
            msg = await self.reader.read(msg_length)
            if not msg:
                break
            msg = msg.decode('utf-8')
            yield msg

    async def connect(self):
        self.reader, self.writer = await asyncio.open_unix_connection(path=self.unix_socket_path)
        await self.receive()
        
    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()



#==============================CONTEXT MANAGED ASYNC UNIX SOCKET SERVER==========================================================
# This is a context manager version of AsyncUnixSocketServer.
# Usage:
#       async def data_consumer():
#           async with ContextManagedAsyncUnixSocketServer("/tmp/my_unix_socket") as server:
#               async for data_chunk in server:
#                   print("Received:", data_chunk)
class ContextManagedAsyncUnixSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path

    async def __aenter__(self):
        self.server = await asyncio.start_unix_server(
            self.client_handler,
            path=self.unix_socket_path
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.server.close()
        await self.server.wait_closed()

    async def send_str(self, msg):
        if self.server.writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg = str(msg)
        message_length = len(msg)
        self.server.writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        self.server.writer.write(msg.encode('utf-8'))
        await self.server.writer.drain()

    async def receive(self, reader):
        while True:
            msg_length_prefix = await reader.read(self.MSG_LENGTH_PREFIX_BYTES)
            if not msg_length_prefix:
                break
            msg_length = int.from_bytes(msg_length_prefix, byteorder='big')
            msg = await reader.read(msg_length)
            if not msg:
                break
            msg = msg.decode('utf-8')
            yield msg

    async def client_handler(self, reader, writer):
        try:
            self.server.reader = reader
            self.server.writer = writer
            await self.receive(reader)
        finally:
            self.writer.close()
            await self.writer.wait_closed()


#==============================CONTEXT MANAGED ASYNC UNIX SOCKET CLIENT==========================================================
# This is a context manager version of AsyncUnixSocketClient.
# Usage:
#       async def send_data_to_server():
#           async with UnixSocketClient("/tmp/my_unix_socket") as client:
#               while True:
#                   data_to_send = input("Enter data to send (or 'exit' to quit): ").encode("utf-8")
#                   if data_to_send == b'exit':
#                       break
#                   await client.send_str(data_to_send)
class ContextManagedAsyncUnixSocketClient:
    MSG_LENGTH_PREFIX_BYTES = 4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path
        self.reader = None
        self.writer = None

    async def __aenter__(self):
        self.reader, self.writer = await asyncio.open_unix_connection(self.unix_socket_path)
        await self.receive()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send_str(self, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to server closing")
        msg = str(msg)
        message_length = len(msg)
        self.writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        self.writer.write(msg.encode('utf-8'))
        await self.writer.drain()

    async def receive(self):
        while True:
            msg_length_prefix = await self.reader.read(self.MSG_LENGTH_PREFIX_BYTES)
            if not msg_length_prefix:
                break
            msg_length = int.from_bytes(msg_length_prefix, byteorder='big')
            msg = await self.reader.read(msg_length)
            if not msg:
                break
            msg = msg.decode('utf-8')
            yield msg