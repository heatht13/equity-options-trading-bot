import asyncio
import json

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

    async def send_json(self, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to server closing")
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
        
    async def close(self):
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
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send_json(self, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg_bytes = msg.encode('utf-8')
        message_length = len(msg_bytes)
        self.writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        self.writer.write(msg_bytes)
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


#==============================ASYNC UNIX SOCKET SERVER==========================================================
# Reference md_handler to see how to implement an async socket server
class AsyncUnixSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path
        self.server = None

    async def send_json(self, writer, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to server closing")
        message_length = len(msg)
        writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        writer.write(msg.encode('utf-8'))
        await writer.drain()

    async def client_handler(self, reader, writer):
        try:
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
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self):
        self.server = await asyncio.start_unix_server(self.client_handler, self.unix_socket_path)

    async def close(self):
        self.server.close()
        await self.server.wait_closed()

#==============================CONTEXT MANAGED ASYNC UNIX SOCKET SERVER==========================================================
# This is a context manager version of AsyncUnixSocketServer.
# Usage:
#       async def data_consumer():
#           async with ContextManagedAsyncUnixSocketServer("/tmp/my_unix_socket") as server:
#               async for data_chunk in server:
# #                   print("Received:", data_chunk)
class ContextManagedAsyncUnixSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, unix_socket_path):
        self.unix_socket_path = unix_socket_path
        self.clients = set()

    async def __aenter__(self):
        self.server = await asyncio.start_unix_server(
            self.handle_client,
            path=self.unix_socket_path
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.server.close()
        await self.server.wait_closed()

    async def send_json(self, writer, msg):
        if self.writer.is_closing():
            raise ConnectionError("Connection to server closing")
        message_length = len(msg)
        writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        writer.write(msg.encode('utf-8'))
        await writer.drain()

    async def handle_client(self, reader, writer):
        self.writer = writer
        try:
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
        finally:
            self.writer = None
            writer.close()
            await writer.wait_closed()