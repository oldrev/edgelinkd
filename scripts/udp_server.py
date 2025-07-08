import asyncio

class UDPEchoServerProtocol:
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode('utf-8')
        print(f"Received message from {addr}:\n{message}\n")
        #print(f"Send {message} to {addr}")
        #self.transport.sendto(data, addr)

async def main():
    loop = asyncio.get_running_loop()
    print("UDP Echo server started on 127.0.0.1:9981")

    # 创建一个 UDP 端点并指定协议
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPEchoServerProtocol(),
        local_addr=('127.0.0.1', 9981)
    )

    try:
        # 保持运行状态，直到接收到 Ctrl+C
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("\nUDP Echo server is shutting down...")
    finally:
        transport.close()

if __name__ == "__main__":
    asyncio.run(main())
