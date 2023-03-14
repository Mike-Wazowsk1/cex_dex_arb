#type: ignore
try:
    from binance_socket_c import main
    print("default import")
except:
    from sockets.binance_socket_c import main
    print("default import")
    

import asyncio
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
