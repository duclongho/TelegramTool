import asyncio
from executor import API_KEY, API_SECRET
from binance import AsyncClient
from binance.exceptions import BinanceAPIException


async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)
    try:
        balances = await client.futures_account_balance()
        for b in balances:
            if b["asset"] == "USDT":
                print("Doc so du: OK ->", b["availableBalance"], "USDT")

        r = await client.futures_change_leverage(symbol="BTCUSDT", leverage=3)
        print("QUYEN DAT LENH: OK ->", r)
    except BinanceAPIException as e:
        print("LOI:", e.code, e.message)
    finally:
        await client.close_connection()


asyncio.run(main())
