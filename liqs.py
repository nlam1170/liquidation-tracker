import rapidjson
import aiohttp
import asyncio
import discord

client = discord.Client()
token = "disc token"
target_channel = 725698223259516959 

async def connect_bitmex():
    url = "wss://www.bitmex.com/realtime?subscribe=instrument:XBTUSD,trade:XBTUSD"
    session = aiohttp.ClientSession(json_serialize=rapidjson.dumps)
    ws = await session.ws_connect(url)
    connect_msg = await ws.receive_json(loads=rapidjson.loads)
    print(connect_msg)
    success_msg = await ws.receive_json(loads=rapidjson.loads)
    print(success_msg)
    success_msg = await ws.receive_json(loads=rapidjson.loads)
    print(success_msg)
    return ws

async def get_rest_oi():
    url = "https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&count=1&reverse=true"
    async with aiohttp.ClientSession(json_serialize=rapidjson.dumps) as session:
        async with session.get(url) as resp:
            resp = await resp.json(loads=rapidjson.loads)
            return resp[0]["openInterest"]

async def extract_oi(resp):
    if "openInterest" in resp["data"][0].keys():
        oi = resp["data"][0]["openInterest"]
        return oi
    return -1

async def send_results(trade_info, prev_oi, following_oi, slippage):
    channel = client.get_channel(target_channel)
    
    if trade_info[0] == "Sell":
        if float(prev_oi) < float(following_oi):
            await channel.send(f'```diff\n-Sell {trade_info[3]} Slippage:{slippage} First Price:{trade_info[1]:,} || Last Price:{trade_info[2]:,}\nTrade Opened Pos\n```')
        else:
            await channel.send(f'```diff\n-Sell {trade_info[3]:,} Slippage:{slippage} First Price:{trade_info[1]:,} || Last Price:{trade_info[2]:,}\nTrade Closed Pos\n```')
    
    if trade_info[0] == "Buy":
        if float(prev_oi) < float(following_oi):
            await channel.send(f'```diff\n+Buy {trade_info[3]:,} Slippage:{slippage} First Price:{trade_info[1]:,} || Last Price:{trade_info[2]:,}\nTrade Opened Pos\n```')
        else:
            await channel.send(f'```diff\n+Buy {trade_info[3]:,} Slippage:{slippage} First Price:{trade_info[1]:,} || Last Price:{trade_info[2]:,}\nTrade Closed Pos\n```')

async def parse_data():
    ws = await connect_bitmex()
    prev_oi = await get_rest_oi()
    following_oi = None
    while True:
        resp = await ws.receive_json(loads=rapidjson.loads)
        
        if resp["table"] == "instrument" and resp["action"] == "update":
            oi = await extract_oi(resp)
            if oi != -1: prev_oi = oi
        
        if resp["table"] == "trade" and resp["action"] == "insert":
            total_size = 0
            for entry in resp["data"]:
                total_size += entry["size"]
                
            if total_size > 100000:
                trade_info = (resp["data"][0]["side"], resp["data"][0]["price"],resp["data"][-1]["price"],total_size)
                slippage = abs(float(resp["data"][0]["price"]) - float(resp["data"][-1]["price"]))
                following_oi = await get_rest_oi()
                await send_results(trade_info, prev_oi, following_oi, slippage)
                prev_oi = following_oi

@client.event
async def on_ready():
    print("connected")
    while True:
        try:
            await parse_data()
        except TypeError:
            continue

client.run(token)
