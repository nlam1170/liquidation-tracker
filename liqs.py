import rapidjson
import aiohttp
import asyncio
import discord

client = discord.Client()
token = "disc token"
target_channel = 714583994901200976

async def connect_bitmex():
    url = r"wss://www.bitmex.com/realtime?subscribe=instrument:XBTUSD,trade:XBTUSD"
    session = aiohttp.ClientSession(json_serialize=rapidjson.dumps)
    ws = await session.ws_connect(url=url,heartbeat=5)
    connect_msg = await ws.receive_json(loads=rapidjson.loads)
    print(connect_msg)
    success_msg = await ws.receive_json(loads=rapidjson.loads)
    print(success_msg)
    success_msg = await ws.receive_json(loads=rapidjson.loads)
    print(success_msg)
    return ws

def extract_oi(resp):
    if "openInterest" in resp["data"][0].keys():
        oi = resp["data"][0]["openInterest"]
        return oi
    return -1

async def get_following_oi(ws):
    while True:
        resp = await ws.receive_json(loads=rapidjson.loads)
        if resp["table"] == "instrument" and resp["action"] == "update":
            oi = extract_oi(resp)
            if oi != -1: return oi

async def send_results(trade_info, prev_oi, following_oi, slippage):
    channel = client.get_channel(target_channel)
      
    if float(prev_oi) < float(following_oi):
        await channel.send(f"Start Price:`{trade_info[0]}`\nEnd Price`{trade_info[1]}`\nSize:`{trade_info[2]}`\nSide:`{trade_info[3]}`\nSlippage:`{slippage}`\nTRADE OPENED POS")
    else:
        await channel.send(f"Start Price:`{trade_info[0]}`\nEnd Price`{trade_info[1]}`\nSize:`{trade_info[2]}`\nSide:`{trade_info[3]}`\nSlippage:`{slippage}`\nTRADE CLOSED POS")

async def parse_data():
    ws = await connect_bitmex()
    prev_oi = None
    following_oi = None
    while True:
        resp = await ws.receive_json(loads=rapidjson.loads)
        
        if resp["table"] == "instrument" and resp["action"] == "update":
            oi = extract_oi(resp)
            if oi != -1: prev_oi = oi
                
        if resp["table"] == "trade" and resp["action"] == "insert":
            total_size = 0
            for entry in resp["data"]:
                total_size += entry["size"]
                
            if total_size > 100000:
                trade_info = (resp["data"][0]["price"],resp["data"][-1]["price"], total_size, resp["data"][0]["side"])
                slippage = abs(float(resp["data"][0]["price"]) - float(resp["data"][-1]["price"]))
                if prev_oi == None: print("previous oi has not been collected from stream yet")
                following_oi = await get_following_oi(ws)
                await send_results(trade_info, prev_oi, following_oi, slippage)
                prev_oi = following_oi

@client.event
async def on_ready():
    print("connected")
    await parse_data()

client.run(token)
        



