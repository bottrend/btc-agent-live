import asyncio, json, logging, os, time
from collections import defaultdict, deque
import aiohttp, websockets

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("btc-agent")

GROUPS=["TECHNICAL","PRICE ACTION","ORDER FLOW","DERIVATIVES","ON-CHAIN","SENTIMENT","MACRO","CROSS-MARKET","LIQUIDITY","MARKET REGIME"]
class Engine:
    def __init__(self):
        self.features={}; self.groups={g:50.0 for g in GROUPS}; self.final=50.0
    def update(self,name,raw,score,group,source):
        now=time.time(); score=max(0,min(100,float(score)))
        self.features[name]={"raw":raw,"score":score,"group":group,"source":source,"ts":now}
        vals=[v["score"] for v in self.features.values() if v["group"]==group and now-v["ts"]<3600]
        if vals:self.groups[group]=sum(vals)/len(vals)
        active=[self.groups[g] for g in GROUPS if any(v["group"]==g for v in self.features.values())]
        self.final=sum(active)/len(active) if active else 50.0
    def status(self): return {"final":round(self.final,2),"groups":{k:round(v,2) for k,v in self.groups.items()},"features":len(self.features)}
E=Engine(); prices=deque(maxlen=240)
def clamp(x): return max(0,min(100,x))
def price_features(px):
    prices.append(px); E.update("okx.btc.last",px,50,"PRICE ACTION","OKX")
    for n in (5,10,20,60,120):
        if len(prices)>n:
            r=(px/prices[-n-1]-1)*100
            E.update(f"return.tick_{n}",r,clamp(50+r*10),"PRICE ACTION","OKX")
    if len(prices)>=20:
        ma=sum(list(prices)[-20:])/20; dev=(px/ma-1)*100
        E.update("trend.ma20_distance",dev,clamp(50+dev*8),"TECHNICAL","OKX")
async def okx_ws():
    url="wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
                args=[{"channel":"tickers","instId":"BTC-USDT"},{"channel":"books5","instId":"BTC-USDT"},{"channel":"trades","instId":"BTC-USDT"},{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},{"channel":"open-interest","instId":"BTC-USDT-SWAP"}]
                await ws.send(json.dumps({"op":"subscribe","args":args}))
                async for raw in ws:
                    m=json.loads(raw); ch=m.get("arg",{}).get("channel"); data=m.get("data") or []
                    if not data: continue
                    d=data[0]
                    if ch=="tickers": price_features(float(d["last"]))
                    elif ch=="books5":
                        bids=sum(float(x[1]) for x in d.get("bids",[])); asks=sum(float(x[1]) for x in d.get("asks",[])); tot=bids+asks
                        if tot: E.update("orderbook.imbalance.5",(bids-asks)/tot,clamp(50+50*(bids-asks)/tot),"ORDER FLOW","OKX")
                    elif ch=="trades": E.update("trades.last_side",d.get("side"),65 if d.get("side")=="buy" else 35,"ORDER FLOW","OKX")
                    elif ch=="funding-rate":
                        f=float(d.get("fundingRate",0)); E.update("derivatives.funding",f,clamp(50-f*50000),"DERIVATIVES","OKX")
                    elif ch=="open-interest": E.update("derivatives.open_interest",float(d.get("oi",0)),50,"DERIVATIVES","OKX")
        except Exception as ex:
            log.warning("OKX reconnect: %s",ex); await asyncio.sleep(3)
async def heartbeat():
    while True:
        log.info("STATE %s",json.dumps(E.status(),ensure_ascii=False)); await asyncio.sleep(10)
async def main(): await asyncio.gather(okx_ws(),heartbeat())
if __name__=="__main__": asyncio.run(main())
