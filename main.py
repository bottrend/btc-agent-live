import asyncio, json, logging, os, time
from collections import deque
import aiohttp, websockets

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("btc-agent")
GROUPS=["TECHNICAL","PRICE ACTION","ORDER FLOW","DERIVATIVES","ON-CHAIN","SENTIMENT","MACRO","CROSS-MARKET","LIQUIDITY","MARKET REGIME"]
STALE={"OKX":30,"MEMPOOL":900,"ALTERNATIVE.ME":172800,"COINGECKO":900,"WORLDBANK":2592000}
def clamp(x): return max(0,min(100,float(x)))

class Engine:
 def __init__(self): self.features={}; self.groups={}; self.final=None
 def update(self,name,raw,score,group,source):
  now=time.time(); self.features[name]={"raw":raw,"score":clamp(score),"group":group,"source":source,"ts":now}
  self.recalc(now)
 def recalc(self,now=None):
  now=now or time.time(); groups={}
  for g in GROUPS:
   vals=[v["score"] for v in self.features.values() if v["group"]==g and now-v["ts"]<=STALE.get(v["source"],3600)]
   if vals: groups[g]=sum(vals)/len(vals)
  self.groups=groups; self.final=sum(groups.values())/len(groups) if groups else None
 def status(self):
  self.recalc(); return {"final":None if self.final is None else round(self.final,2),"groups":{k:round(v,2) for k,v in self.groups.items()},"features_active":sum(1 for v in self.features.values() if time.time()-v["ts"]<=STALE.get(v["source"],3600)),"features_total":len(self.features)}
E=Engine(); prices=deque(maxlen=1200); last_trade_side=None

def price_features(px):
 prices.append(px)
 if len(prices)>1:
  for n in (1,5,10,20,60,120,300,600):
   if len(prices)>n:
    r=(px/prices[-n-1]-1)*100; E.update(f"price.return.tick_{n}",r,50+r*12,"PRICE ACTION","OKX")
 for n in (10,20,50,100,200):
  if len(prices)>=n:
   ma=sum(list(prices)[-n:])/n; d=(px/ma-1)*100; E.update(f"technical.sma_distance.{n}",d,50+d*10,"TECHNICAL","OKX")
 if len(prices)>=30:
  recent=list(prices)[-30:]; hi=max(recent); lo=min(recent)
  pos=50 if hi==lo else (px-lo)/(hi-lo)*100; E.update("price.range_position.30",pos,pos,"PRICE ACTION","OKX")

 regime_features()

async def okx_ws():
 url="wss://ws.okx.com:8443/ws/v5/public"
 while True:
  try:
   async with websockets.connect(url,ping_interval=20,ping_timeout=20,max_size=2**20) as ws:
    args=[{"channel":"tickers","instId":"BTC-USDT"},{"channel":"books5","instId":"BTC-USDT"},{"channel":"trades","instId":"BTC-USDT"},{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},{"channel":"open-interest","instId":"BTC-USDT-SWAP"}]
    await ws.send(json.dumps({"op":"subscribe","args":args}))
    async for raw in ws:
     m=json.loads(raw); ch=m.get("arg",{}).get("channel")
     for d in m.get("data") or []:
      if ch=="tickers":
       px=float(d["last"]); price_features(px)
       for key,mul in (("askPx",1),("bidPx",1)):
        if d.get(key): E.update("market."+key,float(d[key]),50,"LIQUIDITY","OKX")
       if d.get("askPx") and d.get("bidPx"):
        ask,bid=float(d["askPx"]),float(d["bidPx"]); mid=(ask+bid)/2; spread=(ask-bid)/mid*10000
        E.update("liquidity.spread_bps",spread,50-spread*8,"LIQUIDITY","OKX")
      elif ch=="books5":
       bids=d.get("bids",[]); asks=d.get("asks",[])
       for depth in (1,3,5):
        bv=sum(float(x[1]) for x in bids[:depth]); av=sum(float(x[1]) for x in asks[:depth]); tot=bv+av
        if tot: E.update(f"orderflow.imbalance.depth_{depth}",(bv-av)/tot,50+50*(bv-av)/tot,"ORDER FLOW","OKX")
      elif ch=="trades":
       side=d.get("side"); sz=float(d.get("sz",0)); E.update("orderflow.last_trade_side",side,62 if side=="buy" else 38,"ORDER FLOW","OKX")
       E.update("orderflow.last_trade_size",sz,50,"ORDER FLOW","OKX")
      elif ch=="funding-rate":
       f=float(d.get("fundingRate",0)); E.update("derivatives.funding_rate",f,50-f*50000,"DERIVATIVES","OKX")
      elif ch=="open-interest":
       oi=float(d.get("oi",0)); E.update("derivatives.open_interest",oi,50,"DERIVATIVES","OKX")
  except Exception as ex: log.warning("OKX reconnect %s",ex); await asyncio.sleep(3)

async def mempool_loop(session):
 urls={"fees":"https://mempool.space/api/v1/fees/recommended","mempool":"https://mempool.space/api/mempool","difficulty":"https://mempool.space/api/v1/difficulty-adjustment"}
 while True:
  try:
   async with session.get(urls["fees"],timeout=15) as r:
    x=await r.json(); fastest=float(x.get("fastestFee",0)); E.update("onchain.fee.fastest",fastest,50,"ON-CHAIN","MEMPOOL")
   async with session.get(urls["mempool"],timeout=15) as r:
    x=await r.json(); E.update("onchain.mempool.tx_count",float(x.get("count",0)),50,"ON-CHAIN","MEMPOOL"); E.update("onchain.mempool.vsize",float(x.get("vsize",0)),50,"ON-CHAIN","MEMPOOL")
   async with session.get(urls["difficulty"],timeout=15) as r:
    x=await r.json(); p=float(x.get("difficultyChange",0)); E.update("onchain.difficulty_change_pct",p,50+p*2,"ON-CHAIN","MEMPOOL")
  except Exception as ex: log.warning("mempool %s",ex)
  await asyncio.sleep(60)

async def cross_market_loop(session):
 while True:
  try:
   async with session.get("https://api.coingecko.com/api/v3/simple/price",params={"ids":"bitcoin,ethereum","vs_currencies":"usd","include_24hr_change":"true"},timeout=20) as r:d=await r.json()
   bc=float(d["bitcoin"].get("usd_24h_change") or 0); ec=float(d["ethereum"].get("usd_24h_change") or 0)
   E.update("cross.btc_24h",bc,50+bc*3,"CROSS-MARKET","COINGECKO"); E.update("cross.eth_24h",ec,50+ec*3,"CROSS-MARKET","COINGECKO"); E.update("cross.btc_vs_eth",bc-ec,50+(bc-ec)*5,"CROSS-MARKET","COINGECKO")
  except Exception as ex: log.warning("cross-market %s",ex)
  await asyncio.sleep(300)

def regime_features():
 if len(prices)<120:return
 p=list(prices); px=p[-1]
 for n in (30,60,120):
  ma=sum(p[-n:])/n; z=(px/ma-1)*100; E.update(f"regime.trend.{n}",z,50+z*12,"MARKET REGIME","OKX")
 hi=max(p[-120:]); lo=min(p[-120:]); pos=50 if hi==lo else (px-lo)/(hi-lo)*100; E.update("regime.range.120",pos,pos,"MARKET REGIME","OKX")

async def macro_loop(session):
 indicators={
  "FP.CPI.TOTL.ZG":(-2.5,"inflation"),
  "FR.INR.RINR":(-3.0,"real_rate"),
  "NY.GDP.MKTP.KD.ZG":(3.0,"gdp_growth"),
 }
 while True:
  for sid,(mul,name) in indicators.items():
   try:
    url=f"https://api.worldbank.org/v2/country/USA/indicator/{sid}"
    params={"format":"json","mrnev":"2","per_page":"2"}
    async with session.get(url,params=params,timeout=20) as r:
     if r.status!=200:
      log.warning("WorldBank %s HTTP %s %s",sid,r.status,(await r.text())[:200]); continue
     d=await r.json()
    obs=d[1] if isinstance(d,list) and len(d)>1 and isinstance(d[1],list) else []
    vals=[float(o["value"]) for o in obs if o.get("value") is not None]
    if vals:
     current=vals[0]; prev=vals[1] if len(vals)>1 else current
     delta=current-prev
     score=50+delta*mul
     E.update(f"macro.{name}",current,score,"MACRO","WORLDBANK")
     log.info("WorldBank MACRO %s value=%s delta=%s",sid,round(current,4),round(delta,4))
    else:
     log.warning("WorldBank %s no observations",sid)
   except Exception as ex:
    log.warning("WorldBank %s %s",sid,ex)
  await asyncio.sleep(21600)

