import asyncio, json, logging, os, time
from collections import deque
import aiohttp, websockets

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("btc-agent")
GROUPS=["TECHNICAL","PRICE ACTION","ORDER FLOW","DERIVATIVES","ON-CHAIN","SENTIMENT","MACRO","CROSS-MARKET","LIQUIDITY","MARKET REGIME"]
STALE={"OKX":30,"MEMPOOL":900,"ALTERNATIVE.ME":172800}
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

async def sentiment_loop(session):
 while True:
  try:
   async with session.get("https://api.alternative.me/fng/?limit=1",timeout=15) as r:
    x=await r.json(); v=float(x["data"][0]["value"]); E.update("sentiment.fear_greed",v,v,"SENTIMENT","ALTERNATIVE.ME")
  except Exception as ex: log.warning("sentiment %s",ex)
  await asyncio.sleep(3600)

def signal_label(v):
 if v is None: return "WAITING DATA"
 if v<20:return "STRONG SELL"
 if v<40:return "SELL"
 if v<60:return "HOLD"
 if v<80:return "BUY"
 return "STRONG BUY"

def telegram_report():
 s=E.status(); v=s["final"]
 lines=["₿ BTC AGENT LIVE",f"FINAL: {v if v is not None else 'N/A'} / 100  {signal_label(v)}",""]
 for g in GROUPS: lines.append(f"{g}: {s['groups'].get(g,'N/A')}")
 lines += ["",f"FEATURES ACTIVE: {s['features_active']} / {s['features_total']}"]
 return "\n".join(lines)

async def telegram_send(session,text):
 token=os.getenv("TELEGRAM_BOT_TOKEN")
 if not token:return
 chat=getattr(telegram_send,"chat_id",None)
 if not chat:
  try:
   async with session.get(f"https://api.telegram.org/bot{token}/getUpdates",timeout=20) as r:
    x=await r.json()
    for u in reversed(x.get("result",[])):
     msg=u.get("message") or u.get("channel_post") or {}
     if msg.get("chat",{}).get("id") is not None:
      chat=str(msg["chat"]["id"]); telegram_send.chat_id=chat; break
  except Exception as ex: log.warning("telegram discover chat %s",ex)
 if not chat:return
 try:
  async with session.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":text},timeout=20) as r:
   if r.status>=300: log.warning("telegram send HTTP %s %s",r.status,await r.text())
 except Exception as ex: log.warning("telegram send %s",ex)

async def telegram_bot_loop(session):
 token=os.getenv("TELEGRAM_BOT_TOKEN")
 if not token:
  log.warning("TELEGRAM_BOT_TOKEN missing"); return
 offset=0; last_report=0; enabled=True
 while True:
  try:
   async with session.get(f"https://api.telegram.org/bot{token}/getUpdates",params={"timeout":25,"offset":offset},timeout=35) as r:
    x=await r.json()
   for u in x.get("result",[]):
    offset=u["update_id"]+1
    msg=u.get("message") or {}; chat=msg.get("chat",{}).get("id"); cmd=(msg.get("text") or "").split()[0].lower()
    if chat is None: continue
    telegram_send.chat_id=str(chat)
    if cmd=="/start":
     enabled=True; await telegram_send(session,"BTC Agent LIVE: ON\nBáo cáo tự động mỗi 5 phút.")
    elif cmd=="/stop":
     enabled=False; await telegram_send(session,"BTC Agent: OFF\nDùng /start để bật lại.")
    elif cmd in ("/status","/score"):
     await telegram_send(session,telegram_report())
   if enabled and getattr(telegram_send,"chat_id",None) and time.time()-last_report>=300:
    await telegram_send(session,telegram_report()); last_report=time.time()
  except Exception as ex:
   log.warning("telegram polling %s",ex); await asyncio.sleep(3)

async def http_handler(reader,writer):
 try:
  line=await reader.readline(); path=line.decode(errors="ignore").split(" ")[1] if b" " in line else "/"
  while True:
   h=await reader.readline()
   if h in (b"\r\n",b"\n",b""): break
  if path=="/health":
   body=json.dumps({"ok":True,**E.status()}).encode(); ct="application/json"
  else:
   s=E.status(); rows="".join(f"<tr><td>{g}</td><td>{s['groups'].get(g,'N/A')}</td></tr>" for g in GROUPS)
   body=f"""<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="5"><title>BTC Agent Live</title><style>
body{{font-family:system-ui;background:#050b0f;color:#eef4ff;max-width:950px;margin:auto;padding:20px}}h1{{font-size:30px}}.card{{border:1px solid #19313d;border-radius:18px;padding:24px;margin:18px 0;background:#071118}}.score{{font-size:58px;font-weight:900}}.live{{color:#00df79}}table{{width:100%;border-collapse:collapse}}td{{padding:12px 5px;border-bottom:1px solid #18303a}}td:nth-child(2){{text-align:right;font-weight:800;width:70px}}.bar{{height:20px;border-radius:12px;background:linear-gradient(90deg,#ff2828 0%,#ff7b22 25%,#ffd400 50%,#65df3c 75%,#00d878 100%);position:relative;overflow:hidden}}.marker{{position:absolute;top:0;width:4px;height:100%;background:white}}.finalbar{{height:34px}}.ticks{{display:flex;justify-content:space-between;color:#91a3bb;font-size:11px;margin-top:8px}}.na{{color:#71808d}}small{{color:#91a3bb}}@media(max-width:600px){{body{{padding:14px}}.score{{font-size:46px}}.card{{padding:17px}}td{{font-size:13px}}}}
</style></head><body><h1>₿ BTC AGENT <span class="live">LIVE</span></h1><small>AI-POWERED · REAL-TIME · EVENT-DRIVEN</small>
<div class="card"><small>FINAL SCORE</small><div class="score">{{s['final'] if s['final'] is not None else 'N/A'}} / 100</div><h2>{{signal_label(s['final'])}}</h2><div class="bar finalbar"><div class="marker" style="left:{{s['final'] or 0}}%"></div></div><div class="ticks"><span>0 BÁN MẠNH</span><span>20 BÁN</span><span>40</span><span>60 MUA</span><span>80</span><span>100 MUA MẠNH</span></div></div>
<div class="card"><h2>ĐIỂM THEO NHÓM (10 NHÓM)</h2><table>{{''.join("<tr><td>"+g+"</td><td>"+str(s['groups'].get(g,'N/A'))+"</td><td>"+(("<div class='bar'><div class='marker' style='left:"+str(s['groups'][g])+"%'></div></div>") if g in s['groups'] else "<span class='na'>Chưa có dữ liệu</span>")+"</td></tr>" for g in GROUPS)}}</table><p><b>{{s['features_active']}} / {{s['features_total']}}</b> <span class="live">●</span> FEATURES ACTIVE</p></div>
<div class="card"><b class="live">● LIVE</b><p>Cập nhật giao diện mỗi 5 giây · Engine tính lại ngay khi có dữ liệu mới.</p></div></body></html>""".encode(); ct="text/html; charset=utf-8"
  writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()+body); await writer.drain()
 except Exception as ex: log.warning("http %s",ex)
 finally:
  writer.close(); await writer.wait_closed()

async def http_server():
 port=int(os.getenv("PORT","8080")); server=await asyncio.start_server(http_handler,"0.0.0.0",port)
 log.info("HTTP listening on %s",port)
 async with server: await server.serve_forever()

async def heartbeat():
 while True: log.info("STATE %s",json.dumps(E.status(),ensure_ascii=False)); await asyncio.sleep(10)

async def main():
 async with aiohttp.ClientSession(headers={"User-Agent":"btc-agent-live/1.0"}) as s:
  await asyncio.gather(okx_ws(),mempool_loop(s),sentiment_loop(s),telegram_bot_loop(s),http_server(),heartbeat())
if __name__=="__main__": asyncio.run(main())
