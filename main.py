import asyncio, json, logging, os, time
from collections import deque
import aiohttp, websockets

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("btc-agent")
GROUPS=["TECHNICAL","PRICE ACTION","ORDER FLOW","DERIVATIVES","ON-CHAIN","SENTIMENT","MACRO","CROSS-MARKET","LIQUIDITY","MARKET REGIME"]
STALE={"OKX":30,"MEMPOOL":900,"ALTERNATIVE.ME":172800,"COINGECKO":900,"FRED":172800}
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
   url="https://api.coingecko.com/api/v3/simple/price"
   async with session.get(url,params={"ids":"bitcoin,ethereum","vs_currencies":"usd","include_24hr_change":"true"},timeout=20) as r:x=await r.json()
   bc=float(x["bitcoin"].get("usd_24h_change") or 0); ec=float(x["ethereum"].get("usd_24h_change") or 0)
   E.update("cross.btc_24h_change",bc,50+bc*3,"CROSS-MARKET","COINGECKO")
   E.update("cross.eth_24h_change",ec,50+ec*3,"CROSS-MARKET","COINGECKO")
   E.update("cross.btc_vs_eth_24h",bc-ec,50+(bc-ec)*5,"CROSS-MARKET","COINGECKO")
  except Exception as ex: log.warning("cross-market %s",ex)
  await asyncio.sleep(300)

def regime_features():
 if len(prices)<120:return
 p=list(prices); px=p[-1]
 for n in (30,60,120):
  ma=sum(p[-n:])/n; trend=(px/ma-1)*100
  E.update(f"regime.trend.{n}",trend,50+trend*12,"MARKET REGIME","OKX")
 rets=[(p[i]/p[i-1]-1)*100 for i in range(len(p)-59,len(p)) if p[i-1]]
 if rets:
  vol=(sum(x*x for x in rets)/len(rets))**0.5
  E.update("regime.volatility.60",vol,50+(0.12-vol)*120,"MARKET REGIME","OKX")
 hi=max(p[-120:]); lo=min(p[-120:]); pos=50 if hi==lo else (px-lo)/(hi-lo)*100
 E.update("regime.range_position.120",pos,pos,"MARKET REGIME","OKX")

async def macro_loop(session):
 key=os.getenv("FRED_API_KEY")
 if not key:
  log.warning("FRED_API_KEY missing; MACRO remains N/A")
  return
 series={"DGS10":-1,"DFF":-1,"DTWEXBGS":-1}
 while True:
  for sid,direction in series.items():
   try:
    url="https://api.stlouisfed.org/fred/series/observations"
    params={"series_id":sid,"api_key":key,"file_type":"json","sort_order":"desc","limit":2}
    async with session.get(url,params=params,timeout=20) as r:x=await r.json()
    vals=[float(o["value"]) for o in x.get("observations",[]) if o.get("value") not in (None,".")]
    if vals:
     delta=vals[0]-vals[1] if len(vals)>1 else 0
     E.update(f"macro.{sid}.level",vals[0],50+direction*delta*10,"MACRO","FRED")
     if len(vals)>1:E.update(f"macro.{sid}.change",delta,50+direction*delta*20,"MACRO","FRED")
   except Exception as ex: log.warning("FRED %s %s",sid,ex)
  await asyncio.sleep(1800)

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
  s=E.status()
  if path=="/health": body=json.dumps({"ok":True,**s}).encode(); ct="application/json"
  else:
   final=s["final"]; fv=50 if final is None else final; final_txt="N/A" if final is None else f"{final:.2f}"
   def state(v):
    if v is None:return ("N/A","Chưa có dữ liệu","#536475")
    if v<20:return ("Bán mạnh","Rất yếu","#ff3030")
    if v<40:return ("Bán","Yếu","#ff812d")
    if v<60:return ("Trung lập","Trung lập","#ffd400")
    if v<80:return ("Tích cực","Tích cực","#63df3d")
    return ("Mua mạnh","Rất tích cực","#00df79")
   rows=[]
   for g in GROUPS:
    v=s["groups"].get(g); st,desc,col=state(v)
    if v is None: rows.append(f"<div class='grow'><span>{g}</span><b class='na'>N/A</b><div class='gtrack'></div><em class='na'>Chưa có dữ liệu</em></div>")
    else: rows.append(f"<div class='grow'><span>{g}</span><b>{v:.2f}</b><div class='gtrack'><i style='width:{v}%;background:{col}'></i></div><em>{desc}</em></div>")
   rows_html="".join(rows); st,desc,col=state(final)
   body=f"""<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="5"><title>BTC Agent Live</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at 50% 0,#0b1820,#03080c 55%);color:#eef4ff;font-family:system-ui,-apple-system,sans-serif}}.wrap{{max-width:1050px;margin:auto;padding:25px}}.top{{display:flex;justify-content:space-between;align-items:center}}.brand{{font-size:34px;font-weight:900}}.green{{color:#00df79}}.muted,.na{{color:#8294aa}}.live{{color:#00df79;font-weight:900;font-size:20px}}.card{{border:1px solid #19313d;background:rgba(5,15,21,.82);border-radius:18px;padding:28px;margin:25px 0}}.hero{{display:grid;grid-template-columns:1.2fr .8fr;gap:30px}}.score{{font-size:68px;font-weight:900;line-height:1.1}}.sig{{font-size:38px;font-weight:900;color:{col};margin-top:5px}}.market{{border:1px solid #41421b;border-radius:16px;padding:25px;background:#111509}}.market b{{font-size:27px;color:{col}}}.scale{{height:38px;border-radius:25px;background:linear-gradient(90deg,#ff2828,#ff7b22 25%,#ffd400 48%,#65df3c 72%,#00d878);position:relative;margin-top:30px}}.pointer{{position:absolute;left:{fv}%;top:-8px;width:4px;height:54px;background:#fff;border-radius:4px}}.bubble{{position:absolute;left:{fv}%;top:-48px;transform:translateX(-50%);background:#15222c;border:1px solid #506477;padding:5px 10px;border-radius:8px;font-weight:900}}.ticks,.zones{{display:flex;justify-content:space-between;margin-top:10px}}.ticks{{color:#91a3bb}}.zones{{font-weight:800}}.red{{color:#ff3939}}.orange{{color:#ff812d}}.yellow{{color:#ffd400}}.groupsHead{{display:flex;justify-content:space-between;align-items:center}}.grow{{display:grid;grid-template-columns:205px 75px 1fr 125px;gap:15px;align-items:center;padding:11px 0;border-bottom:1px solid #142a35}}.grow span{{color:#b5c6dd;font-size:18px}}.grow b{{text-align:right}}.grow em{{font-style:normal;color:#aab8cb}}.gtrack{{height:22px;background:#1c2a34;border-radius:14px;overflow:hidden}}.gtrack i{{display:block;height:100%;border-radius:14px}}.summary{{display:grid;grid-template-columns:repeat(4,1fr);gap:18px}}.mini{{min-height:155px;border:1px solid #183542;background:#071219;border-radius:16px;padding:22px}}.mini strong{{display:block;font-size:27px;margin:14px 0}}.blue{{color:#35a9ff}}.footer{{display:flex;justify-content:space-between;color:#7f92a8;padding:35px 8px 15px;border-top:1px solid #142b36;margin-top:55px}}
@media(max-width:720px){{.wrap{{padding:7px 9px}}.top{{margin:0}}.brand{{font-size:20px}}.top .muted{{display:none}}.card{{padding:10px 11px;margin:8px 0;border-radius:13px}}.hero{{grid-template-columns:1fr;gap:8px}}.score{{font-size:38px}}.sig{{font-size:24px;margin:2px 0}}.market{{padding:10px}}.market b{{font-size:18px}}.market p{{font-size:11px;margin:4px 0}}.scale{{height:20px;margin-top:18px}}.pointer{{height:32px;top:-6px}}.bubble{{top:-35px;font-size:11px;padding:3px 6px}}.zones{{font-size:8px;margin-top:3px}}.ticks{{font-size:9px;margin-top:3px}}.groupsHead{{margin-bottom:3px}}.groupsHead h2{{font-size:15px;margin:5px 0}}.groupsHead .muted{{font-size:9px}}.grow{{grid-template-columns:96px 46px 1fr;gap:5px;padding:5px 0}}.grow span{{font-size:10px}}.grow b{{font-size:10px}}.grow em{{display:none}}.gtrack{{height:11px}}.summary{{grid-template-columns:1fr 1fr;gap:6px}}.mini{{padding:9px;min-height:88px}}.mini strong{{font-size:15px;margin:6px 0}}.mini span{{font-size:9px}}.footer{{font-size:9px;padding:12px 3px;margin-top:10px}}}}</style></head><body><div class="wrap"><div class="top"><div><div class="brand">₿ BTC AGENT <span class="green">LIVE</span></div><div class="muted">AI-POWERED · REAL-TIME · {s["features_active"]}/{s["features_total"]} FEATURES</div></div><div class="live">● LIVE<br><small class="muted">Cập nhật tự động</small></div></div>
<div class="card"><div class="hero"><div><b class="muted">FINAL SCORE</b><div class="score">{final_txt} <span class="muted">/ 100</span></div><div class="sig">{signal_label(final)}</div></div><div class="market"><div class="muted">TRẠNG THÁI THỊ TRƯỜNG</div><b>{st.upper()}</b><p class="muted">{desc}</p></div></div><div class="scale"><div class="bubble">{final_txt}</div><div class="pointer"></div></div><div class="ticks"><span>0</span><span>20</span><span>40</span><span>60</span><span>80</span><span>100</span></div><div class="zones"><span class="red">BÁN MẠNH</span><span class="orange">BÁN</span><span class="yellow">TRUNG LẬP</span><span class="green">MUA</span><span class="green">MUA MẠNH</span></div></div>
<div class="card"><div class="groupsHead"><h2>ĐIỂM THEO NHÓM (10 NHÓM)</h2><b>{s["features_active"]} / {s["features_total"]} <span class="green">●</span> <span class="muted">FEATURES ACTIVE</span></b></div>{rows_html}</div>
<div class="summary"><div class="mini"><span class="muted">GIÁ BTC</span><strong>BTC-USDT</strong><span class="green">● OKX LIVE</span></div><div class="mini"><span class="muted">XU HƯỚNG NGẮN HẠN</span><strong style="color:{col}">{st.upper()}</strong><span class="muted">Theo điểm tổng hợp</span></div><div class="mini"><span class="muted">ĐỘ TIN CẬY</span><strong class="green">{len(s["groups"])} / 10 NHÓM</strong><span class="muted">{s["features_active"]}/{s["features_total"]} features</span></div><div class="mini"><span class="muted">CẬP NHẬT</span><strong class="blue">MỖI 5 GIÂY ↻</strong><span class="muted">Live engine event-driven</span></div></div>
<div class="footer"><b>BTC AGENT</b><span>Live engine is event-driven · Railway</span></div></div></body></html>""".encode(); ct="text/html; charset=utf-8"
  writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nCache-Control: no-store\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()+body); await writer.drain()
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
  await asyncio.gather(okx_ws(),mempool_loop(s),sentiment_loop(s),cross_market_loop(s),macro_loop(s),telegram_bot_loop(s),http_server(),heartbeat())
if __name__=="__main__": asyncio.run(main())
