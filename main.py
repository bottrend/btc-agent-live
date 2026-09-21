import asyncio, json, logging, os, time, sqlite3
from collections import deque
import aiohttp, websockets

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("btc-agent")
GROUPS=["TECHNICAL","PRICE ACTION","ORDER FLOW","DERIVATIVES","ON-CHAIN","SENTIMENT","MACRO","CROSS-MARKET","LIQUIDITY","MARKET REGIME"]
STALE={"OKX":30,"MEMPOOL":900,"ALTERNATIVE.ME":172800,"COINGECKO":900,"WORLDBANK":2592000}
def clamp(x): return max(0,min(100,float(x)))

TF_WEIGHTS={
 "5M":{"PRICE ACTION":0.28,"ORDER FLOW":0.28,"LIQUIDITY":0.20,"TECHNICAL":0.10,"MARKET REGIME":0.10,"DERIVATIVES":0.04},
 "15M":{"PRICE ACTION":0.24,"ORDER FLOW":0.22,"LIQUIDITY":0.16,"TECHNICAL":0.16,"MARKET REGIME":0.14,"DERIVATIVES":0.08},
 "1H":{"PRICE ACTION":0.18,"ORDER FLOW":0.14,"LIQUIDITY":0.10,"TECHNICAL":0.20,"MARKET REGIME":0.18,"DERIVATIVES":0.12,"CROSS-MARKET":0.08},
 "4H":{"PRICE ACTION":0.12,"ORDER FLOW":0.08,"LIQUIDITY":0.06,"TECHNICAL":0.22,"MARKET REGIME":0.20,"DERIVATIVES":0.14,"CROSS-MARKET":0.08,"ON-CHAIN":0.06,"SENTIMENT":0.04},
 "1D":{"PRICE ACTION":0.08,"TECHNICAL":0.18,"MARKET REGIME":0.18,"DERIVATIVES":0.10,"CROSS-MARKET":0.12,"ON-CHAIN":0.12,"SENTIMENT":0.10,"MACRO":0.12},
}
def timeframe_finals(groups):
 out={}
 for tf,weights in TF_WEIGHTS.items():
  pairs=[(groups[g],w) for g,w in weights.items() if g in groups]
  sw=sum(w for _,w in pairs)
  out[tf]=None if not sw else sum(v*w for v,w in pairs)/sw
 return out

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
E=Engine(); prices=deque(maxlen=5000); last_trade_side=None; btc_market={"price":None,"changes":{}}
ANALYTICS_DB=os.getenv("ANALYTICS_DB","/tmp/btc_analytics.db")
HORIZONS={"5M":300,"15M":900,"1H":3600,"4H":14400,"1D":86400}
def analytics_init():
 db=sqlite3.connect(ANALYTICS_DB); db.execute("CREATE TABLE IF NOT EXISTS snapshots(ts REAL PRIMARY KEY, price REAL, scores TEXT)"); db.commit(); db.close()
def analytics_stats():
 try:
  db=sqlite3.connect(ANALYTICS_DB); rows=db.execute("SELECT ts,price,scores FROM snapshots ORDER BY ts").fetchall(); db.close()
  parsed=[(t,p,json.loads(s)) for t,p,s in rows]; out={}
  def median(vals):
   if not vals:return None
   a=sorted(vals); m=len(a)//2
   return a[m] if len(a)%2 else (a[m-1]+a[m])/2
  def dominant(vals):
   if not vals:return None
   counts={}
   for v in vals: counts[v]=counts.get(v,0)+1
   return max(counts,key=counts.get)
  for name in GROUPS+["FINAL"]:
   out[name]={}
   for h,sec in HORIZONS.items():
    hit=n=up_hit=up_n=down_hit=down_n=0
    up_fav=[]; up_adv=[]; down_fav=[]; down_adv=[]
    up_end=[]; down_end=[]; up_score=[]; down_score=[]
    up_tmfe=[]; up_tmae=[]; down_tmfe=[]; down_tmae=[]
    up_ctx=[]; down_ctx=[]
    for i,(t,p,scores) in enumerate(parsed):
     sc=scores.get(name)
     if sc is None or 40<=sc<=60: continue
     target=t+sec; j=i+1
     while j<len(parsed) and parsed[j][0]<target: j+=1
     if j>=len(parsed): continue
     path=[parsed[k][1] for k in range(i,j+1)]
     end_move=(parsed[j][1]/p-1)*100
     hi=max(path); lo=min(path); hi_k=path.index(hi); lo_k=path.index(lo)
     high_move=(hi/p-1)*100; low_move=(lo/p-1)*100
     regime=scores.get("MARKET REGIME")
     trend="BULL" if regime is not None and regime>60 else "BEAR" if regime is not None and regime<40 else "SIDE"
     look=[parsed[k][1] for k in range(max(0,i-30),i+1)]
     if len(look)>2:
      rets=[abs((look[k]/look[k-1]-1)*100) for k in range(1,len(look))]
      rv=sum(rets)/len(rets)
      vol="HIGH VOL" if rv>=0.08 else "LOW VOL" if rv<0.03 else "MID VOL"
     else: vol="VOL N/A"
     ctx=f"{trend} · {vol}"
     ok=(sc>60 and end_move>0) or (sc<40 and end_move<0)
     if ok: hit+=1
     n+=1
     if sc>60:
      up_n+=1; up_fav.append(max(0,high_move)); up_adv.append(min(0,low_move))
      up_end.append(end_move); up_score.append(sc); up_tmfe.append(hi_k); up_tmae.append(lo_k); up_ctx.append(ctx)
      if end_move>0: up_hit+=1
     else:
      down_n+=1; down_fav.append(min(0,low_move)); down_adv.append(max(0,high_move))
      down_end.append(end_move); down_score.append(sc); down_tmfe.append(lo_k); down_tmae.append(hi_k); down_ctx.append(ctx)
      if end_move<0: down_hit+=1
    out[name][h]={
     "all":(100*hit/n if n else None,n),
     "up":(100*up_hit/up_n if up_n else None,up_n),
     "down":(100*down_hit/down_n if down_n else None,down_n),
     "up_range":(median(up_fav),median(up_adv)), "down_range":(median(down_fav),median(down_adv)),
     "up_end":median(up_end), "down_end":median(down_end),
     "up_score":median(up_score), "down_score":median(down_score),
     "up_time":(median(up_tmfe),median(up_tmae)), "down_time":(median(down_tmfe),median(down_tmae)),
     "up_context":dominant(up_ctx), "down_context":dominant(down_ctx)
    }
  return out
 except Exception as ex:
  log.warning("analytics stats %s",ex); return {}

def price_features(px):
 prices.append(px); p=list(prices)
 if len(p)>1:
  for n in (1,2,3,4,5,6,8,10,12,15,20,25,30,45,60,75,90,120,150,180,240,300,450,600,900,1200):
   if len(p)>n:
    r=(px/p[-n-1]-1)*100
    E.update(f"price.return.tick_{n}",r,50+r*12,"PRICE ACTION","OKX")
 for n in (5,8,10,13,15,20,21,25,30,34,40,50,55,60,75,89,100,120,144,180,200,233,300,377,500):
  if len(p)>=n:
   w=p[-n:]; ma=sum(w)/n; d=(px/ma-1)*100
   E.update(f"technical.sma_distance.{n}",d,50+d*10,"TECHNICAL","OKX")
   if n>=10:
    mean=ma; var=sum((x-mean)**2 for x in w)/n; sd=var**0.5
    z=0 if sd==0 else (px-mean)/sd
    E.update(f"technical.zscore.{n}",z,50+z*10,"TECHNICAL","OKX")
    vol=0 if mean==0 else sd/mean*100
    E.update(f"technical.volatility.{n}",vol,50+(1.5-vol)*5,"TECHNICAL","OKX")
 for n in (10,15,20,25,30,40,50,60,75,100,120,150,180,240,300,500,900):
  if len(p)>=n:
   w=p[-n:]; hi=max(w); lo=min(w); pos=50 if hi==lo else (px-lo)/(hi-lo)*100
   E.update(f"price.range_position.{n}",pos,pos,"PRICE ACTION","OKX")
   E.update(f"price.drawdown_from_high.{n}",(px/hi-1)*100,50+(px/hi-1)*500,"PRICE ACTION","OKX")
   E.update(f"price.distance_from_low.{n}",(px/lo-1)*100,50+(px/lo-1)*500,"PRICE ACTION","OKX")
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
       px=float(d["last"]); btc_market["price"]=px; price_features(px)
       for key,mul in (("askPx",1),("bidPx",1)):
        if d.get(key): E.update("market."+key,float(d[key]),50,"LIQUIDITY","OKX")
       if d.get("askPx") and d.get("bidPx"):
        ask,bid=float(d["askPx"]),float(d["bidPx"]); mid=(ask+bid)/2; spread=(ask-bid)/mid*10000
        E.update("liquidity.spread_bps",spread,50-spread*8,"LIQUIDITY","OKX")
      elif ch=="books5":
       bids=d.get("bids",[]); asks=d.get("asks",[])
       for depth in (1,2,3,4,5):
        bv=sum(float(x[1]) for x in bids[:depth]); av=sum(float(x[1]) for x in asks[:depth]); tot=bv+av
        if tot:
         imb=(bv-av)/tot
         E.update(f"orderflow.imbalance.depth_{depth}",imb,50+50*imb,"ORDER FLOW","OKX")
         if depth in (1,2,3,4,5):
          E.update(f"liquidity.book_depth_ratio.{depth}",bv/av if av else 10,50+25*imb,"LIQUIDITY","OKX")
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

async def btc_timeframe_loop(session):
 # Real OKX candle data; UI-only market context, not extra scoring features.
 frames={"5m":"5m","15m":"15m","1h":"1H","4h":"4H","1d":"1D"}
 while True:
  for label,bar in frames.items():
   try:
    async with session.get("https://www.okx.com/api/v5/market/candles",params={"instId":"BTC-USDT","bar":bar,"limit":"2"},timeout=15) as r:
     d=await r.json()
    rows=d.get("data") or []
    if rows:
     latest=rows[0]; op=float(latest[1]); last=float(latest[4])
     btc_market["changes"][label]=0 if op==0 else (last/op-1)*100
   except Exception as ex: log.warning("BTC timeframe %s %s",label,ex)
  await asyncio.sleep(30)

async def cross_market_loop(session):
 while True:
  try:
   async with session.get("https://api.coingecko.com/api/v3/simple/price",params={"ids":"bitcoin,ethereum","vs_currencies":"usd","include_24hr_change":"true"},timeout=20) as r:d=await r.json()
   bc=float(d["bitcoin"].get("usd_24h_change") or 0); ec=float(d["ethereum"].get("usd_24h_change") or 0)
   E.update("cross.btc_24h",bc,50+bc*3,"CROSS-MARKET","COINGECKO"); E.update("cross.eth_24h",ec,50+ec*3,"CROSS-MARKET","COINGECKO"); E.update("cross.btc_vs_eth",bc-ec,50+(bc-ec)*5,"CROSS-MARKET","COINGECKO")
  except Exception as ex: log.warning("cross-market %s",ex)
  await asyncio.sleep(300)

def regime_features():
 if len(prices)<30:return
 p=list(prices); px=p[-1]
 for n in (30,45,60,90,120,180,240,300,450,600,900,1200):
  if len(p)>=n:
   w=p[-n:]; ma=sum(w)/n; z=(px/ma-1)*100
   E.update(f"regime.trend.{n}",z,50+z*12,"MARKET REGIME","OKX")
   hi=max(w); lo=min(w); pos=50 if hi==lo else (px-lo)/(hi-lo)*100
   E.update(f"regime.range.{n}",pos,pos,"MARKET REGIME","OKX")

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
     current=vals[0]; prev=vals[1] if len(vals)>1 else current; delta=current-prev
     E.update(f"macro.{name}",current,50+delta*mul,"MACRO","WORLDBANK")
     log.info("WorldBank MACRO %s value=%s delta=%s",sid,round(current,4),round(delta,4))
    else: log.warning("WorldBank %s no observations",sid)
   except Exception as ex: log.warning("WorldBank %s %s",sid,ex)
  await asyncio.sleep(21600)

async def sentiment_loop(session):
 while True:
  try:
   async with session.get("https://api.alternative.me/fng/?limit=1",timeout=15) as r:
    x=await r.json(); v=float(x["data"][0]["value"]); E.update("sentiment.fear_greed",v,v,"SENTIMENT","ALTERNATIVE.ME")
  except Exception as ex: log.warning("sentiment %s",ex)
  await asyncio.sleep(3600)

def market_change_html(label):
 v=btc_market["changes"].get(label)
 display=label.upper()
 if v is None: return f'<b style="color:#fff">{display}</b> N/A'
 color="#00df79" if v>=0 else "#ff4d4d"
 return f'<b style="color:#fff">{display}</b> <span style="color:{color};font-weight:700">{v:+.2f}%</span>'

def signal_label(v):
 if v is None: return "WAITING DATA"
 if v<20:return "STRONG SELL"
 if v<40:return "SELL"
 if v<60:return "HOLD"
 if v<80:return "BUY"
 return "STRONG BUY"

def telegram_report():
 s=E.status(); v=s["final"]
 tf=timeframe_finals(E.groups)
 tf_line=" · ".join(f"{k}: {('N/A' if x is None else f'{x:.2f}')}" for k,x in tf.items())
 lines=["₿ BTC AGENT LIVE",f"FINAL: {v if v is not None else 'N/A'} / 100  {signal_label(v)}",tf_line,""]
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
     enabled=True; await telegram_send(session,"BTC Agent LIVE: ON\nAutomatic report every 5 minutes.")
    elif cmd=="/stop":
     enabled=False; await telegram_send(session,"BTC Agent: OFF\nUse /start to turn it back on.")
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
   final=s["final"]; final_txt="N/A" if final is None else f"{final:.2f}"; final_pos=0 if final is None else final
   rows=[]
   for g in GROUPS:
    v=s["groups"].get(g)
    if v is None: rows.append(f"<tr><td>{g}</td><td class=\"na\">N/A</td><td class=\"na\">No data yet</td></tr>")
    else: rows.append(f"<tr><td>{g}</td><td><b>{v:.2f}</b></td><td><div class=\"bar\"><i style=\"left:{v}%\"></i></div></td></tr>")
   rows_html="".join(rows)
   tf_scores=timeframe_finals(E.groups)
   tf_html="".join(f'<div class="tfitem"><b>{tf}</b><span style="color:{("#00df79" if v is not None and v>=60 else "#ff4d4d" if v is not None and v<40 else "#ffd400")}">{("N/A" if v is None else f"{v:.2f}")}</span></div>' for tf,v in tf_scores.items())
   ast=analytics_stats()
   acc_cards=[]
   for name in GROUPS+["FINAL"]:
    vals=ast.get(name,{})
    rows=[]
    for h in HORIZONS:
     stat=vals.get(h,{})
     up=stat.get("up",(None,0)); down=stat.get("down",(None,0))
     up_txt="N/A" if up[0] is None else f"{up[0]:.1f}%"
     down_txt="N/A" if down[0] is None else f"{down[0]:.1f}%"
     ur=stat.get("up_range",(None,None)); dr=stat.get("down_range",(None,None))
     ur_txt="" if ur[0] is None else f" · {ur[0]:+.2f}%/{ur[1]:+.2f}%"
     dr_txt="" if dr[0] is None else f" · {dr[0]:+.2f}%/{dr[1]:+.2f}%"
     rows.append(f'<tr><td>{h}</td><td><div class="dirline">↑ {up_txt} ({up[1]}){ur_txt}</div><div class="dirline">↓ {down_txt} ({down[1]}){dr_txt}</div></td></tr>')
    acc_cards.append(f'<table class="accuracy miniacc"><tr><th>TIME</th><th>{name}</th></tr>{"".join(rows)}</table>')
   accuracy_html="".join(acc_cards)
   def metric_cards(kind):
    cards=[]
    for name in GROUPS+["FINAL"]:
     vals=ast.get(name,{}); mrows=[]
     for h in HORIZONS:
      st=vals.get(h,{})
      if kind=="end":
       u=st.get("up_end"); d=st.get("down_end")
       ut="N/A" if u is None else f"{u:+.2f}%"; dt="N/A" if d is None else f"{d:+.2f}%"
       detail=f'<div class="dirline">↑ {ut}</div><div class="dirline">↓ {dt}</div>'
      elif kind=="score":
       u=st.get("up_score"); d=st.get("down_score")
       ut="N/A" if u is None else f"{u:.2f}"; dt="N/A" if d is None else f"{d:.2f}"
       detail=f'<div class="dirline">↑ {ut}</div><div class="dirline">↓ {dt}</div>'
      elif kind=="time":
       u=st.get("up_time",(None,None)); d=st.get("down_time",(None,None))
       ut="N/A" if u[0] is None else f"MFE {u[0]:.0f}m · MAE {u[1]:.0f}m"
       dt="N/A" if d[0] is None else f"MFE {d[0]:.0f}m · MAE {d[1]:.0f}m"
       detail=f'<div class="dirline">↑ {ut}</div><div class="dirline">↓ {dt}</div>'
      else:
       u=st.get("up_context") or "N/A"; d=st.get("down_context") or "N/A"
       detail=f'<div class="dirline">↑ {u}</div><div class="dirline">↓ {d}</div>'
      mrows.append(f'<tr><td>{h}</td><td>{detail}</td></tr>')
     cards.append(f'<table class="accuracy miniacc"><tr><th>TIME</th><th>{name}</th></tr>{"".join(mrows)}</table>')
    return "".join(cards)
   endmove_html=metric_cards("end")
   strength_html=metric_cards("score")
   timing_html=metric_cards("time")
   context_html=metric_cards("context")
   # Compact FINAL-only rollup: one table per horizon, UP/DOWN side by side.
   summary_cards=[]
   settings_cards=[]
   for h in HORIZONS:
    st=ast.get("FINAL",{}).get(h,{})
    def side_vals(side):
     acc,n=st.get(side,(None,0)); rng=st.get(side+"_range",(None,None))
     return {"acc":acc,"n":n,"mfe":rng[0],"mae":rng[1],"end":st.get(side+"_end"),"score":st.get(side+"_score"),"time":st.get(side+"_time",(None,None)),"ctx":st.get(side+"_context") or "N/A"}
    u=side_vals("up"); d=side_vals("down")
    def pct(v): return "N/A" if v is None else f"{v:+.2f}%"
    def num(v): return "N/A" if v is None else f"{v:.2f}"
    def acc(v): return "N/A" if v is None else f"{v:.1f}%"
    def mins(v): return "N/A" if v is None else f"{v:.0f}m"
    summary_cards.append(f"""<table class="accuracy summary"><tr><th>{h}</th><th>↑ LONG</th><th>↓ SHORT</th></tr>
<tr><td>Accuracy</td><td>{acc(u["acc"])} ({u["n"]})</td><td>{acc(d["acc"])} ({d["n"]})</td></tr>
<tr><td>MFE</td><td>{pct(u["mfe"])}</td><td>{pct(d["mfe"])}</td></tr>
<tr><td>MAE</td><td>{pct(u["mae"])}</td><td>{pct(d["mae"])}</td></tr>
<tr><td>End Move</td><td>{pct(u["end"])}</td><td>{pct(d["end"])}</td></tr>
<tr><td>Score</td><td>{num(u["score"])}</td><td>{num(d["score"])}</td></tr>
<tr><td>MFE Time</td><td>{mins(u["time"][0])}</td><td>{mins(d["time"][0])}</td></tr>
<tr><td>MAE Time</td><td>{mins(u["time"][1])}</td><td>{mins(d["time"][1])}</td></tr>
<tr><td>Context</td><td>{u["ctx"]}</td><td>{d["ctx"]}</td></tr></table>""")
    # Suggested settings are deliberately gated at n>=1000. Values shown are direct
    # summaries of observed medians; they are candidates for paper testing, not invented confidence.
    def setting(v,side):
     if v["n"]<1000: return {"entry":"COLLECTING","tp":"—","sl":"—","hold":"—","ctx":v["ctx"],"status":f'n={v["n"]} / 1000'}
     entry=("≥ "+num(v["score"])) if side=="up" else ("≤ "+num(v["score"]))
     tp=pct(v["mfe"]); sl=pct(v["mae"]); hold=h
     return {"entry":entry,"tp":tp,"sl":sl,"hold":hold,"ctx":v["ctx"],"status":"PAPER TEST"}
    us=setting(u,"up"); ds=setting(d,"down")
    settings_cards.append(f"""<table class="accuracy summary"><tr><th>{h}</th><th>↑ LONG</th><th>↓ SHORT</th></tr>
<tr><td>Accuracy</td><td>{acc(u["acc"])} ({u["n"]})</td><td>{acc(d["acc"])} ({d["n"]})</td></tr>
<tr><td>Entry Score</td><td>{us["entry"]}</td><td>{ds["entry"]}</td></tr>
<tr><td>TP candidate</td><td>{us["tp"]}</td><td>{ds["tp"]}</td></tr>
<tr><td>SL candidate</td><td>{us["sl"]}</td><td>{ds["sl"]}</td></tr>
<tr><td>Max Hold</td><td>{us["hold"]}</td><td>{ds["hold"]}</td></tr>
<tr><td>Context</td><td>{us["ctx"]}</td><td>{ds["ctx"]}</td></tr>
<tr><td>Status</td><td>{us["status"]}</td><td>{ds["status"]}</td></tr></table>""")
   summary_html="".join(summary_cards)
   settings_html="".join(settings_cards)
   body=f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="5"><title>BTC Agent Live</title><style>
*{{box-sizing:border-box}}body{{font-family:system-ui;background:#050b0f;color:#eef4ff;max-width:950px;margin:auto;padding:20px}}h1{{font-size:30px;margin-bottom:4px}}.card{{border:1px solid #19313d;border-radius:18px;padding:24px;margin:18px 0;background:#071118}}.score{{font-size:58px;font-weight:900}}.live{{color:#00df79}}table{{width:100%;border-collapse:collapse}}td{{padding:12px 5px;border-bottom:1px solid #18303a}}td:nth-child(2){{text-align:right;width:75px}}table:not(.accuracy) td:nth-child(3){{width:55%}}.bar{{height:22px;border-radius:12px;background:linear-gradient(90deg,#ff2828 0%,#ff7b22 25%,#ffd400 50%,#65df3c 75%,#00d878 100%);position:relative}}.bar i{{position:absolute;top:-4px;width:4px;height:30px;background:white;border-radius:3px;transform:translateX(-2px)}}.finalbar{{height:34px;margin-top:22px}}.finalbar i{{height:42px}}.ticks{{display:flex;justify-content:space-between;color:#91a3bb;font-size:11px;margin-top:9px}}.tfrow{{display:flex;gap:8px;margin-top:16px}}.tfitem{{flex:1;text-align:center;border:1px solid #19313d;border-radius:10px;padding:8px 3px}}.tfitem b{{display:block;color:#fff;font-size:12px}}.tfitem span{{font-size:16px;font-weight:800}}.accgrid{{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px}}.accuracy{{min-width:0;font-size:11px;border:1px solid #19313d;border-radius:10px;overflow:hidden}}.accuracy th{{padding:8px 5px;text-align:center;font-size:11px;border-bottom:1px solid #18303a}}.accuracy th:first-child,.accuracy td:first-child{{text-align:center!important;white-space:nowrap;width:42px}}.accuracy td{{text-align:left!important;width:auto;padding:8px 5px;font-size:11px}}.accuracy td small{{display:block;font-size:8px;margin-top:1px}}.dirline{{white-space:nowrap;line-height:1.45}}.na,small{{color:#71808d}}@media(max-width:600px){{body{{padding:14px}}.score{{font-size:46px}}.card{{padding:17px}}td{{font-size:12px;padding:10px 3px}}td:first-child{{width:120px}}.accgrid{{grid-template-columns:1fr;gap:12px}}.miniacc td:first-child{{width:48px}}.miniacc td{{font-size:12px}}}}
</style></head><body><h1>₿ BTC AGENT <span class="live">LIVE</span></h1><small><b style="color:#fff">BTC:</b> <b style="color:{('#00df79' if btc_market['changes'].get('1d',0)>=0 else '#ff4d4d')}">{("N/A" if btc_market["price"] is None else f"${btc_market['price']:,.2f}")}</b> · {market_change_html("5m")} · {market_change_html("15m")} · {market_change_html("1h")} · {market_change_html("4h")} · {market_change_html("1d")}</small><div class="card"><small>FINAL SCORE</small><div class="score">{final_txt} / 100</div><h2>{signal_label(final)}</h2><div class="bar finalbar"><i style="left:{final_pos}%"></i></div><div class="ticks"><span>0 STRONG SELL</span><span>20 SELL</span><span>40</span><span>60 BUY</span><span>80</span><span>100 STRONG BUY</span></div><div class="tfrow">{tf_html}</div></div><div class="card"><h2>GROUP SCORES (10 GROUPS)</h2><table>{rows_html}</table><p><b>{s["features_active"]} / {s["features_total"]}</b> <span class="live">●</span> FEATURES ACTIVE</p></div><div class="card"><h2>PRICE-DIRECTION ACCURACY</h2><div class="accgrid">{accuracy_html}</div><small>↑ = up prediction · ↓ = down prediction · number in parentheses = n · last two values = median favorable/adverse BTC move.</small></div><div class="card"><h2>END MOVE</h2><div class="accgrid">{endmove_html}</div><small>Median BTC move at the end of each horizon, split by prediction direction.</small></div><div class="card"><h2>SCORE STRENGTH</h2><div class="accgrid">{strength_html}</div><small>Median entry score for each prediction direction.</small></div><div class="card"><h2>TIME TO MFE / MAE</h2><div class="accgrid">{timing_html}</div><small>Median minutes from entry to maximum favorable/adverse excursion.</small></div><div class="card"><h2>MARKET CONTEXT</h2><div class="accgrid">{context_html}</div><small>Most common entry context: MARKET REGIME direction plus recent realized volatility.</small></div><div class="card"><h2>ANALYTICS SUMMARY</h2><div class="accgrid">{summary_html}</div><small>FINAL summary by timeframe and direction. All values come from collected analytics data.</small></div><div class="card"><h2>SUGGESTED SETTINGS</h2><div class="accgrid">{settings_html}</div><small>Settings remain COLLECTING until n ≥ 1,000 for that timeframe and direction. TP/SL/entry values are observed-data candidates for paper testing, not live-trading guarantees.</small></div><div class="card"><b class="live">● LIVE</b><p>Dashboard refreshes every 5 seconds · Engine recalculates as new data arrives.</p></div></body></html>""".encode(); ct="text/html; charset=utf-8"
  writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nCache-Control: no-store\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()+body); await writer.drain()
 except Exception as ex: log.warning("http %s",ex)
 finally:
  writer.close(); await writer.wait_closed()

async def http_server():
 port=int(os.getenv("PORT","8080")); server=await asyncio.start_server(http_handler,"0.0.0.0",port)
 log.info("HTTP listening on %s",port)
 async with server: await server.serve_forever()

async def analytics_loop():
 analytics_init()
 while True:
  try:
   if btc_market["price"] is not None:
    s=E.status(); scores=dict(s["groups"]); scores["FINAL"]=s["final"]
    now=time.time(); db=sqlite3.connect(ANALYTICS_DB); db.execute("INSERT OR REPLACE INTO snapshots(ts,price,scores) VALUES(?,?,?)",(now,btc_market["price"],json.dumps(scores))); db.execute("DELETE FROM snapshots WHERE ts < ?",(now-7*86400,)); db.commit(); db.close()
  except Exception as ex: log.warning("analytics snapshot %s",ex)
  await asyncio.sleep(60)

async def heartbeat():
 while True: log.info("STATE %s",json.dumps(E.status(),ensure_ascii=False)); await asyncio.sleep(10)

async def main():
 async with aiohttp.ClientSession(headers={"User-Agent":"btc-agent-live/1.0"}) as s:
  await asyncio.gather(okx_ws(),mempool_loop(s),sentiment_loop(s),cross_market_loop(s),btc_timeframe_loop(s),macro_loop(s),telegram_bot_loop(s),analytics_loop(),http_server(),heartbeat())
if __name__=="__main__": asyncio.run(main())
