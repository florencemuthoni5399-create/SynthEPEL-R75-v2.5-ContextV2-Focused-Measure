'use strict';

require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const CONFIG = {
  appId: process.env.DERIV_APP_ID || '',
  apiToken: process.env.DERIV_API_TOKEN || '',
  accountType: (process.env.DERIV_ACCOUNT_TYPE || 'demo').toLowerCase(),
  asset: process.env.ASSET || 'R_75',
  horizons: parseList(process.env.HORIZONS_TICKS || '5,7'),
  lookback: positiveInt(process.env.LOOKBACK_TICKS, 20),
  moveTicks: positiveInt(process.env.MOVE_TICKS, 20),
  minMovePct: Number(process.env.MIN_MOVE_PCT) || 0.15,
  stake: Number(process.env.STAKE) || 1,
  proposalEnabled: String(process.env.PROPOSAL_ENABLED ?? 'true').toLowerCase() === 'true',
  proposalTimeoutMs: positiveInt(process.env.PROPOSAL_TIMEOUT_MS, 3000),
  epelEnabled: String(process.env.EPEL_ENABLED ?? 'true').toLowerCase() === 'true',
  epelLambda: Number(process.env.EPEL_LAMBDA) || 1.0,
  epelMinSamples: positiveInt(process.env.EPEL_MIN_SAMPLES, 100),
  epelEnforce: String(process.env.EPEL_ENFORCE ?? 'false').toLowerCase() === 'true',
  probabilityMode: String(process.env.PROBABILITY_MODE || 'CONTEXT_V2').toUpperCase(),
  wilsonZ: Number(process.env.WILSON_Z) || 1.959964,
  ledgerFile: resolvePath(process.env.LEDGER_FILE || './data/r75_tick_epel_ledger_v25.csv'),
  healthFile: resolvePath(process.env.HEALTH_FILE || './data/health_v25.json'),
  port: Number(process.env.PORT) || 8787,
  dashboardToken: process.env.DASHBOARD_TOKEN || '',
  enableTrading: String(process.env.ENABLE_TRADING || 'false').toLowerCase() === 'true',
};

function resolvePath(p) { return path.isAbsolute(p) ? p : path.join(__dirname, p); }
function positiveInt(v, fallback) { const n = Number(v); return Number.isInteger(n) && n > 0 ? n : fallback; }
function parseList(s) { return [...new Set(s.split(',').map(x => Number(x.trim())).filter(n => Number.isInteger(n) && n > 0))].sort((a,b)=>a-b); }

if (!CONFIG.appId || !CONFIG.apiToken) {
  console.error('[FATAL] DERIV_APP_ID and DERIV_API_TOKEN are required. Use the same credentials as the working SynthTrade bot.');
  process.exit(1);
}
if (CONFIG.accountType !== 'demo' && CONFIG.accountType !== 'real') {
  console.error('[FATAL] DERIV_ACCOUNT_TYPE must be demo or real.'); process.exit(1);
}
if (CONFIG.asset !== 'R_75') {
  console.error(`[FATAL] This research build is frozen to R_75. Got ${CONFIG.asset}.`); process.exit(1);
}
if (CONFIG.enableTrading) {
  console.error('[FATAL] ENABLE_TRADING=true is intentionally blocked in v2.2. This build is measurement-only.');
  process.exit(1);
}

fs.mkdirSync(path.dirname(CONFIG.ledgerFile), {recursive:true});
fs.mkdirSync(path.dirname(CONFIG.healthFile), {recursive:true});
if (!fs.existsSync(CONFIG.ledgerFile)) {
  fs.writeFileSync(CONFIG.ledgerFile,
    'signal_id,signal_time,direction,contract_type,entry_price,lookback_ticks,move_ticks,horizon_ticks,entry_tick_index,expiry_tick_index,expiry_price,result,context_key,context_mode,context_samples_before,prob_samples_before,predicted_p,wilson_lower,ask_price,payout,gross_profit,gross_profit_rate,implied_probability,epel_threshold,price_edge_lower,epel_qualifies,opposite_result,flat_pnl,proposal_age_ms\n');
}

const state = {
  status: 'starting', account: null, connectedAt: null, lastTickAt: null,
  currentPrice: null, tickCount: 0, signalCount: 0, resolvedCount: 0,
  wins: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  losses: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  epelQualified: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  epelQualifiedWins: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  epelQualifiedLosses: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  oppositeWins: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  oppositeLosses: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  proposalRequests: 0, proposalQuotes: 0, proposalErrors: 0, proposalMissing: 0,
  positiveEdge: Object.fromEntries(CONFIG.horizons.map(h=>[h,0])),
  error: null, wsEndpoint: null, lastApiMessage: null,
};

let ws = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let reqId = 0;
let tickIndex = 0;
const ticks = [];
const pending = [];
const proposalRequests = new Map();
const outcomes = Object.fromEntries(CONFIG.horizons.map(h => [h, {wins:[], losses:[]} ]));

function nextReqId(){ return ++reqId; }
function log(...a){ console.log(new Date().toISOString(), ...a); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
async function jsonFetch(url, options, label){
  const r = await fetch(url, options);
  const text = await r.text();
  let j=null; try { j=JSON.parse(text); } catch {}
  if (!r.ok) throw new Error(`${label} HTTP ${r.status}: ${j?.errors?.[0]?.message || text.slice(0,300)}`);
  return j;
}

function wilsonLower(w,n,z=CONFIG.wilsonZ){
  if (!n) return 0;
  const phat=w/n, z2=z*z;
  return (phat + z2/(2*n) - z*Math.sqrt((phat*(1-phat)+z2/(4*n))/n))/(1+z2/n);
}
function epelThreshold(grossProfitRate=0.78){
  // EPEL: EL <= lambda * EP. If a winning contract returns 1+r per 1 stake,
  // then EP = p*r and EL = 1-p, giving p >= 1/(1 + lambda*r).
  const r=Number(grossProfitRate);
  if(!Number.isFinite(r) || r<=0) return 1;
  return 1/(1+CONFIG.epelLambda*r);
}
function proposalMetrics(ask,payout,prob){
  if(!Number.isFinite(ask) || ask<=0 || !Number.isFinite(payout) || payout<=0) return null;
  const grossProfit=payout-ask;
  const grossProfitRate=grossProfit/ask;
  const impliedProbability=ask/payout;
  const threshold=epelThreshold(grossProfitRate);
  const lower=Number.isFinite(prob?.lower)?prob.lower:null;
  const p=Number.isFinite(prob?.p)?prob.p:null;
  return {askPrice:ask,payout,grossProfit,grossProfitRate,impliedProbability,epelThreshold:threshold,priceEdge:p==null?null:p-impliedProbability,priceEdgeLower:lower==null?null:lower-impliedProbability,epelQualifies:prob?.samples>=CONFIG.epelMinSamples && lower!=null && lower>=threshold};
}
// Aggregate same-direction outcomes instead of retaining arrays.
// CONTEXT_V2 adds a pre-signal market-condition key based only on information
// already known at the signal: short-term trend bias + short/long volatility regime.
const directionOutcomes = Object.fromEntries(CONFIG.horizons.map(h=>[h, {RISE:{wins:0,total:0}, FALL:{wins:0,total:0}}]));
const contextOutcomes = Object.fromEntries(CONFIG.horizons.map(h=>[h, {RISE:Object.create(null), FALL:Object.create(null)}]));

function meanAbsReturn(startIndex,endIndex){
  let sum=0,n=0;
  for(let i=Math.max(1,startIndex); i<=endIndex && i<ticks.length; i++){
    const a=ticks[i-1]?.p, b=ticks[i]?.p;
    if(!Number.isFinite(a)||!Number.isFinite(b)||a===0) continue;
    sum += Math.abs((b-a)/a)*100; n++;
  }
  return n ? sum/n : 0;
}
function contextKeyForSignal(t){
  const end=ticks.length-1;
  const shortStart=Math.max(1,end-CONFIG.lookback+1);
  const longWindow=Math.max(CONFIG.lookback*4,80);
  const longStart=Math.max(1,end-longWindow+1);
  let up=0,down=0,flat=0;
  for(let i=shortStart;i<=end;i++){
    const a=ticks[i-1]?.p,b=ticks[i]?.p;
    if(!Number.isFinite(a)||!Number.isFinite(b)) continue;
    if(b>a) up++; else if(b<a) down++; else flat++;
  }
  const n=up+down+flat;
  const upShare=n?up/n:0, downShare=n?down/n:0;
  const trend=upShare>=0.65?'UP':downShare>=0.65?'DOWN':'BALANCED';
  const shortVol=meanAbsReturn(shortStart,end);
  const longVol=meanAbsReturn(longStart,end);
  const volRatio=longVol>0?shortVol/longVol:1;
  const vol=volRatio>=1.25?'HIGH':volRatio<=0.80?'LOW':'NORMAL';
  return {key:`${trend}_${vol}`,trend,vol,volRatio,shortVol,longVol};
}
function getStats(direction,h,contextKey){
  if(CONFIG.probabilityMode==='CONTEXT_V2' && contextKey){
    const c=contextOutcomes[h][direction][contextKey] || {wins:0,total:0};
    return {stats:c,mode:'CONTEXT_V2'};
  }
  return {stats:directionOutcomes[h][direction],mode:'GLOBAL'};
}
function predictedProbability(direction,h,contextKey){
  const selected=getStats(direction,h,contextKey);
  const stats=selected.stats;
  if(stats.total < CONFIG.epelMinSamples) return {p:null,lower:null,samples:stats.total,qualifies:false,threshold:epelThreshold(),mode:selected.mode,contextKey:contextKey||''};
  const wins=stats.wins;
  const p=(wins+1)/(stats.total+2); // Laplace/Beta(1,1), leakage-safe.
  const lower=wilsonLower(wins,stats.total);
  const threshold=epelThreshold();
  return {p,lower,samples:stats.total,qualifies:lower>=threshold,threshold,mode:selected.mode,contextKey:contextKey||''};
}

async function connectAuthenticated(){
  if (ws && (ws.readyState===WebSocket.OPEN || ws.readyState===WebSocket.CONNECTING)) return;
  state.status='authenticating'; state.error=null;
  log('[CONNECT] Starting the SAME REST + account lookup + OTP WebSocket architecture used by SynthTrade Pro.');
  try {
    const accounts = await jsonFetch('https://api.derivws.com/trading/v1/options/accounts', {
      headers:{'Deriv-App-ID':CONFIG.appId,'Authorization':`Bearer ${CONFIG.apiToken}`}
    }, 'account lookup');
    const list=accounts.data || [];
    const match=list.find(a=>a.account_type===CONFIG.accountType && a.status==='active') || list.find(a=>a.account_type===CONFIG.accountType);
    if(!match) throw new Error(`No ${CONFIG.accountType} account found for this token/app ID`);
    state.account={accountId:match.account_id,accountType:match.account_type,currency:match.currency,balance:Number(match.balance)||0,status:match.status};
    log(`[ACCOUNT] ${match.account_id} type=${match.account_type} currency=${match.currency} balance=${state.account.balance}`);

    const otp=await jsonFetch(`https://api.derivws.com/trading/v1/options/accounts/${match.account_id}/otp`, {
      method:'POST', headers:{'Deriv-App-ID':CONFIG.appId,'Authorization':`Bearer ${CONFIG.apiToken}`}
    }, 'OTP request');
    const url=otp?.data?.url;
    if(!url) throw new Error('OTP response did not contain data.url');
    state.wsEndpoint = url.replace(/([?&]otp=)[^&]+/, '$1<redacted>');
    log(`[OTP] Authenticated WebSocket URL received: ${state.wsEndpoint}`);
    log('[OTP] Connecting immediately because the OTP is short-lived and single-use.');

    ws=new WebSocket(url);
    ws.on('open',()=>{
      reconnectAttempt=0; state.status='authenticated'; state.connectedAt=new Date().toISOString();
      log('[WS] AUTHENTICATED — Deriv WebSocket is open.');
      send({time:1,req_id:nextReqId()});
      send({balance:1,subscribe:1,req_id:nextReqId()});
      send({active_symbols:'brief',req_id:nextReqId()});
      send({ticks:CONFIG.asset,subscribe:1,req_id:nextReqId()});
      send({ticks_history:CONFIG.asset,count:200,end:'latest',style:'ticks',req_id:nextReqId()});
      log(`[STREAM] Subscribed to ${CONFIG.asset} ticks + 200-tick history.`);
      writeHealth();
    });
    ws.on('message',raw=>{ try{handleMessage(JSON.parse(raw.toString()));}catch(e){log('[WARN] message parse:',e.message);} });
    ws.on('error',e=>{state.error=e.message; log('[WS ERROR]',e.message); writeHealth();});
    ws.on('close',(code,reason)=>{
      state.status='disconnected'; log(`[WS] closed code=${code} reason=${reason?.toString()||''}`); writeHealth();
      scheduleReconnect();
    });
  } catch(e){
    state.status='error'; state.error=e.message; log('[CONNECT ERROR]',e.message); writeHealth(); scheduleReconnect();
  }
}
function send(o){ if(ws?.readyState===WebSocket.OPEN) ws.send(JSON.stringify(o)); }
function scheduleReconnect(){
  if(reconnectTimer) return;
  reconnectAttempt++;
  const ms=Math.min(30000,2000*Math.max(1,reconnectAttempt));
  log(`[RECONNECT] attempt=${reconnectAttempt} in ${ms}ms`);
  reconnectTimer=setTimeout(()=>{reconnectTimer=null; connectAuthenticated();},ms);
}

function handleMessage(d){
  state.lastApiMessage=d.msg_type||null;
  if(d.error){
    if(d.msg_type==='proposal' || d.echo_req?.proposal===1){ handleProposal(d); return; }
    state.error=`${d.error.code||'API_ERROR'}: ${d.error.message||'unknown'}`; log('[DERIV ERROR]',JSON.stringify(d.error)); writeHealth(); return;
  }
  if(d.msg_type==='time'){ log(`[API] time OK: ${d.time}`); return; }
  if(d.msg_type==='balance' && state.account){ state.account.balance=Number(d.balance?.balance)||state.account.balance; return; }
  if(d.msg_type==='active_symbols'){
    const found=(d.active_symbols||[]).find(x=>x.symbol===CONFIG.asset);
    log(`[SYMBOL] ${CONFIG.asset} ${found?'FOUND':'NOT FOUND'} in active_symbols`); return;
  }
  if(d.msg_type==='proposal'){
    handleProposal(d);
    return;
  }
  if(d.msg_type==='history'){
    const prices=d.history?.prices||[]; const times=d.history?.times||[];
    log(`[HISTORY] received ${prices.length} historical ${CONFIG.asset} ticks`);
    for(let i=0;i<prices.length;i++) addTick(Number(prices[i]), Number(times[i])*1000, true);
    return;
  }
  if(d.msg_type==='tick'){
    addTick(Number(d.tick.quote), Number(d.tick.epoch)*1000, false);
  }
}

function addTick(price,ts,isHistory){
  if(!Number.isFinite(price)) return;
  tickIndex++;
  const t={i:tickIndex,p:price,ts};
  ticks.push(t); if(ticks.length>5000) ticks.shift();
  state.tickCount++; state.currentPrice=price; state.lastTickAt=new Date(ts).toISOString();
  resolvePending(t);
  if(!isHistory && ticks.length>=CONFIG.lookback+1) evaluateSignal(t);
  writeHealth(false);
}

function evaluateSignal(t){
  const prev=ticks[ticks.length-1-CONFIG.lookback];
  if(!prev) return;
  const move=t.p-prev.p;
  const movePct=Math.abs(prev.p)>0 ? Math.abs(move/prev.p)*100 : 0;
  if(movePct < CONFIG.minMovePct) return;
  const direction=move>0?'RISE':'FALL'; // momentum research seed
  const contractType=direction==='RISE'?'CALL':'PUT';
  const context=contextKeyForSignal(t);
  const probs={};
  for(const h of CONFIG.horizons) probs[h]=predictedProbability(direction,h,context.key);
  const signalId=`${Date.now()}-${t.i}`;
  state.signalCount++;
  for(const h of CONFIG.horizons){
    const p=probs[h];
    const test={signalId,direction,contractType,entry:t,horizon:h,targetIndex:t.i+h,prob:p,context,proposal:null,epelQualifiedAtEntry:false,oppositeResult:null};
    pending.push(test);
    if(CONFIG.proposalEnabled) requestProposal(test);
    else {
      // No live quote: retain the fixed research threshold only as a fallback.
      test.epelQualifiedAtEntry=p.qualifies;
      if(p.qualifies) state.epelQualified[h]++;
      appendSignalRow(test);
    }
  }
  log(`[SIGNAL] ${signalId} ${direction}/${contractType} netMove=${move} (${movePct.toFixed(4)}%) over ${CONFIG.lookback} ticks | context=${context.key} volRatio=${context.volRatio.toFixed(2)} | horizons=${CONFIG.horizons.join(',')} | live proposal pricing=${CONFIG.proposalEnabled?'ON':'OFF'}`);
}
function requestProposal(test){
  const req_id=nextReqId();
  state.proposalRequests++;
  const requestedAt=Date.now();
  const timeout=setTimeout(()=>{
    if(!proposalRequests.has(req_id)) return;
    proposalRequests.delete(req_id);
    state.proposalErrors++;
    log(`[PROPOSAL TIMEOUT] ${test.signalId} ${test.contractType} ${test.horizon}t after ${CONFIG.proposalTimeoutMs}ms`);
  },CONFIG.proposalTimeoutMs);
  proposalRequests.set(req_id,{test,requestedAt,timeout});
  const currency=state.account?.currency || 'USD';
  send({proposal:1,amount:CONFIG.stake,basis:'stake',contract_type:test.contractType,currency,duration:test.horizon,duration_unit:'t',underlying_symbol:CONFIG.asset,req_id});
}
function handleProposal(d){
  const req_id=d.req_id ?? d.echo_req?.req_id;
  const meta=proposalRequests.get(req_id);
  if(!meta) return;
  clearTimeout(meta.timeout);
  proposalRequests.delete(req_id);
  const test=meta.test;
  if(d.error){
    state.proposalErrors++;
    log(`[PROPOSAL ERROR] ${test.signalId} ${test.contractType} ${test.horizon}t: ${d.error.message||'unknown error'}`);
    return;
  }
  const pr=d.proposal||{};
  const ask=Number(pr.ask_price);
  const payout=Number(pr.payout);
  const m=proposalMetrics(ask,payout,test.prob);
  if(!m){
    state.proposalErrors++;
    log(`[PROPOSAL ERROR] ${test.signalId} ${test.contractType} ${test.horizon}t: missing/invalid ask_price or payout`);
    return;
  }
  test.proposal={...m,receivedAt:Date.now(),requestAgeMs:Date.now()-meta.requestedAt,spot:pr.spot??null};
  test.epelQualifiedAtEntry=m.epelQualifies;
  state.proposalQuotes++;
  if(m.epelQualifies) state.epelQualified[test.horizon]++;
  if(m.priceEdgeLower!=null && m.priceEdgeLower>0) state.positiveEdge[test.horizon]++;
  log(`[PROPOSAL] ${test.signalId} ${test.contractType} ${test.horizon}t ask=${ask} payout=${payout} implied=${(m.impliedProbability*100).toFixed(2)}% modelLower=${test.prob.lower==null?'—':(test.prob.lower*100).toFixed(2)+'%'} EPEL=${(m.epelThreshold*100).toFixed(2)}% edgeLower=${m.priceEdgeLower==null?'—':(m.priceEdgeLower*100).toFixed(2)+'%'} qualified=${m.epelQualifies?'YES':'NO'}`);
}
function appendSignalRow(test){
  const p=test.prob;
  const m=test.proposal;
  appendRow([test.signalId,new Date(test.entry.ts).toISOString(),test.direction,test.contractType,test.entry.p,CONFIG.lookback,test.entry.p-(ticks.find(z=>z.i===test.entry.i-CONFIG.lookback)?.p??test.entry.p),test.horizon,test.entry.i,test.targetIndex,'','',test.context?.key??'',p.mode??CONFIG.probabilityMode,p.samples,p.p??'',p.lower??'',m?.askPrice??'',m?.payout??'',m?.grossProfit??'',m?.grossProfitRate??'',m?.impliedProbability??'',m?.epelThreshold??p.threshold,m?.priceEdgeLower??'',m?.epelQualifies??test.epelQualifiedAtEntry,'','']);
}

function resolvePending(t){
  for(let i=pending.length-1;i>=0;i--){
    const x=pending[i]; if(t.i<x.targetIndex) continue;
    const win=x.direction==='RISE'?t.p>x.entry.p:t.p<x.entry.p;
    const oppositeWin=x.direction==='RISE'?t.p<x.entry.p:t.p>x.entry.p;
    const flatPnl=x.proposal ? (win ? Number(x.proposal.payout)-Number(x.proposal.askPrice) : -Number(x.proposal.askPrice)) : (win?0.78:-1);
    state.resolvedCount++; if(win) state.wins[x.horizon]++; else state.losses[x.horizon]++;
    if(oppositeWin) state.oppositeWins[x.horizon]++; else state.oppositeLosses[x.horizon]++;
    const dirStats=directionOutcomes[x.horizon][x.direction];
    dirStats.total++;
    if(win) dirStats.wins++;
    const contextKey=x.context?.key || 'UNKNOWN';
    const bucket=contextOutcomes[x.horizon][x.direction][contextKey] || (contextOutcomes[x.horizon][x.direction][contextKey]={wins:0,total:0});
    bucket.total++;
    if(win) bucket.wins++;
    if(x.epelQualifiedAtEntry){
      if(win) state.epelQualifiedWins[x.horizon]++;
      else state.epelQualifiedLosses[x.horizon]++;
    }
    const result=win?'WIN':'LOSS';
    const oppositeResult=oppositeWin?'WIN':'LOSS';
    if(!x.proposal && CONFIG.proposalEnabled){ state.proposalMissing++; }
    appendRow([x.signalId,new Date(x.entry.ts).toISOString(),x.direction,x.contractType,x.entry.p,CONFIG.lookback,x.entry.p-(ticks.find(z=>z.i===x.entry.i-CONFIG.lookback)?.p??x.entry.p),x.horizon,x.entry.i,t.i,t.p,result,x.context?.key??'',x.prob.mode??CONFIG.probabilityMode,x.prob.samples,x.prob.p??'',x.prob.lower??'',x.proposal?.askPrice??'',x.proposal?.payout??'',x.proposal?.grossProfit??'',x.proposal?.grossProfitRate??'',x.proposal?.impliedProbability??'',x.proposal?.epelThreshold??x.prob.threshold,x.proposal?.priceEdgeLower??'',x.epelQualifiedAtEntry,oppositeResult,flatPnl,x.proposal?.requestAgeMs??'']);
    pending.splice(i,1);
  }
}
function appendRow(values){ fs.appendFileSync(CONFIG.ledgerFile, values.map(csv).join(',')+'\n'); }
function csv(v){ if(v===null||v===undefined)return ''; const s=String(v); return /[",\n]/.test(s)?'"'+s.replace(/"/g,'""')+'"':s; }

function dashboardStats(){
  const rows=CONFIG.horizons.map(h=>{
    const samples=state.wins[h]+state.losses[h];
    const winRate=samples ? state.wins[h]/samples : null;
    const epelQ=state.epelQualified[h];
    const epelWins=state.epelQualifiedWins[h];
    const epelLosses=state.epelQualifiedLosses[h];
    const epelSamples=epelWins+epelLosses;
    const epelWinRate=epelSamples ? epelWins/epelSamples : null;
    const oppSamples=state.oppositeWins[h]+state.oppositeLosses[h];
    const oppositeWinRate=oppSamples?state.oppositeWins[h]/oppSamples:null;
    const dirs={};
    for(const d of ['RISE','FALL']){
      const stats=directionOutcomes[h][d];
      const wins=stats.wins, samples=stats.total;
      dirs[d]={samples,wins,losses:samples-wins,winRate:samples?wins/samples:null,lower:samples>=CONFIG.epelMinSamples?wilsonLower(wins,samples):null,qualifies:samples>=CONFIG.epelMinSamples && wilsonLower(wins,samples)>=epelThreshold()};
    }
    const contexts={};
    for(const d of ['RISE','FALL']){
      contexts[d]={};
      for(const [key,stats] of Object.entries(contextOutcomes[h][d])){
        contexts[d][key]={samples:stats.total,wins:stats.wins,losses:stats.total-stats.wins,winRate:stats.total?stats.wins/stats.total:null,lower:stats.total>=CONFIG.epelMinSamples?wilsonLower(stats.wins,stats.total):null,qualifies:stats.total>=CONFIG.epelMinSamples && wilsonLower(stats.wins,stats.total)>=epelThreshold()};
      }
    }
    return {horizon:h,samples,wins:state.wins[h],losses:state.losses[h],winRate,epelQualified:epelQ,epelQualifiedWins:epelWins,epelQualifiedLosses:epelLosses,epelResolved:epelSamples,epelWinRate,oppositeWinRate,proposalQuotes:0,epelThreshold:epelThreshold(),directions:dirs,contexts};
  });
  return {rows,totalResolved:state.resolvedCount,totalPending:pending.length,proposalRequests:state.proposalRequests,proposalQuotes:state.proposalQuotes,proposalErrors:state.proposalErrors,proposalMissing:state.proposalMissing,positiveEdge:state.positiveEdge};
}

function health(){
  return {version:'2.5.0',mode:'MEASURE_ONLY',asset:CONFIG.asset,status:state.status,account:state.account?{accountId:state.account.accountId,accountType:state.account.accountType,currency:state.account.currency,balance:state.account.balance}:null,wsEndpoint:state.wsEndpoint,connectedAt:state.connectedAt,lastTickAt:state.lastTickAt,currentPrice:state.currentPrice,tickCount:state.tickCount,signalCount:state.signalCount,resolvedCount:state.resolvedCount,wins:state.wins,losses:state.losses,oppositeWins:state.oppositeWins,oppositeLosses:state.oppositeLosses,epelQualified:state.epelQualified,epelQualifiedWins:state.epelQualifiedWins,epelQualifiedLosses:state.epelQualifiedLosses,proposal:{enabled:CONFIG.proposalEnabled,requests:state.proposalRequests,quotes:state.proposalQuotes,errors:state.proposalErrors,missing:state.proposalMissing,timeoutMs:CONFIG.proposalTimeoutMs},pending:pending.length,error:state.error,epel:{enabled:CONFIG.epelEnabled,lambda:CONFIG.epelLambda,minSamples:CONFIG.epelMinSamples,enforce:CONFIG.epelEnforce,threshold:epelThreshold()},probabilityMode:CONFIG.probabilityMode,horizons:CONFIG.horizons,dashboard:dashboardStats(),updatedAt:new Date().toISOString()};
}
function writeHealth(verbose=true){ try{fs.writeFileSync(CONFIG.healthFile,JSON.stringify(health(),null,2));}catch(e){if(verbose)log('[HEALTH WRITE ERROR]',e.message);} }

const server=http.createServer((req,res)=>{
  if(req.url==='/health'){
    const body=JSON.stringify(health(),null,2); res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'}); return res.end(body);
  }
  if(req.url==='/'){
    const esc=(v)=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
    const html=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SynthEPEL-R75 Research v2.5</title>
    <style>
      :root{color-scheme:dark}body{margin:0;background:#080b10;color:#e8edf3;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}main{max-width:1100px;margin:auto;padding:18px}h1{font-size:22px;margin:0 0 4px}.sub{color:#9aa6b2;margin-bottom:18px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:14px}.card,.panel{background:#11161d;border:1px solid #252d38;border-radius:12px;padding:14px}.label{font-size:12px;color:#8e9aa8;text-transform:uppercase;letter-spacing:.05em}.value{font-size:20px;font-weight:700;margin-top:5px}.ok{color:#62d89b}.warn{color:#ffd166}.bad{color:#ff7373}table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:10px 8px;border-bottom:1px solid #252d38;text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}th{color:#9aa6b2;font-weight:600}.panel{margin-bottom:14px;overflow:auto}.note{color:#aab4c0;font-size:12px;line-height:1.5}.small{font-size:12px;color:#9aa6b2}.good{color:#62d89b}.footer{margin-top:12px;color:#707c89;font-size:11px}@media(max-width:720px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}table{min-width:1100px}}
    </style></head><body><main>
    <h1>📊 SynthEPEL-R75 Research Dashboard v2.5</h1><div class="sub">Live measurement only — no trades are being placed. This version records Deriv proposal prices at each signal.</div>
    <div class="grid"><div class="card"><div class="label">Status</div><div id="status" class="value">Loading…</div></div>
    <div class="card"><div class="label">Ticks collected</div><div id="ticks" class="value">—</div></div>
    <div class="card"><div class="label">Signals</div><div id="signals" class="value">—</div></div>
    <div class="card"><div class="label">Resolved tests</div><div id="resolved" class="value">—</div></div></div>
    <div class="panel"><h2>Horizon performance + live pricing</h2><div class="small">Each signal is tested at 5 and 7 ticks. Model probability is calculated only from outcomes known before the signal. Proposal data is the live ask price and total payout returned by Deriv.</div><br><table><thead><tr><th>Horizon</th><th>Samples</th><th>Wins</th><th>Losses</th><th>Win rate</th><th>EPEL-qualified</th><th>EPEL resolved</th><th>EPEL win rate</th><th>Opposite win rate</th><th>Positive-edge count</th></tr></thead><tbody id="rows"></tbody></table></div>
    <div class="panel"><h2>What the new EPEL gate means</h2><div id="epelSummary"></div><p class="note">For each signal/horizon, the bot requests a real CALL/PUT proposal using the same stake and tick duration. It records: ask price, total payout, implied break-even probability = ask/payout, EPEL threshold = 1/(1 + λ × gross-profit-rate), model probability, Wilson lower bound, and lower-bound edge versus the quoted break-even probability. A test is EPEL-qualified only when the pre-signal Wilson lower bound clears the live EPEL threshold. No buy request is sent.</p></div>
    <div class="panel"><h2>Proposal coverage</h2><div id="proposalSummary"></div><p class="note">If proposal coverage is incomplete, those observations are still retained as market-outcome tests, but they are not treated as fully priced EPEL candidates. Proposal response age is recorded in the ledger.</p></div>
    <div class="panel"><h2>Direction breakdown</h2><table><thead><tr><th>Horizon</th><th>Direction</th><th>Samples</th><th>Wins</th><th>Win rate</th><th>Wilson lower</th><th>Qualified now?</th></tr></thead><tbody id="dirs"></tbody></table></div>
    <div class="panel"><h2>Context V2 breakdown</h2><div class="small">Context V2 conditions probability on pre-signal market state: trend bias (UP/DOWN/BALANCED) plus short-vs-long volatility regime (LOW/NORMAL/HIGH). A context only qualifies after the stricter minimum sample count is reached. No future information is used to define the context.</div><br><table><thead><tr><th>Horizon</th><th>Direction</th><th>Context</th><th>Samples</th><th>Wins</th><th>Win rate</th><th>Wilson lower</th><th>Qualified?</th></tr></thead><tbody id="contexts"></tbody></table></div>
    <div class="footer">Auto-refresh: 3 seconds · Asset: ${esc(CONFIG.asset)} · Mode: MEASURE_ONLY · Probability: ${esc(CONFIG.probabilityMode)} · EPEL enforce: ${CONFIG.epelEnforce?'ON':'OFF'} · Proposal pricing: ${CONFIG.proposalEnabled?'ON':'OFF'}</div></main>
    <script>
      const pct=x=>x==null?'—':(x*100).toFixed(1)+'%'; const pct2=x=>x==null?'—':(x*100).toFixed(2)+'%'; const num=x=>x==null?'—':Number(x).toLocaleString();
      async function refresh(){try{const r=await fetch('/health',{cache:'no-store'});const d=await r.json();
        const s=document.getElementById('status');s.textContent=d.status;s.className='value '+(d.status==='authenticated'?'ok':(d.status==='error'?'bad':'warn'));
        document.getElementById('ticks').textContent=num(d.tickCount);document.getElementById('signals').textContent=num(d.signalCount);document.getElementById('resolved').textContent=num(d.resolvedCount);
        document.getElementById('rows').innerHTML=d.dashboard.rows.map(r=>'<tr><td><b>'+r.horizon+' ticks</b></td><td>'+num(r.samples)+'</td><td>'+num(r.wins)+'</td><td>'+num(r.losses)+'</td><td>'+pct(r.winRate)+'</td><td>'+num(r.epelQualified)+'</td><td>'+num(r.epelResolved)+'</td><td>'+pct(r.epelWinRate)+'</td><td>'+pct(r.oppositeWinRate)+'</td><td>'+num(d.dashboard.positiveEdge[r.horizon]||0)+'</td></tr>').join('');
        document.getElementById('epelSummary').innerHTML='<b>Live EPEL:</b> lambda '+d.epel.lambda.toFixed(2)+' · minimum '+d.epel.minSamples+' prior context outcomes · enforce '+(d.epel.enforce?'ON':'OFF')+' · mode '+d.probabilityMode+' · fixed fallback threshold '+pct2(d.epel.threshold)+'<br><b>Important:</b> the live proposal can make the actual EPEL threshold different on every signal because payout changes.';
        document.getElementById('proposalSummary').innerHTML='<b>Requests:</b> '+num(d.proposal.requests)+' &nbsp; <b>Quotes received:</b> '+num(d.proposal.quotes)+' &nbsp; <b>Errors:</b> '+num(d.proposal.errors)+' &nbsp; <b>Missing at resolution:</b> '+num(d.proposal.missing)+' &nbsp; <b>Coverage:</b> '+(d.proposal.requests?((d.proposal.quotes/d.proposal.requests)*100).toFixed(1)+'%':'—');
        document.getElementById('dirs').innerHTML=d.dashboard.rows.flatMap(r=>['RISE','FALL'].map(dir=>{const x=r.directions[dir];return '<tr><td>'+r.horizon+' ticks</td><td>'+dir+'</td><td>'+num(x.samples)+'</td><td>'+num(x.wins)+'</td><td>'+pct(x.winRate)+'</td><td>'+pct2(x.lower)+'</td><td>'+(x.qualifies?'🟢 YES':'—')+'</td></tr>'})).join('');
        document.getElementById('contexts').innerHTML=d.dashboard.rows.flatMap(r=>Object.entries(r.contexts).flatMap(([dir,cs])=>Object.entries(cs).map(([key,x])=>'<tr><td>'+r.horizon+' ticks</td><td>'+dir+'</td><td>'+key+'</td><td>'+num(x.samples)+'</td><td>'+num(x.wins)+'</td><td>'+pct(x.winRate)+'</td><td>'+pct2(x.lower)+'</td><td>'+(x.qualifies?'🟢 YES':'—')+'</td></tr>'))).join('');
      }catch(e){document.getElementById('status').textContent='dashboard error';document.getElementById('status').className='value bad'}}
      refresh();setInterval(refresh,3000);
    </script></body></html>`;
    res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'}); return res.end(html);
  }
  res.writeHead(404); res.end('Not found');
});
server.listen(CONFIG.port,'0.0.0.0',()=>log(`[HTTP] listening on ${CONFIG.port}`));

process.on('SIGTERM',()=>shutdown('SIGTERM')); process.on('SIGINT',()=>shutdown('SIGINT'));
function shutdown(sig){ log(`[SHUTDOWN] ${sig}`); try{ws?.close();}catch{} try{server.close();}catch{} process.exit(0); }

log('SynthEPEL-R75 v2.5 starting.');
log(`Research: ${CONFIG.asset} | horizons=${CONFIG.horizons.join(',')} ticks | lookback=${CONFIG.lookback} | minMovePct=${CONFIG.minMovePct} | probabilityMode=${CONFIG.probabilityMode} | epelMinSamples=${CONFIG.epelMinSamples} | epelEnforce=${CONFIG.epelEnforce} | proposalPricing=${CONFIG.proposalEnabled}`);
log('Trading is hard-disabled in this build.');
writeHealth();
connectAuthenticated();
