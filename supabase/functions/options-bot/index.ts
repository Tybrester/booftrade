import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// ─────────────────────────────────────────────
// MATH HELPERS
// ─────────────────────────────────────────────

function calcVWAP(candles: Candle[]): number {
  let cumTPV = 0, cumVol = 0;
  for (const c of candles) {
    const tp = (c.high + c.low + c.close) / 3;
    const vol = c.volume || 1;
    cumTPV += tp * vol;
    cumVol += vol;
  }
  return cumVol > 0 ? cumTPV / cumVol : 0;
}

function calcEMA(data: number[], period: number): number[] {
  const k = 2 / (period + 1);
  const ema = new Array(data.length).fill(0);
  ema[0] = data[0];
  for (let i = 1; i < data.length; i++) ema[i] = data[i] * k + ema[i - 1] * (1 - k);
  return ema;
}

function calcATR(highs: number[], lows: number[], closes: number[], period: number): number[] {
  const tr = highs.map((h, i) => i === 0 ? h - lows[i] : Math.max(h - lows[i], Math.abs(h - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  const atr = new Array(tr.length).fill(0);
  atr[period - 1] = tr.slice(0, period).reduce((a, b) => a + b) / period;
  for (let i = period; i < tr.length; i++) atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period;
  return atr;
}

function calcSuperTrend(highs: number[], lows: number[], closes: number[], atrLen: number, mult: number) {
  const atr = calcATR(highs, lows, closes, atrLen);
  const n = closes.length;
  const trend = new Array(n).fill(1);
  const upperBand = new Array(n).fill(0);
  const lowerBand = new Array(n).fill(0);
  for (let i = 0; i < n; i++) {
    const hl2 = (highs[i] + lows[i]) / 2;
    upperBand[i] = hl2 + mult * atr[i];
    lowerBand[i] = hl2 - mult * atr[i];
    if (i > 0) {
      lowerBand[i] = lowerBand[i] > lowerBand[i - 1] || closes[i - 1] < lowerBand[i - 1] ? lowerBand[i] : lowerBand[i - 1];
      upperBand[i] = upperBand[i] < upperBand[i - 1] || closes[i - 1] > upperBand[i - 1] ? upperBand[i] : upperBand[i - 1];
      if (trend[i - 1] === -1 && closes[i] > upperBand[i - 1]) trend[i] = 1;
      else if (trend[i - 1] === 1 && closes[i] < lowerBand[i - 1]) trend[i] = -1;
      else trend[i] = trend[i - 1];
    }
  }
  return { trend, upperBand, lowerBand };
}

function calcDMI(highs: number[], lows: number[], closes: number[], period: number) {
  const n = highs.length;
  const plusDM = new Array(n).fill(0);
  const minusDM = new Array(n).fill(0);
  for (let i = 1; i < n; i++) {
    const up = highs[i] - highs[i - 1];
    const down = lows[i - 1] - lows[i];
    if (up > down && up > 0) plusDM[i] = up;
    if (down > up && down > 0) minusDM[i] = down;
  }
  const atr = calcATR(highs, lows, closes, period);
  const smoothPlusDM = calcEMA(plusDM, period);
  const smoothMinusDM = calcEMA(minusDM, period);
  const plusDI = smoothPlusDM.map((v, i) => atr[i] ? (v / atr[i]) * 100 : 0);
  const minusDI = smoothMinusDM.map((v, i) => atr[i] ? (v / atr[i]) * 100 : 0);
  const dx = plusDI.map((v, i) => (v + minusDI[i]) ? Math.abs(v - minusDI[i]) / (v + minusDI[i]) * 100 : 0);
  const adx = new Array(n).fill(0);
  const start2 = period * 2 - 1;
  if (start2 < n) {
    const validDx = dx.slice(period - 1, start2);
    adx[start2] = validDx.reduce((a, b) => a + b, 0) / period;
    for (let i = start2 + 1; i < n; i++) adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period;
  }
  return { plusDI, minusDI, adx };
}

function calcRSI(closes: number[], period: number): number[] {
  const rsi: number[] = new Array(closes.length).fill(NaN);
  if (closes.length < period + 1) return rsi;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains += diff; else losses -= diff;
  }
  let avgGain = gains / period, avgLoss = losses / period;
  rsi[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const g = diff > 0 ? diff : 0, l = diff < 0 ? -diff : 0;
    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;
    rsi[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }
  return rsi;
}

function calcMACD(closes: number[], fast: number, slow: number, signal: number): { macdLine: number[], signalLine: number[], hist: number[] } {
  const emaFast = calcEMA(closes, fast);
  const emaSlow = calcEMA(closes, slow);
  const macdLine = closes.map((_, i) => (isNaN(emaFast[i]) || isNaN(emaSlow[i])) ? NaN : emaFast[i] - emaSlow[i]);
  const validStart = macdLine.findIndex(v => !isNaN(v));
  const signalLine: number[] = new Array(closes.length).fill(NaN);
  if (validStart >= 0) {
    const emaSignal = calcEMA(macdLine.slice(validStart), signal);
    for (let i = 0; i < emaSignal.length; i++) signalLine[validStart + i] = emaSignal[i];
  }
  const hist = macdLine.map((v, i) => (isNaN(v) || isNaN(signalLine[i])) ? NaN : v - signalLine[i]);
  return { macdLine, signalLine, hist };
}

function generateSignalRSIMACD(candles: Candle[], tradeDirection = 'both'): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const closes = candles.map(c => c.close);
  const n = closes.length;
  const i = n - 2;
  const rsi = calcRSI(closes, 14);
  const ema50 = calcEMA(closes, 50);
  const { hist } = calcMACD(closes, 12, 26, 9);
  const curRSI = rsi[i], curEma = ema50[i], curHist = hist[i], curClose = closes[i];
  
  // Replay position state
  let inLong = false, inShort = false;
  for (let j = 50; j < i; j++) {
    const r = rsi[j], h = hist[j], e = ema50[j], c = closes[j];
    if (isNaN(r) || isNaN(h) || isNaN(e)) continue;
    const buyCond = (r < 30 || h > 0) && c > e;
    const sellCond = (r > 70 || h < 0) && c < e;
    if (!inLong && !inShort && buyCond) inLong = true;
    else if (!inLong && !inShort && sellCond) inShort = true;
    else if (inLong && sellCond) { inLong = false; inShort = true; }
    else if (inShort && buyCond) { inShort = false; inLong = true; }
  }
  
  const buyCond  = (curRSI < 30 || curHist > 0) && curClose > curEma;
  const sellCond = (curRSI > 70 || curHist < 0) && curClose < curEma;
  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `rsi=${curRSI?.toFixed(1)}, macd_hist=${curHist?.toFixed(4)}, ema=${curEma?.toFixed(2)}, close=${curClose?.toFixed(2)}, pos=${inLong ? 'long' : inShort ? 'short' : 'flat'}`;
  
  if (buyCond) {
    if (inShort) { signal = 'buy'; reason = `EXIT SHORT->LONG. ${reason}`; }
    else if (!inLong) { signal = 'buy'; reason = `ENTER LONG. ${reason}`; }
  } else if (sellCond) {
    if (inLong) { signal = 'sell'; reason = `EXIT LONG->SHORT. ${reason}`; }
    else if (!inShort && tradeDirection !== 'long') { signal = 'sell'; reason = `ENTER SHORT. ${reason}`; }
    else if (tradeDirection === 'long' && inLong) { signal = 'sell'; reason = `EXIT LONG (long-only). ${reason}`; }
  }
  return { signal, price: curClose, trend: buyCond ? 1 : -1, ema: curEma, adx: curRSI, reason };
}

// ─────────────────────────────────────────────
// SHARED HELPERS FOR BOOF 7.0 / 8.0
// ─────────────────────────────────────────────

function b50SMA(data: number[], period: number): number[] {
  const result: number[] = [];
  for (let i = period - 1; i < data.length; i++) {
    const slice = data.slice(i - period + 1, i + 1);
    result.push(slice.reduce((a: number, b: number) => a + b, 0) / period);
  }
  return result;
}
function b50StdDev(data: number[], period: number): number {
  if (data.length < period) return 0;
  const slice = data.slice(-period);
  const mean = slice.reduce((a: number, b: number) => a + b, 0) / period;
  return Math.sqrt(slice.reduce((a: number, b: number) => a + Math.pow(b - mean, 2), 0) / period);
}
function b50Mean(data: number[]): number {
  return data.length > 0 ? data.reduce((a: number, b: number) => a + b, 0) / data.length : 0;
}
function b50ADX(highs: number[], lows: number[], closes: number[], period: number): number {
  const dmP: number[] = [], dmM: number[] = [], trV: number[] = [];
  for (let i = 1; i < closes.length; i++) {
    const up = highs[i] - highs[i-1], dn = lows[i-1] - lows[i];
    dmP.push(up > dn && up > 0 ? up : 0);
    dmM.push(dn > up && dn > 0 ? dn : 0);
    trV.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i-1]), Math.abs(lows[i] - closes[i-1])));
  }
  if (dmP.length < period) return 25;
  const diP = 100 * b50Mean(dmP.slice(-period)) / b50Mean(trV.slice(-period));
  const diM = 100 * b50Mean(dmM.slice(-period)) / b50Mean(trV.slice(-period));
  return (diP + diM) > 0 ? 100 * Math.abs(diP - diM) / (diP + diM) : 0;
}

// ─────────────────────────────────────────────
// BOOF 7.0 + 8.0 — ADAPTIVE SCALPER ENGINE
// ─────────────────────────────────────────────

interface Boof70Regime {
  type: 'TREND_UP' | 'TREND_DOWN' | 'RANGE' | 'HIGH_VOL' | 'LOW_VOL' | 'EXPLOSIVE';
  adx: number; atr: number; atrPercent: number; bbWidth: number;
  maSlope: number; volatilityPercentile: number;
  shouldTrade: boolean; noTradeReason?: string;
}

function isNoTradeZone80(isCrypto: boolean): { skip: boolean; reason: string } {
  const utcHour = new Date().getUTCHours();
  if (isCrypto) {
    if (utcHour >= 3 && utcHour < 5) return { skip: true, reason: 'Crypto dead zone: 03-05 UTC' };
    return { skip: false, reason: '' };
  }
  if (utcHour < 13 || utcHour >= 20) return { skip: true, reason: `Outside NYSE hours (UTC ${utcHour}:00)` };
  return { skip: false, reason: '' };
}

function detectRegime80(highs: number[], lows: number[], closes: number[], volumes: number[]): Boof70Regime {
  const n = closes.length;
  const atrVals: number[] = [];
  for (let i = 1; i < n; i++) atrVals.push(Math.max(highs[i]-lows[i], Math.abs(highs[i]-closes[i-1]), Math.abs(lows[i]-closes[i-1])));
  const atr = b50Mean(atrVals.slice(-14));
  const atrPercent = atr / closes[n-1] * 100;
  const adx = b50ADX(highs, lows, closes, 14);
  const sma20 = b50SMA(closes, 20);
  const std20 = b50StdDev(closes, 20);
  const bbUpper = sma20[sma20.length-1] + 2 * std20;
  const bbLower = sma20[sma20.length-1] - 2 * std20;
  const bbWidth = sma20[sma20.length-1] > 0 ? (bbUpper - bbLower) / sma20[sma20.length-1] : 0;
  const maRecent = sma20[sma20.length-1];
  const maOld = sma20[Math.max(0, sma20.length-6)];
  const maSlope = maOld > 0 ? (maRecent - maOld) / maOld * 100 : 0;
  const atrHistory: number[] = [];
  for (let i = Math.max(1, n-50); i < n; i++) atrHistory.push(Math.max(highs[i]-lows[i], Math.abs(highs[i]-closes[i-1]), Math.abs(lows[i]-closes[i-1])));
  const atrMed = b50Mean(atrHistory);
  const volPercentile = atrMed > 0 ? Math.min(1, atr / (atrMed * 2)) : 0.5;
  const avgVol = b50Mean(volumes.slice(-20));
  const curVol = volumes[n-1] || 0;
  const relVol = avgVol > 0 ? curVol / avgVol : 1;
  let type: Boof70Regime['type'];
  let shouldTrade = true; let noTradeReason: string | undefined;
  const isExplosive = bbWidth > 0.08 && adx > 35 && volPercentile > 0.85;
  const isHighVol   = volPercentile > 0.75 || atrPercent > 3.5;
  const isLowVol    = volPercentile < 0.20 && bbWidth < 0.02;
  const isTrending  = adx > 22 && Math.abs(maSlope) > 0.15;
  const isRange     = adx < 18 && bbWidth < 0.04;
  if (isExplosive)             { type = 'EXPLOSIVE'; }
  else if (isHighVol && !isTrending) { type = 'HIGH_VOL'; shouldTrade = false; noTradeReason = `HIGH_VOL chop`; }
  else if (isLowVol)           { type = 'LOW_VOL';  shouldTrade = false; noTradeReason = `LOW_VOL dead zone`; }
  else if (isTrending)         { type = maSlope > 0 ? 'TREND_UP' : 'TREND_DOWN'; }
  else if (isRange)            { type = 'RANGE'; }
  else                         { type = maSlope > 0 ? 'TREND_UP' : 'TREND_DOWN'; }
  if (relVol < 0.4 && avgVol > 0) { shouldTrade = false; noTradeReason = `Low volume`; }
  return { type, adx, atr, atrPercent, bbWidth, maSlope, volatilityPercentile: volPercentile, shouldTrade, noTradeReason };
}

function runRegimeStrategy80(regime: Boof70Regime, candles: any[], tradeDirection: string): { signal: 'buy'|'sell'|'none'; reason: string } {
  const closes  = candles.map((c: any) => c.close);
  const highs   = candles.map((c: any) => c.high);
  const lows    = candles.map((c: any) => c.low);
  const volumes = candles.map((c: any) => c.volume || 1);
  const n = closes.length; const i = n - 2;
  const rsi = calcRSI(closes, 14); const curRSI = rsi[rsi.length-2] ?? 50;
  const atrVals: number[] = [];
  for (let j = 1; j < n; j++) atrVals.push(Math.max(highs[j]-lows[j], Math.abs(highs[j]-closes[j-1]), Math.abs(lows[j]-closes[j-1])));
  const atrNow = b50Mean(atrVals.slice(-14)); const atrAvg = b50Mean(atrVals.slice(-34,-14));
  const candleBody = Math.abs(closes[i] - candles[i].open);
  const candleBodyPct = candles[i].open > 0 ? candleBody / candles[i].open * 100 : 0;
  const atrSpike = atrNow > atrAvg * 2.0;
  const volAvg = b50Mean(volumes.slice(-20));
  const volSpike = volumes[i] > volAvg * 1.8;
  const bearFlush = closes[i] < candles[i].open && candleBodyPct > 0.12 && atrSpike && volSpike;
  const bullFlush = closes[i] > candles[i].open && candleBodyPct > 0.12 && atrSpike && volSpike;
  const ema21arr = calcEMA(closes, 21);
  const ema21Now = ema21arr[ema21arr.length-1]; const ema21Prev = ema21arr[ema21arr.length-2];
  const prevBearFlush = i >= 1 && closes[i-1] < candles[i-1].open && (candles[i-1].open > 0 ? Math.abs(closes[i-1]-candles[i-1].open)/candles[i-1].open*100 : 0) > 0.10;
  const recoveryBuy = curRSI < 30 && closes[i] > ema21Now && closes[i-1] <= ema21Prev && (prevBearFlush || curRSI < 25);
  if (regime.type === 'TREND_UP' || regime.type === 'TREND_DOWN' || regime.type === 'EXPLOSIVE') {
    const ema9 = calcEMA(closes, 9);
    const { hist } = calcMACD(closes, 12, 26, 9);
    const histLast = hist[hist.length-1] ?? 0; const histPrev = hist[hist.length-2] ?? 0;
    const emaUp = ema9[ema9.length-1] > ema21Now;
    const emaCrossedUp   = ema9[ema9.length-2] <= ema21Prev && emaUp;
    const emaCrossedDown = ema9[ema9.length-2] >= ema21Prev && !emaUp;
    const macdBull = histLast > 0 && histLast > histPrev; const macdBear = histLast < 0 && histLast < histPrev;
    const contBull = emaUp && macdBull && closes[i] > closes[i-1];
    const contBear = !emaUp && macdBear && closes[i] < closes[i-1];
    const rsiBuyOk = curRSI > 40 && curRSI < 75; const rsiSellOk = curRSI < 60 && curRSI > 25;
    const ema50arr = calcEMA(closes, 50);
    const sellSlopeOk = !(ema50arr[ema50arr.length-1] > ema50arr[Math.max(0, ema50arr.length-4)]);
    let signal: 'buy'|'sell'|'none' = 'none'; let reason = '';
    if      (recoveryBuy && tradeDirection !== 'short')                          { signal = 'buy';  reason = `Boof7.0 RECOVERY_BUY [${regime.type}] rsi=${curRSI.toFixed(1)}`; }
    else if (bearFlush && regime.type !== 'TREND_UP' && sellSlopeOk)            { signal = 'sell'; reason = `Boof7.0 FLUSH_BEAR [${regime.type}]`; }
    else if (bullFlush && regime.type !== 'TREND_DOWN')                          { signal = 'buy';  reason = `Boof7.0 FLUSH_BULL [${regime.type}]`; }
    else if ((emaCrossedUp  || contBull) && regime.type !== 'TREND_DOWN' && rsiBuyOk)               { signal = 'buy';  reason = `Boof7.0 BREAKOUT [${regime.type}] rsi=${curRSI.toFixed(1)}`; }
    else if ((emaCrossedDown || contBear) && regime.type !== 'TREND_UP'  && rsiSellOk && sellSlopeOk) { signal = 'sell'; reason = `Boof7.0 BREAKOUT [${regime.type}] rsi=${curRSI.toFixed(1)}`; }
    else { reason = `Boof7.0 NO_ENTRY [${regime.type}] rsi=${curRSI.toFixed(1)}`; }
    if (tradeDirection === 'long'  && signal === 'sell') signal = 'none';
    if (tradeDirection === 'short' && signal === 'buy')  signal = 'none';
    return { signal, reason };
  } else if (regime.type === 'RANGE') {
    const sma20 = b50SMA(closes, 20); const std20 = b50StdDev(closes, 20);
    const bbUpper = sma20[sma20.length-1] + 2 * std20; const bbLower = sma20[sma20.length-1] - 2 * std20;
    let signal: 'buy'|'sell'|'none' = 'none';
    if      (recoveryBuy && tradeDirection !== 'short')                   signal = 'buy';
    else if (closes[i] <= bbLower * 1.005 && curRSI < 40)                signal = 'buy';
    else if (closes[i] >= bbUpper * 0.995 && curRSI > 60)                signal = 'sell';
    if (tradeDirection === 'long'  && signal === 'sell') signal = 'none';
    if (tradeDirection === 'short' && signal === 'buy')  signal = 'none';
    return { signal, reason: `Boof7.0 MEAN_REV [RANGE] rsi=${curRSI.toFixed(1)}` };
  }
  return { signal: 'none', reason: `Boof7.0 NO_STRATEGY regime=${regime.type}` };
}

function calcPositionSize80(regime: Boof70Regime, recentWinRate: number, consecutiveLosses: number): number {
  const base: Record<string, number> = { TREND_UP:1.0, TREND_DOWN:1.0, RANGE:0.75, HIGH_VOL:0.5, LOW_VOL:0.5, EXPLOSIVE:0.6 };
  let size = base[regime.type] || 1.0;
  if (recentWinRate >= 0.60) size *= 1.25; else if (recentWinRate < 0.40) size *= 0.60;
  if (consecutiveLosses >= 5) size *= 0.25; else if (consecutiveLosses >= 3) size *= 0.50;
  if (regime.volatilityPercentile > 0.80) size *= 0.70;
  return Math.max(0.10, Math.min(1.50, size));
}

function calcChoppinessIndex80(highs: number[], lows: number[], closes: number[], period = 14): number {
  const n = closes.length;
  if (n < period + 1) return 50;
  let atrSum = 0;
  for (let i = n - period; i < n; i++) atrSum += Math.max(highs[i]-lows[i], Math.abs(highs[i]-closes[i-1]), Math.abs(lows[i]-closes[i-1]));
  const hh = Math.max(...highs.slice(n - period));
  const ll = Math.min(...lows.slice(n - period));
  const range = hh - ll;
  if (range === 0) return 50;
  return Math.max(0, Math.min(100, 100 * Math.log10(atrSum / range) / Math.log10(period)));
}

interface Boof80Context {
  recentTrades: { reason: string; pnlPct: number; regime: string }[];
  consecutiveLosses: number; recentWinRate: number; isCrypto: boolean;
}

function generateSignalBoof80(candles: any[], tradeDirection = 'both', context: Boof80Context = { recentTrades:[], consecutiveLosses:0, recentWinRate:0.5, isCrypto:false }): { signal:'buy'|'sell'|'none'; price:number; trend:number; ema:number; adx:number; reason:string; regime:string; dynamicTP:number; dynamicSL:number; positionSizePct:number; killSwitch:boolean; killReason?:string; choppiness:number; patternWeight:number; adaptedFromHistory:boolean } {
  const closes  = candles.map((c: any) => c.close);
  const highs   = candles.map((c: any) => c.high);
  const lows    = candles.map((c: any) => c.low);
  const volumes = candles.map((c: any) => c.volume || 1000000);
  const n = closes.length;
  const curPrice = closes[n-2] ?? closes[n-1];
  const { recentTrades, consecutiveLosses, recentWinRate, isCrypto } = context;
  const noResult = (reason: string, kill = false, killReason?: string) => ({ signal: 'none' as const, price: curPrice, trend: 0, ema: curPrice, adx: 0, reason, regime: 'NONE', dynamicTP: 0, dynamicSL: 0, positionSizePct: 0, killSwitch: kill, killReason, choppiness: 50, patternWeight: 1.0, adaptedFromHistory: false });
  if (n < 50)                        return noResult('Boof 8.0: insufficient data');
  if (consecutiveLosses >= 7)        return noResult(`Kill-switch: ${consecutiveLosses} consecutive losses`, true, `${consecutiveLosses} consecutive losses`);
  const timeCheck = isNoTradeZone80(isCrypto);
  if (timeCheck.skip)                return noResult(`Boof 8.0: ${timeCheck.reason}`);
  const regime = detectRegime80(highs, lows, closes, volumes);
  if (!regime.shouldTrade)           return noResult(`Boof 8.0: skipping — ${regime.noTradeReason}`);
  const ci = calcChoppinessIndex80(highs, lows, closes, 14);
  const marketState = ci > 62 ? 'CHOPPY' : ci < 38 ? 'TRENDING' : 'MIXED';
  if (ci > 72 && regime.type !== 'EXPLOSIVE') return noResult(`Boof 8.0: too choppy CI=${ci.toFixed(1)}`);
  const { signal, reason } = runRegimeStrategy80(regime, candles, tradeDirection);
  if (signal === 'none')             return noResult(`Boof 8.0 NO_ENTRY [${regime.type}] CI=${ci.toFixed(1)}`);
  // Pattern weight scoring
  const patternMatch = reason.match(/Boof7\.0\s+(\w+)/);
  const patternLabel = patternMatch?.[1] ?? 'UNKNOWN';
  const patternKey   = `${regime.type}:${patternLabel}`;
  const matched = recentTrades.filter((t: { reason: string; regime: string }) => t.reason?.includes(patternLabel) && t.regime === regime.type);
  const pWins   = matched.filter((t: { pnlPct: number }) => t.pnlPct > 0).length;
  const pLosses = matched.filter((t: { pnlPct: number }) => t.pnlPct <= 0).length;
  const patternWinRate = matched.length > 0 ? pWins / matched.length : 0.5;
  const avgWin  = pWins   > 0 ? matched.filter((t: { pnlPct: number }) => t.pnlPct > 0).reduce((a: number, t: { pnlPct: number }) => a + t.pnlPct, 0) / pWins : 0;
  const avgLoss = pLosses > 0 ? matched.filter((t: { pnlPct: number }) => t.pnlPct <= 0).reduce((a: number, t: { pnlPct: number }) => a + t.pnlPct, 0) / pLosses : 0;
  const expectancy = patternWinRate * avgWin + (1 - patternWinRate) * avgLoss;
  const patternWeight = Math.max(0.5, Math.min(1.5, 1.0 + expectancy / 4));
  const adaptedFromHistory = recentTrades.length >= 2;
  if (patternWeight < 0.65 && recentTrades.length >= 5) return noResult(`Boof 8.0: pattern ${patternKey} underperforming (${pWins}W/${pLosses}L)`);
  // Adaptive TP/SL
  const baseM: Record<string, { tp: number; sl: number }> = { TREND_UP:{tp:3.0,sl:1.2}, TREND_DOWN:{tp:3.0,sl:1.2}, RANGE:{tp:1.5,sl:1.0}, HIGH_VOL:{tp:4.0,sl:2.0}, LOW_VOL:{tp:1.0,sl:0.8}, EXPLOSIVE:{tp:5.0,sl:2.5} };
  const m = baseM[regime.type] || baseM['TREND_UP'];
  const ciScale     = ci > 62 ? 0.70 : ci < 38 ? 1.30 : 1.0 + (50 - ci) / 100;
  const volScale    = regime.volatilityPercentile > 0.75 ? 1.20 : regime.volatilityPercentile < 0.25 ? 0.85 : 1.0;
  const wrScale     = recentWinRate > 0.60 ? 1.15 : recentWinRate < 0.35 ? 0.75 : 1.0;
  const tpPct       = Math.max(1.0, Math.min(80,  ((curPrice + regime.atr * m.tp * ciScale * volScale * patternWeight * wrScale) - curPrice) / curPrice * 100));
  const slPct       = Math.max(-25, Math.min(-0.5, ((curPrice - regime.atr * m.sl * ciScale * volScale) - curPrice) / curPrice * 100));
  const trailPct    = Math.max(0.3, Math.min(3.0, regime.atr * 0.5 / curPrice * 100));
  const positionSizePct = calcPositionSize80(regime, recentWinRate, consecutiveLosses);
  const ema21val    = calcEMA(closes, 21);
  const fullReason  = `${reason} | CI=${ci.toFixed(1)}[${marketState}] pw=${patternWeight.toFixed(2)}(${pWins}W/${pLosses}L) tp=+${tpPct.toFixed(1)}% sl=${slPct.toFixed(1)}% trail=${trailPct.toFixed(1)}% adapted=${adaptedFromHistory}`;
  return { signal, price: curPrice, trend: regime.maSlope > 0 ? 1 : -1, ema: ema21val[ema21val.length-1] ?? curPrice, adx: regime.adx, reason: fullReason, regime: regime.type, dynamicTP: tpPct, dynamicSL: slPct, positionSizePct, killSwitch: false, choppiness: ci, patternWeight, adaptedFromHistory };
}

// ─────────────────────────────────────────────
// BOOF 2.0 ML-STYLE INDICATOR
// ─────────────────────────────────────────────

function generateSignalBoof20(candles: Candle[], tradeDirection = 'both', thresholdBuy = 0.0, thresholdSell = 0.0): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const n = closes.length;

  if (n < 25) {
    return { signal: 'none', price: closes[n - 1], trend: 0, ema: closes[n - 1], adx: 50, reason: 'Insufficient data for Boof 2.0' };
  }

  const length = 14, maFast = 5, maSlow = 20;

  // Past return
  const pastReturn: number[] = new Array(n).fill(0);
  for (let i = length; i < n; i++) {
    pastReturn[i] = (closes[i] - closes[i - length]) / closes[i - length];
  }

  // MA calculations
  const maFastVals: number[] = new Array(n).fill(NaN);
  const maSlowVals: number[] = new Array(n).fill(NaN);
  for (let i = maFast - 1; i < n; i++) {
    maFastVals[i] = closes.slice(i - maFast + 1, i + 1).reduce((a, b) => a + b, 0) / maFast;
  }
  for (let i = maSlow - 1; i < n; i++) {
    maSlowVals[i] = closes.slice(i - maSlow + 1, i + 1).reduce((a, b) => a + b, 0) / maSlow;
  }

  // RSI
  const rsi = calcRSI(closes, length);

  // Current bar
  const i = n - 2;
  const iPrev = i - 1;

  const calcPredicted = (idx: number) => {
    const rP = pastReturn[idx] || 0;
    const rM = (maFastVals[idx] - maSlowVals[idx]) / closes[idx] || 0;
    const rR = (rsi[idx] - 50) / 50 || 0;
    const atrSlice = highs.slice(idx - 13, idx + 1).map((h, j) => h - lows[idx - 13 + j]);
    const rA = Math.max(...atrSlice) / closes[idx] || 0;
    return 0.4 * rP + 0.3 * rM + 0.2 * rR - 0.1 * rA;
  };

  const predictedReturn = calcPredicted(i);
  const prevPredicted = iPrev >= 13 ? calcPredicted(iPrev) : 0;

  const curState = predictedReturn > thresholdBuy ? 1 : predictedReturn < thresholdSell ? -1 : 0;
  const prevState = prevPredicted > thresholdBuy ? 1 : prevPredicted < thresholdSell ? -1 : 0;
  const justFlipped = curState !== prevState;

  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `predicted=${predictedReturn.toFixed(4)}, rsi=${rsi[i]?.toFixed(1)}`;

  if (curState === 1 && justFlipped) {
    signal = 'buy';
    reason = `Boof 2.0 BUY CROSSOVER. ${reason}`;
  } else if (curState === -1 && justFlipped) {
    signal = 'sell';
    reason = `Boof 2.0 SELL CROSSOVER. ${reason}`;
  }

  if (tradeDirection === 'long' && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy') signal = 'none';

  return { signal, price: closes[i], trend: predictedReturn > 0 ? 1 : -1, ema: maSlowVals[i], adx: rsi[i], reason };
}

// ─────────────────────────────────────────────
// BOOF 3.0 — FAST REGIME SCALPER (1m optimized)
// Replaced KMeans (slow) with instant rule-based regime detection
// Same regime logic, ~100x faster execution
// ─────────────────────────────────────────────

type MarketRegime = 'Trend' | 'Range' | 'HighVol';

function generateSignalBoof30(candles: Candle[], tradeDirection = 'both'): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string, regime?: string, rsi?: number, slope?: number, atr?: number } {
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const volumes = candles.map(c => (c as any).volume || 1000000);
  const n = closes.length;
  const i = n - 2;

  if (n < 30) return { signal: 'none', price: closes[n-1], trend: 0, ema: closes[n-1], adx: 50, reason: 'Insufficient data' };

  // ── FAST EMA9 / EMA21 ──
  const ema9  = calcEMA(closes, 9);
  const ema21 = calcEMA(closes, 21);
  const ema9Now  = ema9[ema9.length-1];
  const ema9Prev = ema9[ema9.length-2];
  const ema21Now  = ema21[ema21.length-1];
  const ema21Prev = ema21[ema21.length-2];
  const maSlope = ema9Now - ema9[Math.max(0, ema9.length-4)];

  // ── FAST ATR (last 14 bars only) ──
  let atrSum = 0;
  for (let j = Math.max(1, n-14); j < n; j++) {
    atrSum += Math.max(highs[j]-lows[j], Math.abs(highs[j]-closes[j-1]), Math.abs(lows[j]-closes[j-1]));
  }
  const atrVal = atrSum / Math.min(14, n-1);
  const atrPct = closes[i] > 0 ? atrVal / closes[i] * 100 : 1;

  // ── FAST ADX (simplified DI from last 14 bars) ──
  let dmPlus = 0, dmMinus = 0, tr = 0;
  for (let j = Math.max(1, n-14); j < n; j++) {
    const upMove = highs[j] - highs[j-1];
    const downMove = lows[j-1] - lows[j];
    dmPlus  += (upMove > downMove && upMove > 0) ? upMove : 0;
    dmMinus += (downMove > upMove && downMove > 0) ? downMove : 0;
    tr += Math.max(highs[j]-lows[j], Math.abs(highs[j]-closes[j-1]), Math.abs(lows[j]-closes[j-1]));
  }
  const diPlus  = tr > 0 ? 100 * dmPlus  / tr : 0;
  const diMinus = tr > 0 ? 100 * dmMinus / tr : 0;
  const adxVal  = (diPlus + diMinus) > 0 ? 100 * Math.abs(diPlus - diMinus) / (diPlus + diMinus) : 0;

  // ── RSI (last 14 bars) ──
  const rsiArr = calcRSI(closes, 14);
  const curRSI = rsiArr[rsiArr.length-2] ?? 50;

  // ── FAST VOLUME CHECK ──
  const volSlice = volumes.slice(-20);
  const volAvg = volSlice.reduce((a, b) => a + b, 0) / volSlice.length;
  const relVol = volAvg > 0 ? volumes[i] / volAvg : 1;

  // ── REGIME CLASSIFICATION (rule-based, replaces KMeans) ──
  let regime: MarketRegime;
  if (atrPct > 2.5 && adxVal < 20) {
    regime = 'HighVol';
  } else if (adxVal >= 18 && Math.abs(maSlope) > closes[i] * 0.0002) {
    regime = 'Trend';
  } else {
    regime = 'Range';
  }

  // ── SIGNAL LOGIC per regime ──
  const minSlope = closes[i] * 0.0002;
  const emaCrossUp   = ema9Prev <= ema21Prev && ema9Now > ema21Now;
  const emaCrossDown = ema9Prev >= ema21Prev && ema9Now < ema21Now;
  const contBull = ema9Now > ema21Now && maSlope > minSlope && closes[i] > closes[i-1];
  const contBear = ema9Now < ema21Now && maSlope < -minSlope && closes[i] < closes[i-1];

  let sigVal = 0;
  if (regime === 'Trend' || regime === 'HighVol') {
    if ((emaCrossUp  || contBull) && curRSI > 40 && curRSI < 75) sigVal = 1;
    else if ((emaCrossDown || contBear) && curRSI < 60 && curRSI > 25) sigVal = -1;
  } else {
    // Range: BB bounce
    const sma20 = closes.slice(-20).reduce((a, b) => a + b, 0) / 20;
    const std20 = Math.sqrt(closes.slice(-20).reduce((a, b) => a + (b - sma20) ** 2, 0) / 20);
    const bbLower = sma20 - 2 * std20;
    const bbUpper = sma20 + 2 * std20;
    if (closes[i] <= bbLower * 1.005 && curRSI < 38) sigVal = 1;
    else if (closes[i] >= bbUpper * 0.995 && curRSI > 62) sigVal = -1;
  }

  // Volume gate — skip thin volume
  if (relVol < 0.4) sigVal = 0;

  let signal: 'buy' | 'sell' | 'none' = sigVal === 1 ? 'buy' : sigVal === -1 ? 'sell' : 'none';
  const reason = `Boof3.0 ${signal.toUpperCase()} [${regime}] adx=${adxVal.toFixed(1)} rsi=${curRSI.toFixed(1)} slope=${maSlope.toFixed(3)} atr=${atrPct.toFixed(2)}%`;

  if (tradeDirection === 'long'  && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy')  signal = 'none';

  return { signal, price: closes[i], trend: maSlope > 0 ? 1 : -1, ema: ema21Now, adx: adxVal, reason, regime, rsi: curRSI, slope: maSlope, atr: atrVal };
}

// ─────────────────────────────────────────────
// BOOF 5.0 - QUANTITUTIONAL SIGNAL GENERATION
// Six-Factor Model: Momentum, Mean Reversion, Volatility, Trend, Volume, Microstructure
// ─────────────────────────────────────────────

function generateSignalBoof50(candles: Candle[], tradeDirection = 'both', trendFilterCandles?: Candle[]): { 
  signal: 'buy' | 'sell' | 'none', 
  price: number, 
  trend: number, 
  ema: number, 
  adx: number, 
  reason: string, 
  regime?: string, 
  rsi?: number, 
  slope?: number, 
  atr?: number,
  compositeScore?: number,
  positionSize?: number
} {
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const opens = candles.map(c => c.open);
  const volumes = (candles as any[]).map(c => (c as any).volume || 1000000);
  const n = closes.length;

  if (n < 50) return { signal: 'none', price: closes[n - 1], trend: 0, ema: closes[n - 1], adx: 0, reason: 'Insufficient data', compositeScore: 0, positionSize: 1 };

  const i = n - 2; // Current bar

  // ── FACTOR 1: MOMENTUM (Price Velocity & Acceleration) ──
  const ema20 = calcEMA(closes, 20);
  const ema50 = calcEMA(closes, 50);
  const ema200 = calcEMA(closes, 200);
  
  // Price momentum (10-period)
  const momentum = ((closes[i] - closes[i - 10]) / closes[i - 10]) * 100;
  const momentumPrev = ((closes[i - 1] - closes[i - 11]) / closes[i - 11]) * 100;
  const momentumAccel = momentum - momentumPrev;
  
  // Momentum score (-2 to +2)
  let momScore = 0;
  if (momentum > 1.5 && momentumAccel > 0) momScore = 2;
  else if (momentum > 0.5) momScore = 1;
  else if (momentum < -1.5 && momentumAccel < 0) momScore = -2;
  else if (momentum < -0.5) momScore = -1;

  // ── FACTOR 2: MEAN REVERSION (Z-Score & Bollinger Position) ──
  const sma20 = boof50SMA(closes, 20);
  const std20 = boof50StdDev(closes, 20);
  const zScore = std20 > 0 ? (closes[i] - sma20[sma20.length - 1]) / std20 : 0;
  
  // Bollinger position (0-1)
  const bbUpper = sma20[sma20.length - 1] + (2 * std20);
  const bbLower = sma20[sma20.length - 1] - (2 * std20);
  const bbPosition = bbUpper !== bbLower ? (closes[i] - bbLower) / (bbUpper - bbLower) : 0.5;
  
  // Mean reversion score (-1 to +1)
  let mrScore = 0;
  if (zScore < -1.5 && bbPosition < 0.1) mrScore = 1; // Oversold - bullish mean reversion
  else if (zScore > 1.5 && bbPosition > 0.9) mrScore = -1; // Overbought - bearish mean reversion

  // ── FACTOR 3: VOLATILITY REGIME ──
  const returns: number[] = [];
  for (let j = 1; j < n; j++) returns.push((closes[j] - closes[j - 1]) / closes[j - 1]);
  const currentVol = boof50StdDev(returns.slice(-20), 20);
  const volMean = boof50Mean(returns.slice(-50).map(r => Math.abs(r)));
  const volPercentile = volMean > 0 ? Math.min(1, currentVol / (volMean * 2)) : 0.5;
  
  // Volatility regime
  const highVol = volPercentile > 0.8;
  const lowVol = volPercentile < 0.2;
  
  // ATR for position sizing
  const atr = boof50ATR(highs, lows, closes, 14);
  const atrPercent = atr / closes[i] * 100;

  // ── FACTOR 4: TREND STRENGTH (ADX & Multi-Timeframe) ──
  const adx = boof50ADX(highs, lows, closes, 14);
  const strongTrend = adx > 25;
  const weakTrend = adx < 20;
  
  // Price vs EMA alignment
  const aboveEMA20 = closes[i] > ema20[ema20.length - 1];
  const aboveEMA50 = closes[i] > ema50[ema50.length - 1];
  const aboveEMA200 = closes[i] > ema200[ema200.length - 1];
  
  // Trend score (-2 to +2)
  let trendScore = 0;
  if (strongTrend && aboveEMA20 && aboveEMA50 && aboveEMA200) trendScore = 2;
  else if (aboveEMA20 && aboveEMA50) trendScore = 1;
  else if (strongTrend && !aboveEMA20 && !aboveEMA50 && !aboveEMA200) trendScore = -2;
  else if (!aboveEMA20 && !aboveEMA50) trendScore = -1;
  
  // Trend filter check (if provided)
  let trendAligned = true;
  if (trendFilterCandles && trendFilterCandles.length >= 50) {
    const tfCloses = trendFilterCandles.map(c => c.close);
    const tfEma = boof50SMA(tfCloses, 20); // Use SMA as proxy for EMA
    const tfPrice = tfCloses[tfCloses.length - 1];
    const tfEmaVal = tfEma[tfEma.length - 1];
    trendAligned = momScore > 0 ? tfPrice > tfEmaVal : tfPrice < tfEmaVal;
  }

  // ── FACTOR 5: VOLUME ANALYSIS ──
  const volSMA = boof50SMA(volumes, 20);
  const relVolume = volumes[i] / volSMA[volSMA.length - 1];
  const volIncreasing = relVolume > 1.2;
  
  // OBV momentum (simplified - just check last few bars)
  let obv = 0;
  for (let j = Math.max(1, n - 20); j < n; j++) {
    obv += closes[j] > closes[j - 1] ? volumes[j] : closes[j] < closes[j - 1] ? -volumes[j] : 0;
  }
  const obvMomentum = obv > 0;
  
  // Volume score (0 to 1)
  const volScore = (volIncreasing && obvMomentum) ? 1 : 0;

  // ── FACTOR 6: MARKET MICROSTRUCTURE ──
  const body = Math.abs(closes[i] - opens[i]);
  const wick = highs[i] - lows[i];
  const bodyRatio = wick > 0 ? body / wick : 0;
  
  // Rejection patterns
  const upperWick = highs[i] - Math.max(opens[i], closes[i]);
  const lowerWick = Math.min(opens[i], closes[i]) - lows[i];
  const upperRejection = wick > 0 ? upperWick / wick > 0.6 : false;
  const lowerRejection = wick > 0 ? lowerWick / wick > 0.6 : false;
  
  // Microstructure score (-1 to +1)
  let microScore = 0;
  if (lowerRejection && bodyRatio > 0.3) microScore = 1;
  else if (upperRejection && bodyRatio > 0.3) microScore = -1;

  // ── REGIME CLASSIFICATION ──
  let regime = 'UNCERTAIN';
  if (strongTrend && !weakTrend) regime = aboveEMA50 ? 'TREND_UP' : 'TREND_DOWN';
  else if (weakTrend && Math.abs(zScore) < 1) regime = 'RANGING';
  else if (highVol) regime = 'VOLATILE';

  // ── COMPOSITE SCORING ──
  // Base composite (-5 to +5)
  let composite = momScore + trendScore + mrScore + volScore + microScore;
  
  // Regime adjustments
  if (regime === 'RANGING') {
    // Mean reversion mode: fade extremes
    composite = -composite * 0.5;
  } else if (regime === 'VOLATILE' && !strongTrend) {
    // Chop mode: reduce signals
    composite = composite * 0.3;
  }
  
  // Smooth the composite
  const compositeSmooth = composite; // Could add EMA smoothing here

  // ── SIGNAL GENERATION ──
  const thresholdBuy = 1.5; // Lowered from 2.5 for 10-15 trades/day
  const thresholdSell = -1.5;
  
  // Require confirmation (2 bars)
  const rawBuy = compositeSmooth > thresholdBuy;
  const rawSell = compositeSmooth < thresholdSell;
  const prevBuy = i > 0 ? (momScore > 0 && trendScore > 0) : false;
  const prevSell = i > 0 ? (momScore < 0 && trendScore < 0) : false;
  
  let signal: 'buy' | 'sell' | 'none' = 'none';
  
  if (rawBuy && trendAligned && !highVol && regime !== 'VOLATILE') {
    signal = 'buy';
  } else if (rawSell && trendAligned && !highVol && regime !== 'VOLATILE') {
    signal = 'sell';
  }
  
  // Apply trade direction filter
  if (tradeDirection === 'long' && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy') signal = 'none';

  // ── POSITION SIZING (Kelly Criterion) ──
  // Win rate assumption based on composite strength
  const winRate = 0.55 + (Math.abs(compositeSmooth) / 10);
  const avgWinLossRatio = 2.0;
  const kelly = winRate - ((1 - winRate) / avgWinLossRatio);
  
  // Volatility adjustment
  const volAdj = highVol ? 0.5 : lowVol ? 1.2 : 1.0;
  const positionSize = Math.max(0.1, Math.min(1.0, kelly * volAdj));

  // ── REASON STRING ──
  const reason = `Boof 5.0 [${regime}] MOM=${momScore} TREND=${trendScore} MR=${mrScore} VOL=${volScore} MICRO=${microScore} COMPOSITE=${compositeSmooth.toFixed(2)} SIZE=${(positionSize * 100).toFixed(0)}%`;

  return { 
    signal, 
    price: closes[i], 
    trend: trendScore, 
    ema: ema50[ema50.length - 1], 
    adx, 
    reason, 
    regime, 
    rsi: zScore * 10 + 50, // Approximate RSI from z-score
    slope: momentum,
    atr,
    compositeScore: compositeSmooth,
    positionSize
  };
}

// ─────────────────────────────────────────────
// HELPER FUNCTIONS FOR BOOF 5.0
// ─────────────────────────────────────────────

function boof50SMA(data: number[], period: number): number[] {
  const result: number[] = [];
  for (let i = period - 1; i < data.length; i++) {
    const slice = data.slice(i - period + 1, i + 1);
    result.push(slice.reduce((a, b) => a + b, 0) / period);
  }
  return result;
}

function boof50StdDev(data: number[], period: number): number {
  if (data.length < period) return 0;
  const slice = data.slice(-period);
  const mean = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / period;
  return Math.sqrt(variance);
}

function boof50Mean(data: number[]): number {
  return data.length > 0 ? data.reduce((a, b) => a + b, 0) / data.length : 0;
}

function boof50ATR(highs: number[], lows: number[], closes: number[], period: number): number {
  const trValues: number[] = [];
  for (let i = 1; i < closes.length; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    );
    trValues.push(tr);
  }
  return trValues.length >= period ? boof50Mean(trValues.slice(-period)) : 0;
}

function boof50ADX(highs: number[], lows: number[], closes: number[], period: number): number {
  const dmPlus: number[] = [];
  const dmMinus: number[] = [];
  const trValues: number[] = [];
  
  for (let i = 1; i < closes.length; i++) {
    const upMove = highs[i] - highs[i - 1];
    const downMove = lows[i - 1] - lows[i];
    dmPlus.push(upMove > downMove && upMove > 0 ? upMove : 0);
    dmMinus.push(downMove > upMove && downMove > 0 ? downMove : 0);
    
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    );
    trValues.push(tr);
  }
  
  if (dmPlus.length < period) return 25;
  
  const diPlus = 100 * boof50Mean(dmPlus.slice(-period)) / boof50Mean(trValues.slice(-period));
  const diMinus = 100 * boof50Mean(dmMinus.slice(-period)) / boof50Mean(trValues.slice(-period));
  const dx = (diPlus + diMinus) > 0 ? 100 * Math.abs(diPlus - diMinus) / (diPlus + diMinus) : 0;
  
  return dx;
}

// ─────────────────────────────────────────────
// BOOF 6.0 — MULTI-TIMEFRAME SCALPING SYSTEM
// Best-of-breed: Renaissance regime detection + Citadel direction lock +
// TastyTrade IV filter + LBR-style pullback entry + TTM momentum confirmation
// Target: 10-15 high-quality scalp entries per day
// ─────────────────────────────────────────────
function generateSignalBoof60(
  candles: Candle[],           // signal-interval candles (e.g. 5m)
  candles1h: Candle[],         // 1h candles for trend lock
  candles15m: Candle[],        // 15m candles for EMA confirmation
  candles1m: Candle[],         // 1m candles for VWAP
  tradeDirection: string
): { signal: 'buy' | 'sell' | 'none', price: number, ema: number, adx: number, reason: string } {

  const n = candles.length;
  if (n < 30) return { signal: 'none', price: 0, ema: 0, adx: 0, reason: 'Not enough candles' };

  const closes  = candles.map(c => c.close);
  const highs   = candles.map(c => c.high);
  const lows    = candles.map(c => c.low);
  const volumes = candles.map(c => c.volume ?? 0);
  const curClose = closes[n - 1];
  const prevClose = closes[n - 2];
  const prev2Close = closes[n - 3];

  // Detect if running on 1m candles by checking average spacing between candles
  const avgSpacingSec = n > 2 ? (candles[n-1].time - candles[n-10].time) / (9 * 1000) : 300;
  const is1m = avgSpacingSec < 90; // < 90s between candles = 1m

  // ── FACTOR 1: 1H TREND LOCK (direction-only gate) ──
  // Uses 1h EMA20 slope. Up = calls only. Down = puts only. Flat = skip.
  let trendBias: 'up' | 'down' | 'flat' = 'flat';
  if (candles1h.length >= 25) {
    const closes1h = candles1h.map(c => c.close);
    const ema1h = calcEMA(closes1h, 20);
    const emaLast = ema1h[ema1h.length - 1];
    const emaPrev = ema1h[ema1h.length - 5]; // slope over last 5 1h candles
    const emaSlope = (emaLast - emaPrev) / emaPrev;
    const price1h = closes1h[closes1h.length - 1];
    if (price1h > emaLast && emaSlope > 0.0003) trendBias = 'up';
    else if (price1h < emaLast && emaSlope < -0.0003) trendBias = 'down';
    // else flat — no trade
  }
  if (trendBias === 'flat') {
    return { signal: 'none', price: curClose, ema: 0, adx: 0, reason: 'Boof 6.0: 1h trend flat — no directional bias, skipping' };
  }

  // ── FACTOR 2: ADX TRENDING CONFIRMATION ──
  // 1m candles have naturally lower ADX values — use 14 threshold instead of 18
  const { adx: adxArr } = calcDMI(highs, lows, closes, 14);
  const adxVal = adxArr[adxArr.length - 1] ?? 0;
  const adxMin = is1m ? 14 : 18;
  if (adxVal < adxMin) {
    return { signal: 'none', price: curClose, ema: 0, adx: adxVal, reason: `Boof 6.0: ADX=${adxVal.toFixed(1)} too low (chop, min=${adxMin}), skipping` };
  }

  // ── FACTOR 3: EMA PRICE SIDE CONFIRMATION ──
  // On 1m: use 5m EMA20 (from candles15m reused as 5m ref, or fall back to 1m EMA50)
  // On 5m+: use 15m EMA20
  let ema15Val = 0;
  const emaLabel = is1m ? '5m' : '15m';
  if (candles15m.length >= 22) {
    const closes15m = candles15m.map(c => c.close);
    const emaPeriod = is1m ? 50 : 20; // 1m uses EMA50 on signal candles as proxy for 5m trend
    const ema15 = calcEMA(is1m ? closes : closes15m, emaPeriod);
    ema15Val = ema15[ema15.length - 1] ?? 0;
  }
  if (ema15Val > 0) {
    if (trendBias === 'up' && curClose < ema15Val) {
      return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: BUY blocked — close $${curClose.toFixed(2)} < ${emaLabel} EMA $${ema15Val.toFixed(2)}` };
    }
    if (trendBias === 'down' && curClose > ema15Val) {
      return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: SELL blocked — close $${curClose.toFixed(2)} > ${emaLabel} EMA $${ema15Val.toFixed(2)}` };
    }
  }

  // ── FACTOR 4: VWAP POSITION + BOUNCE ENTRY ──
  // Price must be on correct side of VWAP AND bouncing back toward it (pullback entry)
  let vwapVal = 0;
  let vwapConfirmed = false;
  if (candles1m.length >= 30) {
    vwapVal = calcVWAP(candles1m);
    const aboveVwap = curClose >= vwapVal;
    const prevAbove = prevClose >= vwapVal;
    // For calls: price above VWAP, bouncing up after a dip toward VWAP
    // For puts: price below VWAP, bouncing down after a push toward VWAP
    if (trendBias === 'up' && aboveVwap && prevAbove) vwapConfirmed = true;
    if (trendBias === 'down' && !aboveVwap && !prevAbove) vwapConfirmed = true;
  }
  if (!vwapConfirmed) {
    return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: VWAP position not confirmed for ${trendBias} bias (close=$${curClose.toFixed(2)} vwap=$${vwapVal.toFixed(2)})` };
  }

  // ── FACTOR 5: MACD HISTOGRAM FLIP (momentum turning) ──
  // 1m uses faster MACD 5/13/4 to reduce lag on 1m candles
  const { hist } = is1m ? calcMACD(closes, 5, 13, 4) : calcMACD(closes, 12, 26, 9);
  const histLast = hist[hist.length - 1] ?? 0;
  const histPrev = hist[hist.length - 2] ?? 0;
  const macdFlipBull = histLast > histPrev && histLast > 0; // histogram rising and positive
  const macdFlipBear = histLast < histPrev && histLast < 0; // histogram falling and negative
  // Also allow if hist just crossed zero
  const macdCrossedBull = histPrev <= 0 && histLast > 0;
  const macdCrossedBear = histPrev >= 0 && histLast < 0;
  const macdOK = trendBias === 'up'
    ? (macdFlipBull || macdCrossedBull)
    : (macdFlipBear || macdCrossedBear);
  if (!macdOK) {
    return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: MACD histogram not confirming ${trendBias} momentum (hist=${histLast.toFixed(4)})` };
  }

  // ── FACTOR 6: MOMENTUM BUILDING ──
  // 1m: net direction over last 5 candles (less strict — 1m candles reverse constantly)
  // 5m+: 3 consecutive closes in direction
  let momOK = false;
  if (is1m) {
    const ref5 = closes[n - 6] ?? closes[0];
    momOK = trendBias === 'up' ? curClose > ref5 : curClose < ref5;
  } else {
    const momUp = curClose > prevClose && prevClose > prev2Close;
    const momDown = curClose < prevClose && prevClose < prev2Close;
    const momUpRelaxed = (curClose > prev2Close) && (curClose > prevClose || prevClose > prev2Close);
    const momDownRelaxed = (curClose < prev2Close) && (curClose < prevClose || prevClose < prev2Close);
    momOK = trendBias === 'up' ? (momUp || momUpRelaxed) : (momDown || momDownRelaxed);
  }
  if (!momOK) {
    return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: Momentum not building for ${trendBias} (close=${curClose.toFixed(2)} is1m=${is1m})` };
  }

  // ── FACTOR 7: VOLUME CONFIRMATION ──
  // Current candle volume should be above 20-period average (conviction)
  const avgVol = volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;
  const curVol = volumes[n - 1];
  const volConfirmed = avgVol <= 0 || curVol >= avgVol * 0.8; // 80% of avg minimum
  if (!volConfirmed) {
    return { signal: 'none', price: curClose, ema: ema15Val, adx: adxVal, reason: `Boof 6.0: Volume too low (cur=${curVol} < 80% avg=${(avgVol*0.8).toFixed(0)})` };
  }

  // ── APPLY TRADE DIRECTION OVERRIDE ──
  let signal: 'buy' | 'sell' | 'none' = trendBias === 'up' ? 'buy' : 'sell';
  if (tradeDirection === 'long' && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy') signal = 'none';

  const reason = `Boof 6.0 [${trendBias.toUpperCase()}${is1m?'/1m':'/5m+'}] adx=${adxVal.toFixed(1)} macd=${histLast.toFixed(4)} vwap=$${vwapVal.toFixed(2)} ema=$${ema15Val.toFixed(2)} vol=${curVol}/${avgVol.toFixed(0)}${is1m?' (1m-tuned)':''}`;

  return { signal, price: curClose, ema: ema15Val, adx: adxVal, reason };
}

// ─────────────────────────────────────────────
// FETCH CANDLES (Yahoo Finance - Free)
// ─────────────────────────────────────────────

interface Candle { time: number; open: number; high: number; low: number; close: number; volume?: number; }

async function fetchAlpacaSpotPrice(symbol: string, api_key: string, secret_key: string): Promise<number | null> {
  try {
    const res = await fetch(`https://data.alpaca.markets/v2/stocks/${symbol}/quotes/latest`, {
      headers: { 'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': secret_key }
    });
    const json = await res.json();
    const ask = json?.quote?.ap;
    const bid = json?.quote?.bp;
    if (ask > 0 && bid > 0) {
      const mid = (ask + bid) / 2;
      console.log(`[OptionsBot] Alpaca spot ${symbol} = $${mid.toFixed(2)} (bid=$${bid} ask=$${ask})`);
      return mid;
    }
  } catch (_) {}
  return null;
}

// Sanity check: spot price must be positive, within 50% of candle close, and within hard bounds for known symbols
// referencePrice=0 skips cross-check
function sanityCheckSpot(symbol: string, price: number, referencePrice = 0): boolean {
  if (!price || price <= 0) return false;
  
  // Hard bounds for major ETFs/stocks (catches stale data from months/years ago)
  const hardBounds: Record<string, { min: number; max: number }> = {
    'QQQ': { min: 350, max: 1000 },
    'SPY': { min: 400, max: 1000 },
    'AMD': { min: 50, max: 600 },
    'NVDA': { min: 80, max: 600 },
    'TSLA': { min: 150, max: 900 },
    'IWM': { min: 150, max: 350 },
    'DIA': { min: 300, max: 550 },
    'AAPL': { min: 150, max: 350 },
    'MSFT': { min: 300, max: 600 },
    'GOOGL': { min: 130, max: 280 },
    'AMZN': { min: 150, max: 350 },
    'META': { min: 400, max: 800 },
    'NFLX': { min: 500, max: 1200 },
    'PLTR': { min: 15, max: 200 },
    'MSTR': { min: 200, max: 2000 },
    'COIN': { min: 100, max: 500 },
  };
  
  const bounds = hardBounds[symbol.toUpperCase()];
  if (bounds) {
    if (price < bounds.min || price > bounds.max) {
      console.log(`[OptionsBot] SANITY FAIL: ${symbol} price $${price.toFixed(2)} outside hard bounds $${bounds.min}-$${bounds.max} — stale data suspected, rejecting`);
      return false;
    }
  }
  
  if (referencePrice > 0) {
    const pct = Math.abs(price - referencePrice) / referencePrice;
    if (pct > 0.50) {
      console.log(`[OptionsBot] SANITY FAIL: ${symbol} spot $${price.toFixed(2)} is ${(pct*100).toFixed(1)}% away from candle close $${referencePrice.toFixed(2)} — rejecting`);
      return false;
    }
  }
  return true;
}

async function fetchTastytradeSpotPrice(symbol: string, accessToken: string): Promise<number | null> {
  try {
    // Tastytrade equity quotes endpoint
    const res = await fetch(`https://api.tastytrade.com/market-data/quotes?symbols[]=${encodeURIComponent(symbol)}`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    const json = await res.json();
    const quote = json?.data?.items?.[0];
    const mid = quote?.mid || ((Number(quote?.bid) + Number(quote?.ask)) / 2) || quote?.last;
    if (mid && mid > 0 && sanityCheckSpot(symbol, mid, 0)) { // reference=0: no candle available here, basic check only
      console.log(`[OptionsBot] Tastytrade real-time spot ${symbol} = $${mid} (bid=$${quote?.bid} ask=$${quote?.ask})`);
      return mid;
    }
    console.log(`[OptionsBot] Tastytrade spot bad/missing for ${symbol}: mid=${mid} raw=${JSON.stringify(quote)}`);
  } catch (err) {
    console.log(`[OptionsBot] Tastytrade spot fetch failed for ${symbol}:`, err);
  }
  return null;
}

async function fetchSpotPrice(symbol: string, alpacaApiKey?: string, alpacaSecretKey?: string): Promise<number | null> {
  // For paper trading: use real-time Alpaca data for accurate backtesting
  if (alpacaApiKey && alpacaSecretKey) {
    const p = await fetchAlpacaSpotPrice(symbol, alpacaApiKey, alpacaSecretKey);
    if (p) return p;
  }
  // Fallback: Yahoo real-time (better than delayed for paper testing)
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`;
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' } });
    const json = await res.json();
    const meta = json?.chart?.result?.[0]?.meta;
    const p = meta?.regularMarketPrice ?? meta?.price;
    if (p && p > 0) { console.log(`[OptionsBot] Yahoo spot ${symbol} = $${p}`); return p; }
  } catch (_) {}
  return null;
}

async function fetchCandles(symbol: string, interval = '1h', bars = 150, alpacaApiKey?: string, alpacaSecretKey?: string): Promise<Candle[]> {
  const isCrypto  = symbol.includes('-USD') || symbol.includes('/USD');
  const isFutures = symbol.includes('=F');

  // ── ALPACA FIRST (stocks only, lower latency, no rate limits) ──
  if (!isCrypto && !isFutures) {
    try {
      if (alpacaApiKey && alpacaSecretKey) {
        const alpacaIntervalMap: Record<string, string> = {
          '1m': '1Min', '5m': '5Min', '15m': '15Min', '30m': '30Min',
          '1h': '1Hour', '4h': '4Hour', '1d': '1Day',
        };
        const timeframe = alpacaIntervalMap[interval] || '5Min';
        const limit = Math.min(bars + 10, 1000);
        const url = `https://data.alpaca.markets/v2/stocks/${encodeURIComponent(symbol)}/bars?timeframe=${timeframe}&limit=${limit}&adjustment=raw&feed=sip`;
        const res = await fetch(url, {
          headers: { 'APCA-API-KEY-ID': alpacaApiKey, 'APCA-API-SECRET-KEY': alpacaSecretKey }
        });
        if (res.ok) {
          const json = await res.json();
          const bars_data = json?.bars || [];
          if (bars_data.length >= 30) {
            const candles: Candle[] = bars_data.map((b: any) => ({
              time:   new Date(b.t).getTime(),
              open:   b.o, high: b.h, low: b.l, close: b.c, volume: b.v ?? 0
            }));
            console.log(`[OptionsBot] Alpaca candles ${symbol} (${interval}): ${candles.length} bars`);
            return candles.slice(-bars);
          }
        }
      }
    } catch (e) {
      console.warn(`[OptionsBot] Alpaca candle fetch failed for ${symbol}, falling back to Yahoo:`, e);
    }
  }

  // ── YAHOO FALLBACK (crypto, futures, or Alpaca failure) ──
  const intervalMap: Record<string, { yahooInterval: string; range: string }> = {
    '1m':  { yahooInterval: '1m',  range: '5d'  },
    '5m':  { yahooInterval: '5m',  range: '5d'  },
    '10m': { yahooInterval: '15m', range: '5d'  },
    '15m': { yahooInterval: '15m', range: '5d'  },
    '30m': { yahooInterval: '30m', range: '1mo' },
    '45m': { yahooInterval: '60m', range: '1mo' },
    '1h':  { yahooInterval: '60m', range: '1mo' },
    '2h':  { yahooInterval: '60m', range: '3mo' },
    '4h':  { yahooInterval: '60m', range: '6mo' },
    '1d':  { yahooInterval: '1d',  range: '1y'  },
  };
  const { yahooInterval, range } = intervalMap[interval] ?? intervalMap['1h'];
  const yahooSymbol = isCrypto ? symbol.replace('/', '-') : symbol;
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSymbol)}?interval=${yahooInterval}&range=${range}`;
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' } });
  if (!res.ok) throw new Error(`Yahoo API error: ${res.status}`);
  const json = await res.json();
  if (!json.chart?.result?.[0]) throw new Error(`No Yahoo data for ${symbol}`);
  const result = json.chart.result[0];
  const timestamps = result.timestamp || [];
  const quote = result.indicators?.quote?.[0] || {};
  const candles: Candle[] = [];
  for (let i = 0; i < timestamps.length; i++) {
    if (quote.open?.[i] && quote.high?.[i] && quote.low?.[i] && quote.close?.[i]) {
      candles.push({ time: timestamps[i] * 1000, open: quote.open[i], high: quote.high[i], low: quote.low[i], close: quote.close[i], volume: quote.volume?.[i] ?? 0 });
    }
  }
  if (candles.length < 30) throw new Error(`Not enough data for ${symbol} (got ${candles.length} candles)`);
  console.log(`[OptionsBot] Yahoo candles ${symbol} (${interval}): ${candles.length} bars`);
  return candles.slice(-bars);
}

// ─────────────────────────────────────────────
// SIGNAL GENERATION
// ─────────────────────────────────────────────

function generateSignal(candles: Candle[], settings: BotSettings): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const n = closes.length;
  const tradeDirection = settings.tradeDirection || 'both';
  const emaArr = calcEMA(closes, settings.emaLength);
  const { trend } = calcSuperTrend(highs, lows, closes, settings.atrLength, settings.atrMultiplier);
  const { adx }   = calcDMI(highs, lows, closes, settings.adxLength);
  
  // Options bot: no position state replay needed - each contract is independent
  
  const i = n - 2;
  const curTrend = trend[i], prevTrend = trend[i - 1];
  const curEma = emaArr[i], curAdx = adx[i], curClose = closes[i];
  const trendJustFlipped = curTrend !== prevTrend;
  const longOK  = curTrend === 1;
  const shortOK = curTrend === -1;
  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `trend=${curTrend}, close=${curClose.toFixed(2)}, ema=${curEma.toFixed(2)}, adx=${curAdx?.toFixed(1)}`;
  // Only fire on a fresh trend flip (crossover) — never enter mid-trend
  if (longOK && trendJustFlipped) {
    signal = 'buy';
    reason = `TREND FLIP ENTER LONG. SuperTrend UP. ${reason}`;
  } else if (shortOK && trendJustFlipped && tradeDirection !== 'long') {
    signal = 'sell';
    reason = `TREND FLIP ENTER SHORT. SuperTrend DOWN. ${reason}`;
  }
  return { signal, price: curClose, trend: curTrend, ema: curEma, adx: curAdx, reason };
}

// ─────────────────────────────────────────────
// BLACK-SCHOLES OPTION PRICING
// ─────────────────────────────────────────────

function erf(x: number): number {
  const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=0.3275911;
  const sign = x < 0 ? -1 : 1;
  x = Math.abs(x);
  const t = 1 / (1 + p * x);
  const y = 1 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x);
  return sign * y;
}

function normCDF(x: number): number { return 0.5 * (1 + erf(x / Math.sqrt(2))); }

function blackScholes(S: number, K: number, T: number, r: number, sigma: number, type: 'call' | 'put'): number {
  if (T <= 0) return Math.max(0, type === 'call' ? S - K : K - S);
  const d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
  const d2 = d1 - sigma * Math.sqrt(T);
  if (type === 'call') return S * normCDF(d1) - K * Math.exp(-r * T) * normCDF(d2);
  return K * Math.exp(-r * T) * normCDF(-d2) - S * normCDF(-d1);
}

function calcHistoricalVolatility(closes: number[], period = 20, interval = '1d'): number {
  const returns: number[] = [];
  for (let i = 1; i < closes.length; i++) returns.push(Math.log(closes[i] / closes[i - 1]));
  const recent = returns.slice(-period);
  const mean = recent.reduce((a, b) => a + b, 0) / recent.length;
  const variance = recent.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / recent.length;
  // Annualization factor: scale per-bar variance to annual
  const barsPerDay: Record<string, number> = { '1m': 390, '5m': 78, '10m': 39, '15m': 26, '30m': 13, '45m': 9, '1h': 7, '2h': 4, '4h': 2, '1d': 1 };
  const bpd = barsPerDay[interval] ?? 1;
  return Math.sqrt(variance * 252 * bpd);
}

// ─────────────────────────────────────────────
// OPTION PRICE: Alpaca OPRA → Black-Scholes
// ─────────────────────────────────────────────

async function fetchRealOptionPrice(symbol: string, strike: number, expiration: string, optionType: string, interval = '1h', userId?: string, expiryType = 'weekly', alpacaApiKey?: string, alpacaSecretKey?: string): Promise<number> {
  // 1. Try Alpaca options snapshot first (real-time OPRA data with Algo Trader Plus)
  if (alpacaApiKey && alpacaSecretKey) {
    try {
      // Alpaca OCC symbol format: SPY260529C00745000
      const exp = expiration.replace(/-/g, '').slice(2); // YYMMDD
      const strikeStr = String(Math.round(strike * 1000)).padStart(8, '0');
      const typeChar = optionType.toLowerCase() === 'call' ? 'C' : 'P';
      const alpacaSymbol = `${symbol}${exp}${typeChar}${strikeStr}`;
      // Try opra first (Algo Trader Plus), fall back to indicative (free)
      for (const feed of ['opra', 'indicative']) {
        const snapUrl = `https://data.alpaca.markets/v1beta1/options/snapshots?symbols=${encodeURIComponent(alpacaSymbol)}&feed=${feed}`;
        const snapRes = await fetch(snapUrl, {
          headers: { 'APCA-API-KEY-ID': alpacaApiKey, 'APCA-API-SECRET-KEY': alpacaSecretKey }
        });
        const snapJson = await snapRes.json();
        console.log(`[OptionsBot] Alpaca snapshot ${alpacaSymbol} feed=${feed}: status=${snapRes.status} raw=${JSON.stringify(snapJson).slice(0,200)}`);
        if (!snapRes.ok) continue; // try next feed
        const snap = snapJson?.snapshots?.[alpacaSymbol];
        const bid = snap?.latestQuote?.bp;
        const ask = snap?.latestQuote?.ap;
        if (bid > 0 && ask > 0) {
          const mid = (bid + ask) / 2;
          console.log(`[OptionsBot] Alpaca ${feed} price ${alpacaSymbol}: $${mid.toFixed(4)} (bid=$${bid} ask=$${ask})`);
          return mid;
        }
        const lastTrade = snap?.latestTrade?.p;
        if (lastTrade > 0) {
          console.log(`[OptionsBot] Alpaca ${feed} last trade ${alpacaSymbol}: $${lastTrade}`);
          return lastTrade;
        }
      }
    } catch (err) {
      console.log('[OptionsBot] Alpaca options snapshot failed:', err);
    }
  }

  // 2. Try Tastytrade (token only — no option quotes via REST)
  if (userId) {
    try {
      const supabase = createClient(
        Deno.env.get('SUPABASE_URL')!,
        Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
      );
      
      const { data: creds } = await supabase.from('broker_credentials')
        .select('credentials')
        .eq('user_id', userId)
        .eq('broker', 'tastytrade')
        .maybeSingle();
      
      if (creds?.credentials?.refresh_token) {
        // Get fresh access token
        const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${userId}`, {
          headers: { 'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}`, 'apikey': Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '' }
        });
        const tokenJson = await tokenRes.json();
        
        if (tokenJson.access_token) {
          // Note: Tastytrade REST API does not provide option quotes (only DXLink streaming does)
          // We use Tastytrade only for: stock spot prices and order placement
          // Option pricing falls through to Black-Scholes below
          console.log(`[OptionsBot] Tastytrade token valid — using for spot price only (REST has no option quotes)`);
        }
      }
    } catch (err) {
      console.log('[OptionsBot] Tastytrade price fetch failed:', err);
    }
  }
  
  // Black-Scholes with realistic IV — calibrated by VIX + expiry type
  try {
    // Always fetch candles for historical vol calculation
    const candles = await fetchCandles(symbol, interval, 60);
    if (!candles.length) return 0;

    // Use Alpaca live spot price if available (much more accurate than stale Yahoo candle close)
    let spotPrice = candles[candles.length - 1].close;
    if (alpacaApiKey && alpacaSecretKey) {
      try {
        const alpacaSpot = await fetchAlpacaSpotPrice(symbol, alpacaApiKey, alpacaSecretKey);
        if (alpacaSpot && alpacaSpot > 0) {
          console.log(`[OptionsBot] Using Alpaca live spot $${alpacaSpot.toFixed(2)} (Yahoo candle was $${spotPrice.toFixed(2)})`);
          spotPrice = alpacaSpot;
        }
      } catch (_) {}
    }

    // Base IV by symbol type — calibrated to real market observed IVs
    const etfs = ['SPY','QQQ','IWM','DIA','GLD','TLT','XLF','XLE','XLK','XLV','EEM','VXX'];
    const highVol = ['TSLA','NVDA','AMD','MSTR','COIN','PLTR','GME','AMC','RIVN','LCID'];
    let baseIv = etfs.includes(symbol) ? 0.18 : highVol.includes(symbol) ? 0.55 : 0.30;

    // VIX adjustment skipped (Polygon removed)

    // Blend with historical vol from candles (40% weight)
    const closes = candles.map((c: any) => c.close);
    const histVol = calcHistoricalVolatility(closes, 20, interval);
    if (histVol > 0.01 && histVol < 5) {
      baseIv = baseIv * 0.6 + histVol * 0.4;
    }

    // 0DTE IV boost: same-day expiry options carry higher IV due to gamma risk
    let iv = baseIv;
    if (expiryType === '0dte') {
      iv = baseIv * 1.5;  // 0DTE ~1.5x baseline IV (gamma risk)
    } else if (expiryType === '1dte') {
      iv = baseIv * 1.2;  // 1DTE elevated but less than same-day
    } else if (expiryType === 'weekly') {
      iv = baseIv * 1.05; // minimal premium for weekly
    }

    // Use 4PM ET (20:00 UTC) on expiry date as expiry time — not midnight
    const expParts = expiration.split('-');
    const expDate = new Date(Date.UTC(Number(expParts[0]), Number(expParts[1]) - 1, Number(expParts[2]), 20, 0, 0));
    const T = Math.max(1 / (365 * 24 * 60), (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
    const price = blackScholes(spotPrice, strike, T, 0.05, iv, optionType as 'call' | 'put');
    console.log(`[OptionsBot] BS price for ${symbol} ${optionType} $${strike} (IV=${(iv*100).toFixed(0)}% histVol=${(histVol*100).toFixed(0)}% expiryType=${expiryType} T=${(T*365*24).toFixed(1)}h): $${price.toFixed(4)}`);
    return price;
  } catch (_) { return 0; }
}

function getExpirationDate(type: string): string {
  const now = new Date();
  if (type === '0dte') {
    // Find the closest future valid expiration day (today if available, otherwise next available day)
    const target = new Date(now.getTime());
    
    // Search up to 7 days forward for the next valid trading day
    for (let i = 0; i < 7; i++) {
      const candidate = new Date(target.getTime());
      candidate.setDate(candidate.getDate() + i);
      const day = candidate.getDay();
      
      // Skip weekends (0=Sunday, 6=Saturday)
      if (day === 0 || day === 6) continue;
      
      // Return first valid weekday (handles holidays via findValidExpiration later)
      return candidate.toISOString().split('T')[0];
    }
    
    // Fallback to today if no valid day found (shouldn't happen)
    return target.toISOString().split('T')[0];
  } else if (type === '1dte') {
    // Next trading day (skip weekends, assume no holiday check needed — findValidExpiration handles it)
    const target = new Date(now.getTime());
    for (let i = 1; i <= 7; i++) {
      const candidate = new Date(target.getTime());
      candidate.setDate(candidate.getDate() + i);
      const day = candidate.getDay();
      if (day !== 0 && day !== 6) return candidate.toISOString().split('T')[0];
    }
    return new Date(now.getTime() + 86400000).toISOString().split('T')[0];
  } else if (type === 'weekly') {
    // Always pick NEXT Friday for consistent 7+ day holds (minimum 7 days)
    const thisFriday = new Date(now.getTime());
    const daysToThisFriday = (5 - thisFriday.getDay() + 7) % 7;
    thisFriday.setDate(thisFriday.getDate() + daysToThisFriday);
    
    const nextFriday = new Date(thisFriday.getTime());
    nextFriday.setDate(nextFriday.getDate() + 7);
    
    // Always use next Friday (at least 7 days from today)
    return nextFriday.toISOString().split('T')[0];
  } else if (type === 'biweekly') {
    // Biweekly — closest Friday to 14 days from now (could be 13, 14, or 15 days out)
    const target = new Date(now.getTime());
    target.setDate(target.getDate() + 14);
    const dow = target.getDay();
    const fwdDays = (5 - dow + 7) % 7;           // days forward to reach Friday
    const bkDays = dow === 5 ? 0 : (dow - 5 + 7) % 7; // days back to reach Friday
    const closestFri = new Date(target.getTime());
    closestFri.setDate(target.getDate() + (fwdDays <= bkDays ? fwdDays : -bkDays));
    return closestFri.toISOString().split('T')[0];
  } else {
    // Monthly — third Friday closest to 30 days away
    // Find this month's and next month's third Friday
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    let thisFridays = 0, thisThirdFriday: Date | null = null;
    for (let d = 1; d <= 31; d++) {
      const date = new Date(thisMonth.getFullYear(), thisMonth.getMonth(), d);
      if (date.getMonth() !== thisMonth.getMonth()) break;
      if (date.getDay() === 5) {
        thisFridays++;
        if (thisFridays === 3) { thisThirdFriday = date; break; }
      }
    }
    
    const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    let nextFridays = 0, nextThirdFriday: Date | null = null;
    for (let d = 1; d <= 31; d++) {
      const date = new Date(nextMonth.getFullYear(), nextMonth.getMonth(), d);
      if (date.getMonth() !== nextMonth.getMonth()) break;
      if (date.getDay() === 5) {
        nextFridays++;
        if (nextFridays === 3) { nextThirdFriday = date; break; }
      }
    }
    
    // Pick whichever third Friday is closest to 30 days from now
    const daysToThis = thisThirdFriday ? Math.ceil((thisThirdFriday.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)) : Infinity;
    const daysToNext = nextThirdFriday ? Math.ceil((nextThirdFriday.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)) : Infinity;
    
    const diffFrom30This = Math.abs(daysToThis - 30);
    const diffFrom30Next = Math.abs(daysToNext - 30);
    
    const target = diffFrom30This <= diffFrom30Next && daysToThis > 0 ? thisThirdFriday : nextThirdFriday;
    return target ? target.toISOString().split('T')[0] : (thisThirdFriday || nextThirdFriday || now).toISOString().split('T')[0];
  }
}

// Find nearest valid expiration: tries target, then -1 day, then +1 day, then -2 day, then +2 day
function findValidExpiration(targetDate: string): string {
  const target = new Date(targetDate);
  const candidates = [
    target,
    new Date(target.getTime() - 1 * 24 * 60 * 60 * 1000), // -1 day
    new Date(target.getTime() + 1 * 24 * 60 * 60 * 1000), // +1 day
    new Date(target.getTime() - 2 * 24 * 60 * 60 * 1000), // -2 days
    new Date(target.getTime() + 2 * 24 * 60 * 60 * 1000), // +2 days
  ];
  for (const d of candidates) {
    const day = d.getDay();
    if (day !== 0 && day !== 6) return d.toISOString().split('T')[0]; // Skip weekends
  }
  return targetDate; // Fallback to original
}

function pickStrike(spotPrice: number, otmStrikes: number, optionType: 'call' | 'put', strikeInterval = 5): number {
  // Round spot to nearest strike interval
  const atm = Math.round(spotPrice / strikeInterval) * strikeInterval;
  if (optionType === 'call') return atm + otmStrikes * strikeInterval;
  return atm - otmStrikes * strikeInterval;
}

// Smart strike selection: target ~0.30 delta for best risk/reward
function pickSmartStrike(
  spotPrice: number, optionType: 'call' | 'put', T: number, sigma: number,
  strikeInterval: number, budget: number, targetDelta = 0.30
): { strike: number; premium: number; delta: number } {
  const atm = Math.round(spotPrice / strikeInterval) * strikeInterval;
  const R = 0.05;
  
  // Scan strikes from 10 ITM to 10 OTM
  let bestStrike = atm;
  let bestPremium = blackScholes(spotPrice, atm, T, R, sigma, optionType);
  let bestDelta = 0.5; // ATM delta is ~0.5
  let bestDeltaDiff = Math.abs(0.5 - targetDelta);
  
  for (let offset = -10; offset <= 10; offset++) {
    const s = atm + offset * strikeInterval;
    if (s <= 0) continue;
    
    const p = blackScholes(spotPrice, s, T, R, sigma, optionType);
    if (p <= 0.01) continue;
    
    // Approximate delta using Black-Scholes
    const d1 = (Math.log(spotPrice / s) + (R + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    let delta: number;
    if (optionType === 'call') {
      delta = normCDF(d1);
    } else {
      delta = Math.abs(normCDF(d1) - 1); // Put delta as positive number
    }
    
    const deltaDiff = Math.abs(delta - targetDelta);
    const affordable = p * 100 <= budget;
    
    // Pick strike closest to target delta that's within budget
    if (affordable && deltaDiff < bestDeltaDiff) {
      bestStrike = s;
      bestPremium = p;
      bestDelta = delta;
      bestDeltaDiff = deltaDiff;
    }
  }
  
  return { strike: bestStrike, premium: bestPremium, delta: bestDelta };
}

// ─────────────────────────────────────────────
// SETTINGS INTERFACE
// ─────────────────────────────────────────────

interface BotSettings {
  atrLength: number; atrMultiplier: number; emaLength: number;
  adxLength: number; adxThreshold: number; symbol: string;
  dollarAmount: number; interval: string; tradeDirection: string;
  expiryType: string; otmStrikes: number;
  strikeMode: string; manualStrike: number | null;
  takeProfitPct: number; stopLossPct: number;
  symbolRules: Array<{symbol:string;tp:number;sl:number;dir?:string}>;
  marketOpenDelayMin: number;
  botSignal: string;
}

// ─────────────────────────────────────────────
// ALPACA OPTIONS TRADING
// ─────────────────────────────────────────────

// Format option symbol for Alpaca: SPY240531C00580000
function formatOptionSymbol(symbol: string, expirationDate: string, optionType: 'call' | 'put', strike: number): string {
  const date = new Date(expirationDate);
  const year = date.getFullYear().toString().slice(2); // 24
  const month = (date.getMonth() + 1).toString().padStart(2, '0'); // 06
  const day = date.getDate().toString().padStart(2, '0'); // 15
  const type = optionType === 'call' ? 'C' : 'P';
  const strikeStr = Math.round(strike * 1000).toString().padStart(8, '0'); // 00580000
  return `${symbol.toUpperCase()}${year}${month}${day}${type}${strikeStr}`;
}

// Place options order via Tastytrade
async function placeTastytradeOptionOrder(
  supabase: any,
  userId: string,
  symbol: string,
  expirationDate: string,
  optionType: 'call' | 'put',
  strike: number,
  side: 'Buy to Open' | 'Sell to Close',
  qty: number
): Promise<{ success: boolean; orderId?: string; error?: string; status?: string; fillPrice?: number }> {
  try {
    const { data: creds } = await supabase.from('broker_credentials')
      .select('credentials')
      .eq('user_id', userId)
      .eq('broker', 'tastytrade')
      .maybeSingle();

    if (!creds?.credentials?.refresh_token) {
      return { success: false, error: 'No Tastytrade credentials found' };
    }

    // Get fresh access token
    const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${userId}`, {
      headers: { 'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}`, 'apikey': Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '' }
    });
    const tokenJson = await tokenRes.json();
    if (!tokenJson.access_token) return { success: false, error: 'Failed to get access token' };

    const accessToken = tokenJson.access_token;
    const accountNumber = creds.credentials.account_number;
    if (!accountNumber) return { success: false, error: 'No account number found' };

    // Format Tastytrade OCC option symbol: SPY 260523C00590000
    const expParts = expirationDate.split('-');
    const yy = expParts[0].slice(2);
    const mm = expParts[1];
    const dd = expParts[2];
    const typeChar = optionType === 'call' ? 'C' : 'P';
    const strikeStr = String(Math.round(strike * 1000)).padStart(8, '0');
    const occSymbol = `${symbol}  ${yy}${mm}${dd}${typeChar}${strikeStr}`;

    const orderBody = {
      'order-type': 'Market',
      'time-in-force': 'Day',
      legs: [{
        'instrument-type': 'Equity Option',
        symbol: occSymbol,
        quantity: qty,
        action: side,
      }]
    };

    console.log(`[TastyOptions] Placing order: ${side} ${qty}x ${occSymbol} on account ${accountNumber}`);

    const res = await fetch(`https://api.tastytrade.com/accounts/${accountNumber}/orders`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(orderBody),
    });

    const orderJson = await res.json();

    if (!res.ok) {
      const errMsg = orderJson?.error?.message || orderJson?.errors?.[0]?.message || JSON.stringify(orderJson);
      console.error('[TastyOptions] Order failed:', errMsg);
      return { success: false, error: errMsg, status: 'failed' };
    }

    const order = orderJson?.data?.order;
    const orderId = order?.id ? String(order.id) : null;
    const orderStatus = order?.status || 'received';
    console.log(`[TastyOptions] Order placed: id=${orderId} status=${orderStatus}`);

    // Poll up to 10s for fill price
    let fillPrice: number | undefined;
    if (orderId) {
      for (let i = 0; i < 5; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const pollRes = await fetch(`https://api.tastytrade.com/accounts/${accountNumber}/orders/${orderId}`, {
          headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        const polled = await pollRes.json();
        const filledOrder = polled?.data;
        const legs = filledOrder?.legs || [];
        const avgFill = legs[0]?.['average-fill-price'] || filledOrder?.['average-fill-price'];
        console.log(`[TastyOptions] Poll ${i+1}: status=${filledOrder?.status} avg_fill=$${avgFill}`);
        if (avgFill) { fillPrice = Number(avgFill); break; }
        if (filledOrder?.status === 'Filled') { fillPrice = Number(avgFill); break; }
      }
    }

    return { success: true, orderId: orderId || undefined, status: orderStatus, fillPrice };
  } catch (err) {
    console.error('[TastyOptions] Error:', err);
    return { success: false, error: String(err), status: 'error' };
  }
}

// Place options order via Alpaca
async function placeAlpacaOptionOrder(
  supabase: any,
  userId: string,
  symbol: string,
  expirationDate: string,
  optionType: 'call' | 'put',
  strike: number,
  side: 'buy' | 'sell',
  qty: number
): Promise<{ success: boolean; orderId?: string; error?: string; status?: string; fillPrice?: number }> {
  try {
    // Fetch Alpaca credentials
    const { data: creds } = await supabase
      .from('broker_credentials')
      .select('credentials')
      .eq('user_id', userId)
      .eq('broker', 'alpaca')
      .maybeSingle();

    if (!creds) {
      return { success: false, error: 'No Alpaca credentials found' };
    }

    const { api_key, secret_key, env } = creds.credentials;
    const baseUrl = env === 'live'
      ? 'https://api.alpaca.markets'
      : 'https://paper-api.alpaca.markets';

    const optionSymbol = formatOptionSymbol(symbol, expirationDate, optionType, strike);

    const orderBody = {
      symbol: optionSymbol,
      side,
      type: 'market',
      time_in_force: 'day',
      qty: String(qty),
    };

    console.log(`[AlpacaOptions] Placing order: ${side} ${qty} x ${optionSymbol}`);

    const res = await fetch(`${baseUrl}/v2/orders`, {
      method: 'POST',
      headers: {
        'APCA-API-KEY-ID': api_key,
        'APCA-API-SECRET-KEY': secret_key,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(orderBody),
    });

    const order = await res.json();

    if (!res.ok) {
      console.error('[AlpacaOptions] Order failed:', order.message || order);
      return { success: false, error: order.message || 'Alpaca order failed', status: 'failed' };
    }

    console.log(`[AlpacaOptions] Order placed: ${order.id} status=${order.status} filled_avg_price=${order.filled_avg_price}`);

    // If not immediately filled, poll up to 10s for fill price
    let fillPrice: number | undefined = order.filled_avg_price ? Number(order.filled_avg_price) : undefined;
    if (!fillPrice && order.status !== 'filled') {
      for (let i = 0; i < 5; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const pollRes = await fetch(`${baseUrl}/v2/orders/${order.id}`, {
          headers: { 'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': secret_key }
        });
        const polled = await pollRes.json();
        console.log(`[AlpacaOptions] Poll ${i+1}: status=${polled.status} filled_avg_price=${polled.filled_avg_price}`);
        if (polled.filled_avg_price) { fillPrice = Number(polled.filled_avg_price); break; }
        if (polled.status === 'filled') { fillPrice = Number(polled.filled_avg_price); break; }
      }
    }

    return { success: true, orderId: order.id, status: order.status, fillPrice };

  } catch (err) {
    console.error('[AlpacaOptions] Error:', err);
    return { success: false, error: String(err), status: 'error' };
  }
}

// ─────────────────────────────────────────────
// MAIN HANDLER
// ─────────────────────────────────────────────

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });

  const supabase = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
  );

  // GET /portfolio-value?bot_id=xxx — returns cash + live value of open positions
  if (req.method === 'GET') {
    const url = new URL(req.url);
    const botId = url.searchParams.get('bot_id');
    if (!botId) return new Response(JSON.stringify({ error: 'bot_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

    const { data: bot } = await supabase.from('options_bots').select('paper_balance, bot_interval').eq('id', botId).single();
    const cash = Number(bot?.paper_balance ?? 100000);
    const interval = bot?.bot_interval ?? '1h';

    const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', botId).eq('status', 'open');
    let openValue = 0;
    const R = 0.05;
    if (openTrades && openTrades.length > 0) {
      for (const t of openTrades) {
        try {
          const candles = await fetchCandles(t.symbol, interval, 60);
          if (!candles.length) { openValue += Number(t.total_cost); continue; }
          const price = candles[candles.length - 1].close;
          const sigma = calcHistoricalVolatility(candles.map(c => c.close), 20, interval);
          const expDate = new Date(t.expiration_date);
          const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
          const currentPremium = blackScholes(price, t.strike, T, R, sigma, t.option_type);
          openValue += currentPremium * t.contracts * 100;
        } catch (_) { openValue += Number(t.total_cost); }
      }
    }

    return new Response(JSON.stringify({ cash, open_value: openValue, total: cash + openValue }), {
      status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  // Parse body once for all POST handlers
  let _parsedBody: any = null;
  if (req.method === 'POST') {
    _parsedBody = await req.json().catch(() => ({}));
  }

  // ── INSTANT ACTIONS (POST with action field) ──
  if (req.method === 'POST') {
    const body = _parsedBody;
    const action = body.action;

    // Fetch current option price for frontend P&L display
    if (action === 'get_option_price') {
      const { symbol, strike, expiration, option_type, user_id } = body;
      if (!symbol || !strike || !expiration || !option_type) return new Response(JSON.stringify({ price: null }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      let alpacaKey, alpacaSecret;
      if (user_id) {
        const { data: ac } = await supabase.from('broker_credentials').select('credentials').eq('user_id', user_id).eq('broker', 'alpaca').maybeSingle();
        alpacaKey = ac?.credentials?.api_key;
        alpacaSecret = ac?.credentials?.secret_key;
      }
      const price = await fetchRealOptionPrice(symbol, Number(strike), expiration, option_type, '1h', user_id, 'weekly', alpacaKey, alpacaSecret);
      return new Response(JSON.stringify({ price }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Instant TP/SL check: called immediately when user saves new thresholds
    if (action === 'check_tpsl') {
      const botId = body.bot_id;
      if (!botId) return new Response(JSON.stringify({ error: 'bot_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: bot } = await supabase.from('options_bots').select('*').eq('id', botId).single();
      if (!bot) return new Response(JSON.stringify({ error: 'Bot not found' }), { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const takeProfitPct = Number(body.take_profit_pct ?? bot.take_profit_pct ?? 100);
      const stopLossPct   = Number(body.stop_loss_pct  ?? bot.stop_loss_pct  ?? 20);
      const interval      = bot.bot_interval ?? '1h';
      const R = 0.05;
      const closed: object[] = [];

      const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', botId).eq('status', 'open');
      for (const open of (openTrades || [])) {
        try {
          const { data: alpacaCredsForTpsl } = await supabase.from('broker_credentials').select('credentials').eq('user_id', bot.user_id).eq('broker', 'alpaca').maybeSingle();
          const tpslExpiryType = bot.bot_expiry_type ?? 'weekly';
          let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, bot.user_id, tpslExpiryType, alpacaCredsForTpsl?.credentials?.api_key, alpacaCredsForTpsl?.credentials?.secret_key);
          if (!optionPrice || optionPrice <= 0) {
            console.log(`[check_tpsl] SKIP: no real price for ${open.symbol} $${open.strike}`);
            continue;
          }
          const totalCost = Number(open.total_cost) || (Number(open.premium_per_contract) * open.contracts * 100);
          const currentValue = optionPrice * open.contracts * 100;
          const pnl = currentValue - totalCost;
          const pctChange = (pnl / totalCost) * 100;
          const slThreshold = stopLossPct < 0 ? stopLossPct : -Math.abs(stopLossPct);
          const shouldTP = pctChange >= takeProfitPct;
          const shouldSL = pctChange <= slThreshold;
          if (shouldTP || shouldSL) {
            await supabase.from('options_trades').update({ status: 'closed', exit_price: optionPrice, pnl, closed_at: new Date().toISOString() }).eq('id', open.id);
            if (bot.broker === 'paper') {
              const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', botId).single();
              const bal = Number(bRow?.paper_balance ?? 100000);
              await supabase.from('options_bots').update({ paper_balance: bal + totalCost + pnl }).eq('id', botId);
            }
            closed.push({ id: open.id, symbol: open.symbol, pct_change: pctChange.toFixed(1) + '%', pnl: pnl.toFixed(2), reason: shouldTP ? 'take_profit' : 'stop_loss' });
          }
        } catch (_) {}
      }
      return new Response(JSON.stringify({ checked: (openTrades || []).length, closed }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Fast TP/SL Daemon: checks ALL open positions every 30 seconds for instant exit
    if (action === 'tpsl_daemon') {
      const now = new Date();
      
      // Get all open trades with their bot settings
      const { data: openTrades } = await supabase.from('options_trades')
        .select('*, options_bots!inner(take_profit_pct, stop_loss_pct, symbol_rules, bot_interval, broker, user_id, name, bot_expiry_type)')
        .eq('status', 'open');
      
      if (!openTrades || openTrades.length === 0) {
        return new Response(JSON.stringify({ checked: 0, closed: [], message: 'No open positions' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      console.log(`[TPSL_Daemon] Checking ${openTrades.length} open positions for TP/SL...`);
      const closed: object[] = [];
      const R = 0.05;
      // Cache Alpaca creds per user to avoid repeated DB queries
      const alpacaCredsCache: Record<string, { api_key?: string; secret_key?: string }> = {};

      for (const open of openTrades) {
        try {
          const bot = (open as any).options_bots;
          const symRulesDaemon: Array<{symbol:string;tp:number;sl:number}> = (bot?.symbol_rules as any) || [];
          const symRuleDaemon = symRulesDaemon.find((r:any) => r.symbol?.toUpperCase() === (open as any).symbol?.toUpperCase());
          const takeProfitPct  = symRuleDaemon ? Number(symRuleDaemon.tp) : Number(bot?.take_profit_pct ?? 35);
          const stopLossPct    = symRuleDaemon ? Number(symRuleDaemon.sl) : Number(bot?.stop_loss_pct ?? -25);
          const interval = bot?.bot_interval ?? '1h';
          const userId = bot?.user_id;
          
          // Fetch Alpaca creds once per user (cached)
          if (userId && !alpacaCredsCache[userId]) {
            const { data: alpacaCredsRow } = await supabase.from('broker_credentials').select('credentials').eq('user_id', userId).eq('broker', 'alpaca').maybeSingle();
            alpacaCredsCache[userId] = alpacaCredsRow?.credentials ?? {};
          }
          const alpacaApiKey = alpacaCredsCache[userId]?.api_key;
          const alpacaSecretKey = alpacaCredsCache[userId]?.secret_key;
          const botExpiryType = bot?.bot_expiry_type ?? 'weekly';
          // Fetch real-time price — Alpaca OPRA → Black-Scholes
          const optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, userId, botExpiryType, alpacaApiKey, alpacaSecretKey);
          const source = optionPrice > 0 ? 'alpaca/bs' : 'none';
          
          if (!optionPrice || optionPrice <= 0) {
            console.log(`[TPSL_Daemon] SKIP: no real price for ${open.symbol} $${open.strike}`);
            continue;
          }

          const totalCost = Number(open.total_cost) || (Number(open.premium_per_contract) * open.contracts * 100);
          const currentValue = optionPrice * open.contracts * 100;
          const pnl = currentValue - totalCost;
          const pctChange = (pnl / totalCost) * 100;
          const slThreshold = stopLossPct < 0 ? stopLossPct : -Math.abs(stopLossPct);
          const shouldTP = pctChange >= takeProfitPct;
          const shouldSL = pctChange <= slThreshold;

          // EOD auto-close: force-close all 0DTE positions at 12:00 PM MST (18:00 UTC)
          const utcHour = now.getUTCHours();
          const utcMinute = now.getUTCMinutes();
          const is0dte = botExpiryType === '0dte' || open.expiration_date === new Date().toISOString().slice(0, 10);
          const shouldEOD = is0dte && (utcHour > 18 || (utcHour === 18 && utcMinute >= 0));
          
          console.log(`[TPSL_Daemon] ${open.symbol} ${open.option_type} $${open.strike}: current=$${optionPrice.toFixed(2)} entry=$${Number(open.premium_per_contract).toFixed(2)} pct=${pctChange.toFixed(1)}% tp=${takeProfitPct}% sl=${slThreshold}% shouldTP=${shouldTP} shouldSL=${shouldSL} shouldEOD=${shouldEOD} source=${source}`);
          
          if (shouldTP || shouldSL || shouldEOD) {
            const exitReason = shouldEOD ? 'eod_close_noon_mst' : shouldTP ? 'take_profit' : 'stop_loss';
            await supabase.from('options_trades').update({ 
              status: 'closed', 
              exit_price: optionPrice, 
              pnl, 
              closed_at: now.toISOString(),
              exit_reason: exitReason
            }).eq('id', open.id);
            
            // Update paper balance if paper trading
            if (bot?.broker === 'paper') {
              const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', open.bot_id).single();
              const bal = Number(bRow?.paper_balance ?? 100000);
              await supabase.from('options_bots').update({ paper_balance: bal + totalCost + pnl }).eq('id', open.bot_id);
            }
            
            closed.push({ 
              id: open.id, 
              bot_name: bot?.name || 'Unknown',
              symbol: open.symbol, 
              strike: open.strike,
              pct_change: pctChange.toFixed(1) + '%', 
              pnl: pnl.toFixed(2), 
              reason: exitReason,
              source
            });
          }
        } catch (err) {
          console.log(`[TPSL_Daemon] Error checking trade ${open.id}:`, err);
        }
      }
      
      console.log(`[TPSL_Daemon] Closed ${closed.length} positions`);
      return new Response(JSON.stringify({ checked: openTrades.length, closed }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Instant manual close: called when user clicks "Close Now" on a specific trade
    if (action === 'close_trade') {
      const tradeId = body.trade_id;
      if (!tradeId) return new Response(JSON.stringify({ error: 'trade_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: open } = await supabase.from('options_trades').select('*').eq('id', tradeId).single();
      if (!open) return new Response(JSON.stringify({ error: 'Trade not found' }), { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: bot } = await supabase.from('options_bots').select('*').eq('id', open.bot_id).single();
      const interval = bot?.bot_interval ?? '1h';
      const R = 0.05;

      let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, bot?.user_id);
      if (!optionPrice || optionPrice <= 0) {
        const candles = await fetchCandles(open.symbol, interval, 60);
        if (candles.length) {
          const spotPrice = candles[candles.length - 1].close;
          const sigma = calcHistoricalVolatility(candles.map((c: any) => c.close), 20, interval);
          const expDate = new Date(open.expiration_date);
          const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
          optionPrice = blackScholes(spotPrice, open.strike, T, R, sigma, open.option_type);
        }
      }
      if (!optionPrice || optionPrice <= 0) optionPrice = Number(open.premium_per_contract);
      // Sanity clamp: exit price can never be negative or produce loss > 100% of entry
      const _entryPremium1 = Number(open.premium_per_contract);
      optionPrice = Math.max(0, Math.min(optionPrice, _entryPremium1 * 10));

      const pnl = Math.max(-(Number(open.total_cost) || _entryPremium1 * open.contracts * 100), (optionPrice - _entryPremium1) * open.contracts * 100);
      await supabase.from('options_trades').update({ status: 'closed', exit_price: optionPrice, pnl, closed_at: new Date().toISOString() }).eq('id', tradeId);

      if (bot && bot.broker === 'paper') {
        const bal = Number(bot.paper_balance ?? 100000);
        await supabase.from('options_bots').update({ paper_balance: bal + Number(open.total_cost) + pnl }).eq('id', open.bot_id);
      }
      return new Response(JSON.stringify({ success: true, symbol: open.symbol, exit_price: optionPrice, pnl: pnl.toFixed(2) }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

  }

  try {
    let targetBotId: string | null = null;
    let targetUserId: string | null = null;
    let forceRun = false;

    // INDEPENDENT MODE: Options bot runs on its own schedule via cron
    // No sync trigger from stock bot - generates its own signals
    if (req.method === 'POST') {
      const authHeader = req.headers.get('Authorization');
      const body = _parsedBody || {};
      const cronSecret = body.cron_secret;
      const validCron  = cronSecret === Deno.env.get('CRON_SECRET');
      if (!validCron && authHeader) {
        const token = authHeader.replace('Bearer ', '');
        const { data: { user } } = await supabase.auth.getUser(token);
        if (user) targetUserId = user.id;
      }
      targetBotId = body.bot_id || null;
      targetUserId = targetUserId || body.user_id || null;
      forceRun = body.force === true;
    }

    let query = supabase.from('options_bots').select('*');
    if (!forceRun) query = query.eq('enabled', true).eq('auto_submit', true);
    if (targetBotId)  query = query.eq('id', targetBotId);
    if (targetUserId) query = query.eq('user_id', targetUserId);

    console.log(`[OptionsBot] Query: targetBotId=${targetBotId}, targetUserId=${targetUserId}, independent_mode=true`);

    const { data: bots, error: botErr } = await query;
    
    if (botErr) {
      console.error('[OptionsBot] Query error:', botErr);
    }
    console.log(`[OptionsBot] Found ${bots?.length || 0} bots`);
    if (bots && bots.length > 0) {
      console.log('[OptionsBot] Bot names:', bots.map(b => b.name).join(', '));
    }
    if (botErr) throw botErr;
    if (!bots || bots.length === 0) {
      return new Response(JSON.stringify({ message: 'No active options bots', debug: { targetBotId, targetUserId } }), {
        status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    console.log(`Found ${bots.length} active bots:`, bots.map(b => ({ id: b.id, name: b.name, user_id: b.user_id?.slice(0,8), symbol: b.bot_symbol, scan_mode: b.bot_scan_mode })));

    const results: object[] = [];
    const R = 0.05; // risk-free rate
    const now = new Date();
    
    // Check market hours (options on stocks only trade 9:30 AM - 4:00 PM ET)
    // Use UTC offset for ET (UTC-5 or UTC-4 depending on DST)
    const utcHour = now.getUTCHours();
    const utcMinute = now.getUTCMinutes();
    const utcDay = now.getUTCDay();
    
    // Convert UTC to ET using proper timezone (handles DST automatically)
    const etNowStr = now.toLocaleString('en-US', { timeZone: 'America/New_York' });
    const etDate = new Date(etNowStr);
    let etHour = etDate.getHours();
    let etMinute = etDate.getMinutes();
    let etDay = etDate.getDay();
    
    const isWeekday = etDay >= 1 && etDay <= 5;
    const isOptionsMarketHours = isWeekday && (etHour > 9 || (etHour === 9 && etMinute >= 30)) && etHour < 16;
    
    // Wait 3 minutes after market open to avoid opening volatility
    const isAfter930Buffer = etHour > 9 || (etHour === 9 && etMinute >= 33);
    
    console.log(`[OptionsBot] Market hours check: ET=${etHour}:${etMinute}, day=${etDay}, weekday=${isWeekday}, open=${isOptionsMarketHours}, after930buffer=${isAfter930Buffer}`);

    const SCAN_STOCKS = [
      'AAPL','MSFT','AMZN','NVDA','TSLA','GOOG','GOOGL','META','NFLX','BRK-B',
      'JPM','BAC','WFC','V','MA','PG','KO','PFE','UNH','HD',
      'INTC','CSCO','ADBE','CRM','ORCL','AMD','QCOM','TXN','IBM','AVGO',
      'XOM','CVX','BA','CAT','MMM','GE','HON','LMT','NOC','DE',
      'C','GS','MS','AXP','BLK','SCHW','BK','SPGI','ICE',
      'MRK','ABBV','AMGN','BMY','LLY','GILD','JNJ','REGN','VRTX','BIIB',
      'WMT','COST','TGT','LOW','MCD','SBUX','NKE','BKNG',
      'SNAP','UBER','LYFT','SPOT','ZM','DOCU','PINS','ROKU','SHOP',
      'CVS','TMO','MDT','ISRG','F','GM',
      // High volatility growth stocks (great for options)
      'SNOW','CRWD','NET','DDOG','MDB','OKTA','SPLK','FSLR','ENPH','SEDG',
      'DKNG','CHPT','LCID','RIVN','HOOD','SOFI','AI','PLTR','ASML','MU',
      'LRCX','KLAC','AMAT','MRVL','NXPI','CDNS','SNPS','ANET','FTNT','PANW',
      'GME','AMC','BBBY','EXPR','KOSS','NAKD','SNDL','TLRY','ACB','CGC',
      // ETFs (high volume options)
      'QQQ','SPY','VOO','IVV','VTI','VUG','QQQM','SCHG','XLK','VGT','SMH','TQQQ',
    ];

    const SCAN_ETFS = [
      'QQQ','SPY','VOO','IVV','VTI','VUG','QQQM','SCHG','XLK','VGT','SMH','TQQQ',
    ];

    const SCAN_TOP10 = [
      'SMCI','TSLA','NVDA','COIN','PLTR','AMD','MRNA','MSTY','ENPH','VKTX','CCL',
    ];

    const SCAN_BOOF = [
      'QQQ','SPY','TSLA','NVDA','AMD',
    ];

    const SCAN_DUO = [
      'SPY','QQQ',
    ];

    const SCAN_CRYPTO = [
      'BTC/USD','ETH/USD','SOL/USD','AVAX/USD','LINK/USD',
      'UNI/USD','AAVE/USD','CRV/USD','LDO/USD','MATIC/USD',
    ];

    const SCAN_TOP50 = [
      'SNGX','HTCO','ERAS','BIYA','ACST','ACB','AIXI','AMST','EOSE','JBLU',
      'LAES','SLS','BE','CIFR','RDW','IREN','BRLS','EDSA','KNSA','OMCL',
      'CVLT','CNC','HRI','NVTS','CLS','RBLX','PLTR','TSLA','NVDA','AMD',
      'META','NFLX','AMZN','SMCI','NVR','AZO','MELI','GEV','MPWR','CAR',
      'SPY','QQQ','AAPL','MSFT','GOOGL','AVGO','INTC','PYPL','SNAP','UBER',
    ];
    for (const bot of bots) {
      // INDEPENDENT MODE: Options bot runs on its own schedule, scans symbols like stock bot
      console.log(`[OptionsBot] Running bot "${bot.name}" independently`);
      
      // Derive run interval from trading style (bot_interval) — no separate UI setting needed
      const intervalToMinutes: Record<string, number> = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '4h': 240 };
      const botInterval = (bot.bot_interval as string) || '5m';
      const runIntervalMin = (bot.run_interval_min as number) ?? intervalToMinutes[botInterval] ?? 5;
      const lastRunAt = bot.last_run_at ? new Date(bot.last_run_at as string) : null;
      const minutesSinceLastRun = lastRunAt ? (now.getTime() - lastRunAt.getTime()) / (1000 * 60) : Infinity;
      
      if (!forceRun && minutesSinceLastRun < runIntervalMin) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - ran ${minutesSinceLastRun.toFixed(1)}m ago, interval=${runIntervalMin}m`);
        continue;
      }
      
      // Options only trade during market hours + delay buffer after 9:30 open
      const delayMin = (bot.market_open_delay_min as number) ?? 0;
      const isAfterOpenBuffer = etHour > 9 || (etHour === 9 && etMinute >= (30 + delayMin));
      if (!forceRun && (!isOptionsMarketHours || !isAfterOpenBuffer)) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - markets closed or within open delay (ET=${etHour}:${etMinute}, delayMin=${delayMin})`);
        results.push({ bot_id: bot.id, symbol: bot.bot_symbol, status: 'skipped', reason: `Markets closed (ET=${etHour}:${etMinute})` });
        continue;
      }
      const expiryType = bot.bot_expiry_type ?? 'weekly';
      const isAfter2PM_ET = etHour >= 14; // 2:00 PM ET or later
      if (!forceRun && expiryType === '0dte' && isAfter2PM_ET) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - 0DTE cutoff reached (after 2:00 PM ET, 2 hours before close)`);
        continue;
      }
      
      console.log(`[OptionsBot] Running "${bot.name}" | interval=${runIntervalMin}m | expiry=${expiryType}`);
      const settings: BotSettings = {
        atrLength:      bot.bot_atr_length     ?? 10,
        atrMultiplier:  bot.bot_atr_multiplier ?? 3.0,
        emaLength:      bot.bot_ema_length     ?? 50,
        adxLength:      bot.bot_adx_length     ?? 14,
        adxThreshold:   bot.bot_adx_threshold  ?? 10,
        symbol:         bot.bot_symbol         ?? 'SPY',
        dollarAmount:   bot.bot_dollar_amount  ?? 500,
        interval:       bot.bot_interval       ?? '1h',
        tradeDirection: bot.bot_trade_direction ?? 'both',
        expiryType:     bot.bot_expiry_type    ?? 'weekly',
        otmStrikes:     bot.bot_otm_strikes    ?? 1,
        strikeMode:     bot.bot_strike_mode    ?? 'budget',
        manualStrike:   bot.bot_manual_strike  ?? null,
        takeProfitPct:   bot.take_profit_pct    ?? 40,
        stopLossPct:     bot.stop_loss_pct      ?? 20,
        symbolRules:     (bot.symbol_rules as any) || [],
        marketOpenDelayMin: bot.market_open_delay_min ?? 0,
        botSignal:      (bot.bot_signal as string) || 'supertrend',
      };

      const scanMode: string = (bot.bot_scan_mode as string) || 'single';
      
      // INDEPENDENT MODE: Build symbol list from bot's scan mode
      // Single mode supports CSV: "SPY, QQQ, NVDA" → scans all three
      const singleSymbols = (settings.symbol as string).split(',').map((s:string) => s.trim().toUpperCase()).filter(Boolean);
      const symbolList: string[] = scanMode === 'scan_stocks' ? SCAN_STOCKS
        : scanMode === 'scan_etfs' ? SCAN_ETFS
        : scanMode === 'scan_top10' ? SCAN_TOP10
        : scanMode === 'scan_top50' ? SCAN_TOP50
        : scanMode === 'scan_boof' ? SCAN_BOOF
        : scanMode === 'scan_duo' ? SCAN_DUO
        : scanMode === 'scan_crypto' ? SCAN_CRYPTO
        : singleSymbols;

      console.log(`[OptionsBot] "${bot.name}" | scanMode=${scanMode} | symbols=${symbolList.length} | list=[${symbolList.slice(0,5).join(',')}...${symbolList.slice(-3).join(',')}]`);

      try {
        // Fetch Alpaca creds once per bot run (used for both TP/SL and new trade pricing)
        const alpacaCreds = await supabase.from('broker_credentials').select('credentials').eq('user_id', bot.user_id).eq('broker', 'alpaca').maybeSingle().then((r: any) => r.data?.credentials);

        // ── TP/SL check on all open positions using REAL option prices ──
        const { data: allOpen } = await supabase.from('options_trades').select('*').eq('bot_id', bot.id).eq('status', 'open');
        if (allOpen && allOpen.length > 0) {
          for (const open of allOpen) {
            try {
              // Minimum hold time (paper trading only): skip TP/SL for trades less than 2 minutes old
              // Paper uses Black-Scholes which can misfire immediately — live trades use real prices so no hold needed
              if (bot.broker === 'paper') {
                const tradeAgeMs = Date.now() - new Date(open.created_at).getTime();
                const minHoldMs = 2 * 60 * 1000; // 2 minutes
                if (tradeAgeMs < minHoldMs) {
                  console.log(`[OptionsBot] SKIP TP/SL for ${open.symbol} — paper trade only ${Math.round(tradeAgeMs/1000)}s old, min hold=${minHoldMs/1000}s`);
                  continue;
                }
              }

              // Build Tradier option symbol format: SPY241231C00580000
              const expDate = new Date(open.expiration_date);
              const yy = String(expDate.getFullYear()).slice(-2);
              const mm = String(expDate.getMonth() + 1).padStart(2, '0');
              const dd = String(expDate.getDate()).padStart(2, '0');
              const strikeCents = Math.round(open.strike * 1000);
              const optSymbol = `${open.symbol}${yy}${mm}${dd}${open.option_type.toUpperCase().charAt(0)}${String(strikeCents).padStart(8, '0')}`;
              
              // Fetch REAL option price — Alpaca OPRA → Black-Scholes
              // Try Alpaca first to see if we get a real quote
              let optionPrice = 0;
              let source = 'none';
              if (alpacaCreds?.api_key) {
                optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, settings.interval, bot.user_id, settings.expiryType, alpacaCreds.api_key, alpacaCreds.secret_key);
                source = optionPrice > 0 ? 'alpaca_real' : 'alpaca_miss';
              }
              // Fall back to Black-Scholes if Alpaca returned nothing
              if (!optionPrice || optionPrice <= 0) {
                optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, settings.interval, bot.user_id, settings.expiryType, undefined, undefined);
                source = optionPrice > 0 ? 'black_scholes' : 'none';
              }

              if (!optionPrice || optionPrice <= 0) {
                console.log(`[OptionsBot] SKIP TP/SL for ${open.symbol} $${open.strike} — no real price available, will retry next cycle`);
                continue;
              }

              const totalCost = Number(open.total_cost) || (Number(open.premium_per_contract) * open.contracts * 100);
              const currentValue = optionPrice * open.contracts * 100;
              const pnlNow = currentValue - totalCost;
              const pctChange = (pnlNow / totalCost) * 100;
              const entryPremium = Number(open.premium_per_contract);

              // Sanity check: block if showing worse than -95% (likely bad price data)
              if (pctChange < -95) {
                console.log(`[OptionsBot] SKIP TP/SL for ${open.symbol} $${open.strike} — pct ${pctChange.toFixed(1)}% looks wrong, skipping`);
                continue;
              }
              // Sanity check: when using Black-Scholes (no Alpaca real quote),
              // cross-check against delta-estimated P&L using actual spot movement.
              // If BS shows >2x what delta math predicts, replace with delta estimate.
              if (source === 'black_scholes' && open.entry_spot) {
                try {
                  const tpslCandles = await fetchCandles(open.symbol, settings.interval, 5);
                  const spotNow = tpslCandles.length > 0 ? tpslCandles[tpslCandles.length - 1].close : 0;
                  if (spotNow > 0) {
                    const spotMove = spotNow - Number(open.entry_spot);
                    const delta = open.option_type === 'call' ? 0.5 : -0.5;
                    const deltaEstPnl = delta * spotMove * open.contracts * 100;
                    const deltaEstPct = (deltaEstPnl / totalCost) * 100;
                    // If BS pctChange is more than 2x worse than delta estimate, it's wrong
                    if (pctChange < -5 && deltaEstPct > pctChange * 2) {
                      console.log(`[OptionsBot] BS price override for ${open.symbol}: BS=${pctChange.toFixed(1)}% but delta estimate=${deltaEstPct.toFixed(1)}% (spot moved $${spotMove.toFixed(2)}). Using delta estimate.`);
                      const correctedPnl = deltaEstPnl;
                      const correctedPct = deltaEstPct;
                      // Re-evaluate TP/SL with corrected values
                      const slThreshold2 = settings.stopLossPct < 0 ? settings.stopLossPct : -Math.abs(settings.stopLossPct);
                      if (correctedPct < slThreshold2 || correctedPct >= settings.takeProfitPct) {
                        // Corrected value still triggers — allow with corrected P&L
                        const exitPrice2 = entryPremium + (delta * spotMove);
                        await supabase.from('options_trades').update({ status: 'closed', exit_price: Math.max(0, exitPrice2), pnl: correctedPnl, closed_at: new Date().toISOString() }).eq('id', open.id);
                        if (bot.broker === 'paper') {
                          const bal2 = Number((await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single()).data?.paper_balance ?? 100000);
                          await supabase.from('options_bots').update({ paper_balance: bal2 + totalCost + correctedPnl }).eq('id', bot.id);
                        }
                        results.push({ bot_id: bot.id, symbol: open.symbol, status: 'closed', pnl: correctedPnl.toFixed(2), reason: `TP/SL triggered (delta-corrected): ${correctedPct.toFixed(1)}%` });
                      } else {
                        console.log(`[OptionsBot] After delta correction, ${open.symbol} pct=${correctedPct.toFixed(1)}% — no TP/SL trigger`);
                      }
                      continue;
                    }
                  }
                } catch (_) {}
              }

              const symRuleMain = settings.symbolRules?.find(r => r.symbol?.toUpperCase() === (open as any).symbol?.toUpperCase());
              const effectiveTP = symRuleMain ? Number(symRuleMain.tp) : settings.takeProfitPct;
              const effectiveSL = symRuleMain ? Number(symRuleMain.sl) : settings.stopLossPct;
              const slThreshold = effectiveSL < 0 ? effectiveSL : -Math.abs(effectiveSL);
              const shouldTP = pctChange >= effectiveTP;
              const shouldSL = pctChange <= slThreshold;
              console.log(`[OptionsBot] TP/SL ${open.symbol} ${open.option_type} $${open.strike}: current=$${optionPrice.toFixed(2)} entry=$${Number(open.premium_per_contract).toFixed(2)} pct=${pctChange.toFixed(1)}% tp=${settings.takeProfitPct}% sl=${slThreshold}% shouldTP=${shouldTP} shouldSL=${shouldSL} source=${source}`);
              
              // EOD exit: 0DTE options — force close all positions at 2:00 PM ET
              // Compute ET date correctly: ET = UTC - 4 hours (DST) or -5 hours (standard)
              // Since March 10 - Nov 3 is DST, during most trading hours we use -4
              const isDST = etDate.getHours() !== now.getUTCHours(); // Simple DST check
              const etOffsetMs = (isDST ? 4 : 5) * 60 * 60 * 1000;
              const etAdjustedDate = new Date(now.getTime() - etOffsetMs);
              const etDateStr = etAdjustedDate.toISOString().split('T')[0];
              const is0DTE = open.expiration_date === etDateStr;
              const shouldEOD = is0DTE && isAfter2PM_ET;
              console.log(`[OptionsBot] EOD Check ${open.symbol} ${open.option_type} $${open.strike}: exp=${open.expiration_date} etDate=${etDateStr} is0DTE=${is0DTE} isAfter2PM=${isAfter2PM_ET} shouldEOD=${shouldEOD}`);
              
              if (shouldTP || shouldSL || shouldEOD) {
                const pnl = pnlNow;
                let closeStatus = 'closed';
                let closeOrderId = null;
                let closeError = null;

                // Close live position
                if (bot.broker === 'tastytrade') {
                  console.log(`[OptionsBot] Closing Tastytrade position: ${open.contracts} contracts of ${open.symbol} ${open.option_type}`);
                  const tastyResult = await placeTastytradeOptionOrder(
                    supabase, bot.user_id, open.symbol, open.expiration_date,
                    open.option_type, open.strike, 'Sell to Close', open.contracts
                  );
                  if (tastyResult.success) {
                    closeStatus = 'closed';
                    closeOrderId = tastyResult.orderId;
                    if (tastyResult.fillPrice && tastyResult.fillPrice > 0) {
                      optionPrice = tastyResult.fillPrice; // use real exit price for P&L
                      console.log(`[OptionsBot] Tastytrade real exit price: $${optionPrice.toFixed(2)}/contract`);
                    }
                  } else {
                    closeError = tastyResult.error;
                    console.error(`[OptionsBot] Tastytrade close failed: ${closeError}`);
                  }
                } else if (bot.broker === 'alpaca' && open.order_id) {
                  console.log(`[OptionsBot] Closing Alpaca position: ${open.contracts} contracts of ${open.symbol} ${open.option_type}`);
                  const alpacaResult = await placeAlpacaOptionOrder(
                    supabase, bot.user_id, open.symbol, open.expiration_date,
                    open.option_type, open.strike, 'sell', open.contracts
                  );
                  if (alpacaResult.success) {
                    closeStatus = alpacaResult.status === 'filled' ? 'closed' : 'closing';
                    closeOrderId = alpacaResult.orderId;
                    if (alpacaResult.fillPrice && alpacaResult.fillPrice > 0) {
                      optionPrice = alpacaResult.fillPrice;
                    }
                  } else {
                    closeError = alpacaResult.error;
                    console.error(`[OptionsBot] Alpaca close failed: ${closeError}`);
                  }
                } else {
                  // Paper trading: update virtual balance
                  const { data: botRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                  const bal = Number(botRow?.paper_balance ?? 100000);
                  await supabase.from('options_bots').update({ paper_balance: bal + (open.total_cost + pnl) }).eq('id', bot.id);
                }

                await supabase.from('options_trades').update({ 
                  status: closeStatus, 
                  exit_price: optionPrice, 
                  pnl, 
                  close_order_id: closeOrderId,
                  broker_error: closeError,
                  closed_at: new Date().toISOString() 
                }).eq('id', open.id);
                
                const exitReason = shouldEOD ? 'eod_exit' : shouldTP ? 'take_profit' : 'stop_loss';
                results.push({ bot_id: bot.id, symbol: open.symbol, status: exitReason, pct_change: pctChange.toFixed(1) + '%', pnl: pnl.toFixed(2), order_id: closeOrderId, broker_error: closeError });
              }
            } catch (_) {}
          }
        }

        const tradedThisRun = new Set<string>();
        for (const sym of symbolList) {
          try {
              const candles = await fetchCandles(sym, settings.interval, 150, alpacaCreds?.api_key, alpacaCreds?.secret_key);
              if (candles.length < 60) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Not enough candle data' }); continue; }

              // INDEPENDENT MODE: Always generate our own signal based on bot_signal setting
              let signal: 'buy' | 'sell' | 'none';
              let price: number;
              let reason: string;
              
              const botSignal = settings.botSignal || 'supertrend';
              let sigResult: { signal: 'buy' | 'sell' | 'none', price: number, reason: string, trend?: number, ema?: number, adx?: number };
              if (botSignal === 'rsi_macd') {
                sigResult = generateSignalRSIMACD(candles, settings.tradeDirection);
              } else if (botSignal === 'boof20') {
                // Tightened for 1m scalping: ~3 trades/hour target (0.8% threshold vs 0.3% default)
                sigResult = generateSignalBoof20(candles, settings.tradeDirection, 0.008, -0.008);
              } else if (botSignal === 'boof30') {
                sigResult = generateSignalBoof30(candles, settings.tradeDirection);
              } else if (botSignal === 'boof50') {
                // Boof 5.0: Six-Factor Quant Model with optional trend filter
                const boof50TrendFilter = bot.trend_filter as string || 'none';
                const tfCandles = boof50TrendFilter !== 'none' ? await fetchCandles(sym, boof50TrendFilter, 100) : undefined;
                sigResult = generateSignalBoof50(candles, settings.tradeDirection, tfCandles);
              } else if (botSignal === 'boof60') {
                // Boof 6.0: Multi-Timeframe Scalping System
                // Fetches 1h (trend lock), 15m (EMA confirm), 1m (VWAP) in parallel
                const [b60_1h, b60_15m, b60_1m] = await Promise.all([
                  fetchCandles(sym, '1h', 50).catch(() => [] as Candle[]),
                  fetchCandles(sym, '15m', 50).catch(() => [] as Candle[]),
                  fetchCandles(sym, '1m', 390).catch(() => [] as Candle[]),
                ]);
                sigResult = generateSignalBoof60(candles, b60_1h, b60_15m, b60_1m, settings.tradeDirection);
              } else if (botSignal === 'longer_swing') {
                // For 30m swing, fetch more candles and use Boof 3.0 for regime detection
                const swingCandles = await fetchCandles(sym, '30m', 200);
                if (swingCandles.length < 60) {
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Not enough 30m candle data' });
                  continue;
                }
                sigResult = generateSignalBoof30(swingCandles, settings.tradeDirection);
              } else if (botSignal === 'boof70') {
                const { data: recentTrades70 } = await supabase.from('options_trades')
                  .select('pnl').eq('bot_id', bot.id as string).eq('symbol', sym)
                  .not('pnl', 'is', null).order('closed_at', { ascending: false }).limit(20);
                const pnls70 = (recentTrades70 || []).map((t: any) => Number(t.pnl));
                const wins70 = pnls70.filter((p: number) => p > 0).length;
                const recentWinRate70 = pnls70.length > 0 ? wins70 / pnls70.length : 0.5;
                let consecutiveLosses70 = 0;
                for (const p of pnls70) { if (p <= 0) consecutiveLosses70++; else break; }
                const isCryptoSym70 = sym.includes('-USD') || sym.includes('/USD');
                // Kill-switch: 7+ consecutive losses → skip
                if (consecutiveLosses70 >= 7) {
                  console.log(`[Boof7.0][OptionsBot] Kill-switch: ${consecutiveLosses70} consecutive losses — paused`);
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Boof 7.0 kill-switch: ${consecutiveLosses70} consecutive losses` });
                  continue;
                }
                // Adaptive position sizing
                let sizePct70 = 1.0;
                if (recentWinRate70 >= 0.60) sizePct70 = 1.25;
                else if (recentWinRate70 < 0.40) sizePct70 = 0.60;
                if (consecutiveLosses70 >= 5) sizePct70 *= 0.25;
                else if (consecutiveLosses70 >= 3) sizePct70 *= 0.50;
                sizePct70 = Math.max(0.10, Math.min(1.50, sizePct70));
                if (sizePct70 !== 1.0) {
                  const orig = settings.dollarAmount;
                  settings.dollarAmount = Math.round(settings.dollarAmount * sizePct70);
                  console.log(`[Boof7.0][OptionsBot] Position size: ${(sizePct70*100).toFixed(0)}% → $${orig} → $${settings.dollarAmount}`);
                  await supabase.from('options_bots').update({ last_position_size_pct: sizePct70 }).eq('id', bot.id as string);
                }
                sigResult = generateSignalBoof30(candles, settings.tradeDirection);
              } else if (botSignal === 'boof80') {
                // Boof 8.0: Adaptive AI Scalper — self-tunes TP/SL from trade history
                const { data: recentTrades80 } = await supabase.from('options_trades')
                  .select('reason, pnl, regime').eq('bot_id', bot.id as string).eq('symbol', sym)
                  .not('pnl', 'is', null).order('closed_at', { ascending: false }).limit(20);
                const trades80 = (recentTrades80 || []).map((t: any) => ({
                  reason:  t.reason  || '',
                  pnlPct:  Number(t.pnl) || 0,
                  regime:  t.regime  || 'UNKNOWN',
                }));
                const pnls80  = trades80.map((t: { pnlPct: number }) => t.pnlPct);
                const wins80  = pnls80.filter((p: number) => p > 0).length;
                const winRate80 = pnls80.length > 0 ? wins80 / pnls80.length : 0.5;
                let consLosses80 = 0;
                for (const p of pnls80) { if (p <= 0) consLosses80++; else break; }
                const isCrypto80 = sym.includes('-USD') || sym.includes('/USD');
                const boof80result = generateSignalBoof80(candles, settings.tradeDirection, {
                  recentTrades:      trades80,
                  consecutiveLosses: consLosses80,
                  recentWinRate:     winRate80,
                  isCrypto:          isCrypto80,
                });
                if (boof80result.killSwitch) {
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Boof 8.0 kill-switch: ${boof80result.killReason}` });
                  continue;
                }
                sigResult = { signal: boof80result.signal, price: boof80result.price, reason: boof80result.reason, trend: boof80result.trend, ema: boof80result.ema, adx: boof80result.adx };
                // Write adaptive TP/SL back to symbol_rules so UI reflects live values
                if (boof80result.signal !== 'none' && boof80result.dynamicTP > 0) {
                  const currentRules: Array<{symbol:string;tp:number;sl:number;dir?:string;adapted_at?:string}> = (bot.symbol_rules as any) || [];
                  const ruleIdx = currentRules.findIndex((r: any) => r.symbol?.toUpperCase() === sym.toUpperCase());
                  // Hard SL floor by expiry — 0DTE can't go past -15%, 1DTE -20%, weekly+ -25%
                  const slFloor = settings.expiryType === '0dte' ? -15 : settings.expiryType === '1dte' ? -20 : -25;
                  const adaptedSL = Math.max(slFloor, boof80result.dynamicSL);
                  const adaptedRule = {
                    symbol:     sym,
                    tp:         Math.round(boof80result.dynamicTP * 10) / 10,
                    sl:         Math.round(adaptedSL * 10) / 10,
                    dir:        ruleIdx >= 0 ? (currentRules[ruleIdx].dir || 'both') : 'both',
                    adapted_at: new Date().toISOString(),
                  };
                  const updatedRules = ruleIdx >= 0
                    ? currentRules.map((r: any, idx: number) => idx === ruleIdx ? adaptedRule : r)
                    : [...currentRules, adaptedRule];
                  await supabase.from('options_bots').update({ symbol_rules: updatedRules }).eq('id', bot.id as string);
                  console.log(`[Boof8.0] Adaptive TP/SL written: ${sym} tp=${adaptedRule.tp}% sl=${adaptedRule.sl}% ci=${boof80result.choppiness.toFixed(1)} pw=${boof80result.patternWeight.toFixed(2)}`);
                }
              } else if (botSignal === 'test_always_buy') {
                // TEST MODE: Always fires BUY signal to test trade execution
                const lastClose = candles[candles.length - 1].close;
                sigResult = { signal: 'buy', price: lastClose, reason: 'TEST MODE: Always BUY' };
              } else if (botSignal === 'test_always_sell') {
                // TEST MODE: Always fires SELL signal to test trade execution
                const lastClose = candles[candles.length - 1].close;
                sigResult = { signal: 'sell', price: lastClose, reason: 'TEST MODE: Always SELL' };
              } else {
                sigResult = generateSignal(candles, settings);
              }
              signal = sigResult.signal;
              price = sigResult.price;
              reason = sigResult.reason;

              // ── ADX GATE: Block all signals in choppy/ranging markets ──
              // ADX < 20 = no trend = chop = skip. Applies to ALL strategies.
              // Boof 5.0 & SuperTrend return adx in sigResult. For others, calculate it.
              if (signal !== 'none' && botSignal !== 'test_always_buy' && botSignal !== 'test_always_sell') {
                let adxVal = sigResult.adx ?? 0;
                if (!adxVal || adxVal <= 0) {
                  // Calculate ADX from current candles if not returned by signal
                  const dmi = calcDMI(candles.map(c => c.high), candles.map(c => c.low), candles.map(c => c.close), 14);
                  adxVal = dmi.adx[dmi.adx.length - 1] ?? 0;
                }
                const adxThreshold = 20;
                if (adxVal > 0 && adxVal < adxThreshold) {
                  console.log(`[OptionsBot] ${sym} ADX GATE: adx=${adxVal.toFixed(1)} < ${adxThreshold} — market is choppy, skipping signal`);
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `ADX gate: ${adxVal.toFixed(1)} < ${adxThreshold} (choppy market)` });
                  continue;
                }
                console.log(`[OptionsBot] ${sym} ADX GATE: adx=${adxVal.toFixed(1)} >= ${adxThreshold} — trending, allowing signal`);
              }

              // ── EMA PRICE CONFIRMATION GATE ──
              // Always uses a HIGHER timeframe EMA to avoid noise from fast signal intervals.
              // 0DTE bots: 15m EMA20. Weekly+: 1h EMA20. Prevents 5m chop from bypassing gate.
              if (signal !== 'none' && botSignal !== 'test_always_buy' && botSignal !== 'test_always_sell') {
                const curClose = candles[candles.length - 1].close;
                const gateInterval = settings.expiryType === '0dte' ? '15m' : '1h';
                let emaVal = 0;
                try {
                  const gateCandles = await fetchCandles(sym, gateInterval, 40);
                  if (gateCandles.length >= 20) {
                    const ema20 = calcEMA(gateCandles.map(c => c.close), 20);
                    emaVal = ema20[ema20.length - 1] ?? 0;
                  }
                } catch (_) {}
                if (!emaVal || emaVal <= 0) {
                  // EMA unavailable (likely Yahoo rate limit) — warn but allow trade through
                  // Blocking on data failure kills good trades; the other gates (ADX, trend filter) still protect
                  console.log(`[OptionsBot] ${sym} EMA GATE: cannot compute ${gateInterval} EMA — passing through (other gates active)`);
                } else {
                  if (signal === 'buy' && curClose < emaVal) {
                    console.log(`[OptionsBot] ${sym} EMA GATE: BUY blocked — close=$${curClose.toFixed(2)} < ${gateInterval} ema=$${emaVal.toFixed(2)}`);
                    results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `EMA gate: BUY blocked, close $${curClose.toFixed(2)} < ${gateInterval} EMA $${emaVal.toFixed(2)}` });
                    continue;
                  }
                  if (signal === 'sell' && curClose > emaVal) {
                    console.log(`[OptionsBot] ${sym} EMA GATE: SELL blocked — close=$${curClose.toFixed(2)} > ${gateInterval} ema=$${emaVal.toFixed(2)}`);
                    results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `EMA gate: SELL blocked, close $${curClose.toFixed(2)} > ${gateInterval} EMA $${emaVal.toFixed(2)}` });
                    continue;
                  }
                  console.log(`[OptionsBot] ${sym} EMA GATE: ${signal.toUpperCase()} confirmed — close=$${curClose.toFixed(2)} ${gateInterval} ema=$${emaVal.toFixed(2)}`);
                }
              }

              console.log(`[OptionsBot] "${bot.name}" | ${sym} | SIGNAL: ${signal} | price=$${price.toFixed(2)} | signal_type=${botSignal} | ${reason}`);
              console.log(`[OptionsBot] ${sym} STEP 1: Signal generated, proceeding to trend filter...`);

              // Trend Filter: EMA 25 on selected timeframe (or EMA 150 on 1m) — 2-candle confirmation required
              const trendFilter = bot.trend_filter as string || 'none';
              console.log(`[OptionsBot] ${sym} trend filter check: trendFilter=${trendFilter}, signal=${signal}`);
              
              if (trendFilter !== 'none' && signal !== 'none') {
                try {
                  // VWAP 1m filter — uses today's 1m candles
                  if (trendFilter === 'vwap_1m') {
                    const vwapCandles: Candle[] = await Promise.race([
                      fetchCandles(sym, '1m', 390),
                      new Promise<Candle[]>((_, reject) => setTimeout(() => reject(new Error('Timeout')), 10000))
                    ]) as Candle[];
                    if (vwapCandles.length < 30) {
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `VWAP: not enough 1m candles (${vwapCandles.length}/30) — waiting for 30min after open` });
                      continue;
                    }
                    const vwap = calcVWAP(vwapCandles);
                    const lastClose = vwapCandles[vwapCandles.length - 1].close;
                    const prevClose = vwapCandles[vwapCandles.length - 2].close;
                    const lastAboveVwap = lastClose >= vwap;
                    const prevAboveVwap = prevClose >= vwap;
                    console.log(`[OptionsBot] ${sym} VWAP 1m: price=${lastClose.toFixed(2)} vwap=${vwap.toFixed(2)} last=${lastAboveVwap?'above':'below'} prev=${prevAboveVwap?'above':'below'}`);
                    if (signal === 'buy' && !(lastAboveVwap && prevAboveVwap)) {
                      console.log(`[OptionsBot] ${sym} BUY blocked — price not confirmed above VWAP`);
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'BUY blocked: price below VWAP' });
                      continue;
                    }
                    if (signal === 'sell' && (lastAboveVwap || prevAboveVwap)) {
                      console.log(`[OptionsBot] ${sym} SELL blocked — price not confirmed below VWAP`);
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'SELL blocked: price above VWAP' });
                      continue;
                    }
                    console.log(`[OptionsBot] ${sym} VWAP filter passed — signal aligned with VWAP`);
                  } else {
                  const is1mEma150 = trendFilter === '1m_ema150';
                  const tfToFetch = is1mEma150 ? '1m' : trendFilter;
                  const emaPeriod = is1mEma150 ? 150 : 25;
                  const minCandles = is1mEma150 ? 160 : 30;
                  const candlesToFetch = is1mEma150 ? 300 : 120;
                  console.log(`[OptionsBot] ${sym} fetching ${tfToFetch} candles for EMA${emaPeriod} trend check...`);
                  const higherTfCandles: Candle[] = await Promise.race([
                    fetchCandles(sym, tfToFetch, candlesToFetch),
                    new Promise<Candle[]>((_, reject) => setTimeout(() => reject(new Error('Timeout')), 10000))
                  ]) as Candle[];
                  console.log(`[OptionsBot] ${sym} fetched ${higherTfCandles.length} ${tfToFetch} candles`);
                  if (higherTfCandles.length < minCandles) {
                    console.log(`[OptionsBot] ${sym} trend filter: not enough candles (${higherTfCandles.length}) — blocking trade`);
                    results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Trend filter: insufficient candles` });
                    continue;
                  }
                  if (higherTfCandles.length >= minCandles) {
                    const higherTfCloses = higherTfCandles.map((c: Candle) => c.close);
                    const higherTfEma = calcEMA(higherTfCloses, emaPeriod);
                    const emaLast = higherTfEma[higherTfEma.length - 1];
                    const emaPrev = higherTfEma[higherTfEma.length - 2];
                    const priceLast = higherTfCloses[higherTfCloses.length - 1];
                    const pricePrev = higherTfCloses[higherTfCloses.length - 2];
                    // 2-candle confirmation: both recent closes must be on same side of EMA
                    const lastAbove = priceLast >= emaLast;
                    const prevAbove = pricePrev >= emaPrev;
                    let higherTfTrend: string;
                    if (lastAbove && prevAbove) higherTfTrend = 'up';
                    else if (!lastAbove && !prevAbove) higherTfTrend = 'down';
                    else higherTfTrend = 'neutral'; // mixed — don't trade
                    const higherTfEmaVal = emaLast;
                    const currentPrice = priceLast;
                    console.log(`[OptionsBot] ${sym} EMA25 ${trendFilter}: price=${priceLast.toFixed(2)} ema=${emaLast.toFixed(2)} trend=${higherTfTrend} (2-bar confirm: last=${lastAbove?'above':'below'} prev=${prevAbove?'above':'below'})`);
                    if (higherTfTrend === 'neutral') {
                      console.log(`[OptionsBot] ${sym} BLOCKED — price crossing EMA25, no confirmed trend`);
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Trend filter: no confirmed trend (crossing EMA25)' });
                      continue;
                    }
                    
                    // Filter signal: Only trade if aligned with higher timeframe
                    if (signal === 'buy' && higherTfTrend === 'down') {
                      console.log(`[OptionsBot] ${sym} BUY blocked - ${trendFilter} trend is DOWN (price ${currentPrice.toFixed(2)} < EMA ${higherTfEmaVal.toFixed(2)})`);
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `BUY blocked: ${trendFilter} trend is DOWN` });
                      continue;
                    }
                    if (signal === 'sell' && higherTfTrend === 'up') {
                      console.log(`[OptionsBot] ${sym} SELL blocked - ${trendFilter} trend is UP (price ${currentPrice.toFixed(2)} > EMA ${higherTfEmaVal.toFixed(2)})`);
                      results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `SELL blocked: ${trendFilter} trend is UP` });
                      continue;
                    }
                    console.log(`[OptionsBot] ${sym} ${signal} approved - ${trendFilter} trend aligned (${higherTfTrend})`);
                  }
                  } // end else (non-VWAP filter)
                } catch (e) {
                  console.log(`[OptionsBot] ${sym} trend filter error — blocking trade to be safe: ${e}`);
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Trend filter error: ${e}` });
                  continue;
                }
              } else {
                console.log(`[OptionsBot] ${sym} no trend filter or no signal, proceeding`);
              }
              
              console.log(`[OptionsBot] ${sym} STEP 2: Trend filter passed, checking signal validity...`);
              if (signal === 'none') {
                console.log(`[OptionsBot] ${sym} signal is none, skipping`);
                // Clear pending signal if trend reversed
                if (bot.last_signal && bot.last_signal !== 'none') {
                  await supabase.from('options_bots').update({ last_signal: null, last_signal_at: null }).eq('id', bot.id);
                }
                results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'no_signal' }); continue;
              }
              console.log(`[OptionsBot] ${sym} signal=${signal}, direction=${settings.tradeDirection}`);
              
              if (signal === 'buy'  && settings.tradeDirection === 'short') { console.log(`[OptionsBot] ${sym} blocked by direction filter (buy vs short)`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Direction filter' }); continue; }
              if (signal === 'sell' && settings.tradeDirection === 'long')  { console.log(`[OptionsBot] ${sym} blocked by direction filter (sell vs long)`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Direction filter' }); continue; }
              console.log(`[OptionsBot] ${sym} STEP 3: Direction filter passed, checking dedup...`);

              // In-memory dedup: prevent parallel batch from trading same symbol twice in one run
              console.log(`[OptionsBot] ${sym} checking tradedThisRun...`);
              if (tradedThisRun.has(sym)) { console.log(`[OptionsBot] ${sym} blocked - already traded this run`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Already traded this symbol in this run' }); continue; }
              tradedThisRun.add(sym);
              console.log(`[OptionsBot] ${sym} STEP 4: Dedup passed, checking 1-min race...`);
              console.log(`[OptionsBot] ${sym} checking 1-minute race condition...`);
              const oneMinuteAgo = new Date(Date.now() - 60 * 1000).toISOString();
              const { data: recent1m } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).gte('created_at', oneMinuteAgo).limit(1);
              console.log(`[OptionsBot] ${sym} recent1m check: ${recent1m?.length || 0} trades found`);
              if (recent1m && recent1m.length > 0) { console.log(`[OptionsBot] ${sym} STEP 4.5: Race condition hit, skipping`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Trade within 1 minute' }); continue; }
              console.log(`[OptionsBot] ${sym} STEP 5: Race check passed, checking open positions...`);
              if (recent1m && recent1m.length > 0) { console.log(`[OptionsBot] ${sym} blocked - 1min race condition`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Duplicate trade within 1 minute (race condition)` }); continue; }
              console.log(`[OptionsBot] ${sym} passed 1-minute check`);

              // Block if already in open position on this symbol for this bot
              console.log(`[OptionsBot] ${sym} checking existing open positions...`);
              const { data: existingOpen } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).eq('status', 'open').limit(1);
              console.log(`[OptionsBot] ${sym} existingOpen check: ${existingOpen?.length || 0} open trades`);
              if (existingOpen && existingOpen.length > 0) { console.log(`[OptionsBot] ${sym} blocked - already in open position`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Already in open position' }); continue; }
              console.log(`[OptionsBot] ${sym} passed existingOpen check`);

              // Distributed lock: check for any trade inserted in last 5 seconds (concurrent invocation guard)
              console.log(`[OptionsBot] ${sym} checking 5-second lock...`);
              const fiveSecAgo = new Date(Date.now() - 5 * 1000).toISOString();
              const { data: veryRecent } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).gte('created_at', fiveSecAgo).limit(1);
              console.log(`[OptionsBot] ${sym} veryRecent check: ${veryRecent?.length || 0} recent trades`);
              if (veryRecent && veryRecent.length > 0) { console.log(`[OptionsBot] ${sym} blocked - 5sec lock`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Concurrent invocation guard (5s lock)' }); continue; }
              console.log(`[OptionsBot] ${sym} passed 5-second check`);

              // 2-minute cooldown between entries on same symbol for this bot (0DTE scalping)
              console.log(`[OptionsBot] ${sym} checking 2-minute cooldown...`);
              const twoMinAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();
              const { data: recent2m } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).eq('signal', 'buy').gte('created_at', twoMinAgo).limit(1);
              console.log(`[OptionsBot] ${sym} recent2m check: ${recent2m?.length || 0} recent trades`);
              if (recent2m && recent2m.length > 0) { console.log(`[OptionsBot] ${sym} blocked - 2min cooldown`); results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: '2-minute cooldown between entries' }); continue; }
              console.log(`[OptionsBot] ${sym} passed ALL checks, proceeding to entry!`);

              // Close open opposite positions and return balance
              const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', bot.id).eq('symbol', sym).eq('status', 'open');
              const sigma = calcHistoricalVolatility(candles.map(c => c.close), 20, settings.interval);
              if (openTrades && openTrades.length > 0) {
                for (const open of openTrades) {
                  const optType: 'call' | 'put' = open.option_type;
                  const expDate = new Date(open.expiration_date);
                  const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
                  const _rawExit = blackScholes(price, open.strike, T, R, sigma, optType);
                  const _entry = Number(open.premium_per_contract);
                  // Sanity clamp: exit price cannot produce loss > 100% of entry cost
                  const exitPremium = Math.max(0, Math.min(_rawExit, _entry * 10));
                  const _maxLoss = -(Number(open.total_cost) || _entry * open.contracts * 100);
                  const pnl = Math.max(_maxLoss, (exitPremium - _entry) * open.contracts * 100);
                  
                  // Close via Alpaca if live trading
                  if (bot.broker === 'alpaca' && open.order_id) {
                    const alpacaResult = await placeAlpacaOptionOrder(
                      supabase,
                      bot.user_id,
                      open.symbol,
                      open.expiration_date,
                      open.option_type,
                      open.strike,
                      'sell',
                      open.contracts
                    );
                    if (alpacaResult.success) {
                      await supabase.from('options_trades').update({ 
                        status: 'closed', 
                        exit_price: exitPremium, 
                        pnl, 
                        close_order_id: alpacaResult.orderId,
                        closed_at: new Date().toISOString() 
                      }).eq('id', open.id);
                    } else {
                      await supabase.from('options_trades').update({ 
                        status: 'closed', 
                        exit_price: exitPremium, 
                        pnl, 
                        broker_error: alpacaResult.error,
                        closed_at: new Date().toISOString() 
                      }).eq('id', open.id);
                    }
                  } else {
                    // Paper trading
                    await supabase.from('options_trades').update({ status: 'closed', exit_price: exitPremium, pnl, closed_at: new Date().toISOString() }).eq('id', open.id);
                    // Return original cost + profit/loss back to balance
                    const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                    const bBal = Number(bRow?.paper_balance ?? 100000);
                    await supabase.from('options_bots').update({ paper_balance: bBal + Number(open.total_cost) + pnl }).eq('id', bot.id);
                  }
                }
              }

              // Determine option type based on signal and bot setting
              let optionType: 'call' | 'put';
              const botOptionType = bot.bot_option_type || 'both';
              if (botOptionType === 'call') {
                optionType = 'call';
              } else if (botOptionType === 'put') {
                optionType = 'put';
              } else {
                // 'both' - follow signal
                optionType = signal === 'buy' ? 'call' : 'put';
              }
              const targetExpiration = getExpirationDate(settings.expiryType);
              const expirationDate = findValidExpiration(targetExpiration);
              // Fetch Tastytrade access token for ALL bots if user has Tastytrade connected
              // This ensures paper trading uses the same real data as live trading
              let tastyAccessToken: string | null = null;
              try {
                const { data: tastyCreds } = await supabase.from('broker_credentials')
                  .select('credentials').eq('user_id', bot.user_id).eq('broker', 'tastytrade').maybeSingle();
                console.log(`[OptionsBot] ${sym} Tastytrade creds check: has_creds=${!!tastyCreds}, has_refresh=${!!tastyCreds?.credentials?.refresh_token}`);
                if (tastyCreds?.credentials?.refresh_token) {
                  const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${bot.user_id}`, {
                    headers: {
                      'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}`,
                      'apikey': Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
                    }
                  });
                  const tokenJson = await tokenRes.json();
                  console.log(`[OptionsBot] ${sym} Tastytrade token refresh: status=${tokenRes.status}, has_access_token=${!!tokenJson.access_token}, error=${tokenJson.error || 'none'}`);
                  if (tokenJson.access_token) tastyAccessToken = tokenJson.access_token;
                }
              } catch (e) {
                console.log(`[OptionsBot] ${sym} Tastytrade token refresh error: ${e}`);
              }

              // Spot price priority: Alpaca → Tastytrade → Yahoo
              // Alpaca is first — real-time SIP data, most accurate
              let spotPrice: number | null = null;

              // 1. Try Alpaca first (real-time SIP data, always accurate)
              if (alpacaCreds?.api_key) {
                spotPrice = await fetchAlpacaSpotPrice(sym, alpacaCreds.api_key, alpacaCreds.secret_key);
                if (spotPrice && !sanityCheckSpot(sym, spotPrice, price)) spotPrice = null;
              }

              // 2. Try Tastytrade real-time
              if (!spotPrice && tastyAccessToken) {
                spotPrice = await fetchTastytradeSpotPrice(sym, tastyAccessToken);
              }

              // 3. Yahoo fallback — sanity check required
              if (!spotPrice) {
                try {
                  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1m&range=1d`;
                  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
                  const json = await res.json();
                  const meta = json?.chart?.result?.[0]?.meta;
                  const p = meta?.regularMarketPrice ?? meta?.price;
                  if (p && p > 0 && sanityCheckSpot(sym, p, price)) spotPrice = p;
                } catch (_) {}
              }

              // HARD STOP: if we can't get a sane spot price, don't trade
              if (!spotPrice || spotPrice <= 0) {
                console.log(`[OptionsBot] BLOCKED: Cannot get reliable spot price for ${sym} — skipping trade`);
                results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'No reliable spot price' });
                continue;
              }
              console.log(`[OptionsBot] Spot price for ${sym}: $${spotPrice} (broker=${bot.broker})`);
              const strikeInterval = spotPrice > 500 ? 5 : spotPrice > 100 ? 5 : spotPrice > 50 ? 2.5 : 1;
              const dollarAmount = bot.bot_dollar_amount || 500;

              // Strike selection:
              // - 0DTE: start 2 strikes ITM (higher delta ~0.65-0.75, moves more with stock)
              // - Weekly/Monthly: start ATM (~0.50 delta)
              // Walk toward OTM until 1-contract cost fits budget.
              // Hard minimum: $1.00/contract. Never buy cheap far-OTM garbage.
              const atmStrike = Math.round(spotPrice / strikeInterval) * strikeInterval;
              const MIN_PREMIUM = 1.00; // $100/contract minimum
              const MAX_STRIKES_WALK = 10; // walk up to 10 strikes to find budget-fitting option
              const MAX_STRIKE_PCT_FROM_SPOT = 0.10; // never buy more than 10% OTM
              // For 0DTE, start ITM (negative offset = ITM for calls, positive = ITM for puts)
              const startOffset = expiryType === '0dte' ? -2 : 0;
              const startStrike = optionType === 'call'
                ? atmStrike + startOffset * strikeInterval  // calls: go lower = ITM
                : atmStrike - startOffset * strikeInterval; // puts: go higher = ITM
              let strike = startStrike;
              let premium = 0;
              let cheapestStrike = startStrike;
              let cheapestPremium = 0;

              console.log(`[OptionsBot] ${sym} starting strike selection: spot=$${spotPrice}, budget=$${dollarAmount}, startStrike=$${startStrike}, optionType=${optionType}`);
              
              for (let offset = 0; offset <= MAX_STRIKES_WALK; offset++) {
                const candidateStrike = optionType === 'call'
                  ? startStrike + offset * strikeInterval
                  : startStrike - offset * strikeInterval;
                console.log(`[OptionsBot] ${sym} trying strike $${candidateStrike} (offset=${offset})...`);
                
                const candidatePremium = await fetchRealOptionPrice(sym, candidateStrike, expirationDate, optionType, settings.interval, bot.user_id, expiryType, alpacaCreds?.api_key, alpacaCreds?.secret_key);
                console.log(`[OptionsBot] ${sym} $${candidateStrike} premium=$${candidatePremium?.toFixed(2) ?? 'null'}, tastyAccessToken=${tastyAccessToken ? 'YES' : 'NO'}`);
                
                // If no price found, continue to next strike
                if (!candidatePremium || candidatePremium <= 0) {
                  console.log(`[OptionsBot] ${sym} CONTINUING: No price for $${candidateStrike}, trying next offset`);
                  continue;
                }

                // Track cheapest valid strike seen (above min premium)
                if (candidatePremium >= MIN_PREMIUM && (cheapestPremium === 0 || candidatePremium < cheapestPremium)) {
                  cheapestStrike = candidateStrike;
                  cheapestPremium = candidatePremium;
                }

                // Too cheap — stop walking further OTM (premiums only get cheaper from here)
                if (candidatePremium < MIN_PREMIUM) {
                  console.log(`[OptionsBot] $${candidateStrike} premium $${candidatePremium.toFixed(2)} below $${MIN_PREMIUM} min — stopping walk`);
                  break;
                }

                // Sanity check: strike must be within 10% of spot price
                const pctFromSpot = Math.abs(candidateStrike - spotPrice) / spotPrice;
                if (pctFromSpot > MAX_STRIKE_PCT_FROM_SPOT) {
                  console.log(`[OptionsBot] BLOCKED deep OTM: $${candidateStrike} is ${(pctFromSpot*100).toFixed(1)}% from spot $${spotPrice.toFixed(2)} — stopping walk`);
                  break;
                }

                // 1 contract fits budget — use it
                if (candidatePremium * 100 <= dollarAmount) {
                  strike = candidateStrike;
                  premium = candidatePremium;
                  break;
                }

                console.log(`[OptionsBot] $${candidateStrike} @ $${candidatePremium.toFixed(2)}/contract ($${(candidatePremium*100).toFixed(0)}) exceeds budget $${dollarAmount} — trying next strike`);
              }

              // If nothing fit budget, buy 1 contract of the cheapest valid strike found
              if ((!premium || premium < MIN_PREMIUM) && cheapestPremium >= MIN_PREMIUM) {
                strike = cheapestStrike;
                premium = cheapestPremium;
                console.log(`[OptionsBot] ${sym} no strike fit budget — buying 1 contract of cheapest: $${strike} @ $${premium.toFixed(2)}`);
              }

              // HARD STOP — never trade below $1.00 premium under any circumstance
              console.log(`[OptionsBot] ${sym} strike selection complete: final strike=$${strike}, final premium=$${premium?.toFixed(2) ?? '0'}`);
              
              if (!premium || premium < MIN_PREMIUM) {
                console.log(`[OptionsBot] BLOCKED: ${sym} premium $${premium?.toFixed(2) ?? '0'} < $${MIN_PREMIUM} — refusing to trade`);
                continue;
              }

              const contracts = Math.max(1, Math.floor(dollarAmount / (premium * 100)));
              const totalCost = contracts * premium * 100;
              console.log(`[OptionsBot] Selected: ${sym} ${optionType} $${strike} @ $${premium.toFixed(2)}/contract x${contracts} = $${totalCost.toFixed(2)} (budget=$${dollarAmount} spot=$${spotPrice.toFixed(2)})`);


              let tradeStatus = 'open';
              let orderId = null;
              let brokerError = null;

              // Live trading
              if (bot.broker === 'tastytrade') {
                console.log(`[OptionsBot] Placing Tastytrade order: ${contracts} contracts of ${sym} ${optionType}`);
                const tastyResult = await placeTastytradeOptionOrder(
                  supabase, bot.user_id, sym, expirationDate, optionType, strike, 'Buy to Open', contracts
                );
                if (tastyResult.success) {
                  tradeStatus = 'open';
                  orderId = tastyResult.orderId;
                  if (tastyResult.fillPrice && tastyResult.fillPrice > 0) {
                    premium = tastyResult.fillPrice;
                    console.log(`[OptionsBot] Tastytrade real fill price: $${premium.toFixed(2)}/contract`);
                  } else {
                    console.log(`[OptionsBot] Tastytrade fill pending, using estimate: $${premium.toFixed(2)}/contract`);
                  }
                } else {
                  tradeStatus = 'failed';
                  brokerError = tastyResult.error;
                  console.error(`[OptionsBot] Tastytrade order failed: ${brokerError}`);
                }
              } else if (bot.broker === 'alpaca') {
                console.log(`[OptionsBot] Placing Alpaca order: ${contracts} contracts of ${sym} ${optionType}`);
                const alpacaResult = await placeAlpacaOptionOrder(
                  supabase, bot.user_id, sym, expirationDate, optionType, strike, 'buy', contracts
                );
                if (alpacaResult.success) {
                  tradeStatus = alpacaResult.status === 'filled' ? 'filled' : 'pending';
                  orderId = alpacaResult.orderId;
                  if (alpacaResult.fillPrice && alpacaResult.fillPrice > 0) {
                    premium = alpacaResult.fillPrice;
                    console.log(`[OptionsBot] Alpaca real fill price: $${premium.toFixed(2)}/contract`);
                  }
                } else {
                  tradeStatus = 'failed';
                  brokerError = alpacaResult.error;
                  console.error(`[OptionsBot] Alpaca order failed: ${brokerError}`);
                }
              } else {
                // Paper trading: check balance before entering
                const { data: botRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                const currentBalance = Number(botRow?.paper_balance ?? 100000);
                if (currentBalance < totalCost) {
                  console.log(`[OptionsBot] SKIP: insufficient paper balance $${currentBalance.toFixed(2)} for ${sym} trade costing $${totalCost.toFixed(2)}`);
                  results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Insufficient balance ($${currentBalance.toFixed(2)} < $${totalCost.toFixed(2)})` });
                  continue;
                }
                await supabase.from('options_bots').update({ paper_balance: currentBalance - totalCost }).eq('id', bot.id);
              }

              console.log(`[OptionsBot] Inserting trade: ${sym} ${optionType} strike=${strike} premium=$${premium.toFixed(2)} contracts=${contracts} total=$${totalCost.toFixed(2)} status=${tradeStatus}`);
              const tradeNow = new Date();
              const { error: insertErr } = await supabase.from('options_trades').insert({
                user_id: bot.user_id, bot_id: bot.id, symbol: sym,
                option_type: optionType, strike, expiration_date: expirationDate,
                contracts, premium_per_contract: premium, total_cost: totalCost,
                entry_price: premium, status: tradeStatus, signal, reason,
                broker: bot.broker || 'paper',
                broker_error: brokerError,
                created_at: tradeNow.toISOString(),
                // ML features for Boof 4.0 training
                entry_regime: (sigResult as any).regime,
                entry_rsi: (sigResult as any).rsi,
                entry_slope: (sigResult as any).slope,
                entry_atr: (sigResult as any).atr,
                entry_spot: price,
                entry_ema: sigResult.ema,
                hour_of_day: tradeNow.getHours(),
                day_of_week: tradeNow.getDay(),
                signal_version: botSignal,
              });
              if (insertErr) console.error(`[OptionsBot] INSERT FAILED for ${sym}:`, insertErr.message);

              results.push({ bot_id: bot.id, status: tradeStatus, symbol: sym, option_type: optionType, strike, expiration_date: expirationDate, contracts, premium: premium.toFixed(2), total_cost: totalCost.toFixed(2), budget: dollarAmount, order_id: orderId, broker_error: brokerError, sigma: (sigma * 100).toFixed(1) + '%', signal, reason });

          } catch (err) {
            results.push({ bot_id: bot.id, symbol: sym, status: 'error', error: String(err) });
          }
        }
      } catch (err) {
        results.push({ bot_id: bot.id, status: 'error', error: String(err) });
      }
      
      // Update last_run_at after successful processing
      await supabase.from('options_bots').update({ last_run_at: now.toISOString() }).eq('id', bot.id);
    }

    console.log(`Processed ${results.length} results:`, results);

    return new Response(JSON.stringify({ processed: results.length, results }), {
      status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
