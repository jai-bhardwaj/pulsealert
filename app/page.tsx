"use client";

import { useEffect, useMemo, useRef, useState } from "react";

const SYMBOLS = ["BTC", "ETH", "SOL"] as const;
type Sym = (typeof SYMBOLS)[number];

interface Tick { symbol: string; price: number; ts: string }
interface Move {
  symbol: string; window_start: string; window_end: string;
  open_price: number; close_price: number; low_price: number; high_price: number; pct_change: number;
}
interface Alert {
  rule_id: string; user_id: string; symbol: string; direction: "DROP" | "RISE";
  threshold_pct: number; pct_change: number; open_price: number; close_price: number; fired_at: string;
}
interface Rule { rule_id: string; symbol: string; direction: "DROP" | "RISE"; threshold_pct: number; created_at: string }

const ALERT_COOLDOWN_MS = 5 * 60 * 1000; // one notification per rule per window

const fmtPrice = (n: number) =>
  n >= 1000 ? n.toLocaleString(undefined, { maximumFractionDigits: 0 }) : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
const fmtPct = (n: number) => `${n > 0 ? "+" : ""}${n.toFixed(3)}%`;
const fmtTime = (s: string) => {
  const d = new Date(s.replace(" ", "T"));
  return isNaN(d.getTime()) ? s : d.toLocaleTimeString();
};

export default function Home() {
  const [status, setStatus] = useState<"connecting" | "live" | "error">("connecting");
  const [prices, setPrices] = useState<Record<string, Tick>>({});
  const [moves, setMoves] = useState<Record<string, Move>>({});
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [rules, setRules] = useState<Rule[]>([]);
  const [tickCount, setTickCount] = useState(0);
  const [notifOk, setNotifOk] = useState(false);
  const lastFired = useRef<Map<string, number>>(new Map());
  const rulesRef = useRef<Rule[]>([]);

  // form
  const [symbol, setSymbol] = useState<Sym>("BTC");
  const [direction, setDirection] = useState<"DROP" | "RISE">("DROP");
  const [threshold, setThreshold] = useState("0.5");
  const [submitting, setSubmitting] = useState(false);
  const [formMsg, setFormMsg] = useState<string | null>(null);

  useEffect(() => {
    try {
      const saved = localStorage.getItem("pulsealert.rules");
      if (saved) { const r = JSON.parse(saved) as Rule[]; setRules(r); rulesRef.current = r; }
    } catch {}
    if (typeof Notification !== "undefined") setNotifOk(Notification.permission === "granted");
  }, []);

  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.addEventListener("status", (e) => setStatus(JSON.parse((e as MessageEvent).data).state));
    es.addEventListener("price_ticks", (e) => {
      const t = JSON.parse((e as MessageEvent).data) as Tick;
      setPrices((p) => ({ ...p, [t.symbol]: t }));
      setTickCount((n) => n + 1);
    });
    es.addEventListener("price_moves", (e) => {
      const m = JSON.parse((e as MessageEvent).data) as Move;
      setMoves((p) => ({ ...p, [m.symbol]: m }));
    });
    es.addEventListener("alerts", (e) => {
      const a = JSON.parse((e as MessageEvent).data) as Alert;
      // Only surface alerts for rules created in this browser, once per cooldown.
      const mine = rulesRef.current.some((r) => r.rule_id === a.rule_id);
      if (!mine) return;
      const now = Date.now();
      const last = lastFired.current.get(a.rule_id) ?? 0;
      if (now - last < ALERT_COOLDOWN_MS) return;
      lastFired.current.set(a.rule_id, now);
      setAlerts((prev) => [a, ...prev].slice(0, 50));
      if (typeof Notification !== "undefined" && Notification.permission === "granted") {
        new Notification(`${a.symbol} ${a.direction === "DROP" ? "dropped" : "rose"} ${fmtPct(a.pct_change)}`, {
          body: `Now $${fmtPrice(a.close_price)} (was $${fmtPrice(a.open_price)} five minutes ago). Your rule: ${a.direction} ${a.threshold_pct}%`,
        });
      }
    });
    es.onerror = () => setStatus("error");
    return () => es.close();
  }, []);

  async function enableNotifications() {
    if (typeof Notification === "undefined") return;
    const p = await Notification.requestPermission();
    setNotifOk(p === "granted");
  }

  async function createRule(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true); setFormMsg(null);
    try {
      const res = await fetch("/api/rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol, direction, threshold_pct: Number(threshold), user_id: "demo-user" }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error ?? "Failed");
      const next = [json.rule as Rule, ...rulesRef.current];
      rulesRef.current = next; setRules(next);
      try { localStorage.setItem("pulsealert.rules", JSON.stringify(next)); } catch {}
      setFormMsg(`Watching ${symbol} for a ${threshold}% ${direction.toLowerCase()} — rule is now in Kafka.`);
    } catch (err) {
      setFormMsg(`Error: ${(err as Error).message}`);
    } finally {
      setSubmitting(false);
    }
  }

  function removeRule(id: string) {
    const next = rulesRef.current.filter((r) => r.rule_id !== id);
    rulesRef.current = next; setRules(next);
    try { localStorage.setItem("pulsealert.rules", JSON.stringify(next)); } catch {}
  }

  const statusLabel = useMemo(() => ({
    connecting: "Connecting to Confluent Cloud…",
    live: `Live · ${tickCount} ticks this session`,
    error: "Stream disconnected — refresh",
  })[status], [status, tickCount]);

  return (
    <main>
      <header className="top">
        <div>
          <h1>Pulse<span>Alert</span></h1>
          <p className="tag">Set a price alert in 10 seconds. Get pinged the moment it moves.</p>
        </div>
        <div className={`status ${status}`}><i />{statusLabel}</div>
      </header>

      <section className="cards">
        {SYMBOLS.map((s) => {
          const t = prices[s]; const m = moves[s];
          const pct = m?.pct_change ?? 0;
          return (
            <article key={s} className={`card ${pct > 0 ? "up" : pct < 0 ? "down" : ""}`}>
              <div className="sym">{s}<small>/USD</small></div>
              <div className="price">{t ? `$${fmtPrice(t.price)}` : <span className="dim">waiting…</span>}</div>
              <div className="move">
                {m ? (<><b>{fmtPct(pct)}</b> <span className="dim">5m · L ${fmtPrice(m.low_price)} · H ${fmtPrice(m.high_price)}</span></>)
                   : <span className="dim">5-minute window warming up (first result ≈ 30s)</span>}
              </div>
            </article>
          );
        })}
      </section>

      <section className="grid">
        <div className="panel">
          <h2>New alert</h2>
          <form onSubmit={createRule} className="form">
            <label>Asset
              <select value={symbol} onChange={(e) => setSymbol(e.target.value as Sym)}>
                {SYMBOLS.map((s) => <option key={s}>{s}</option>)}
              </select>
            </label>
            <label>Notify me if it
              <select value={direction} onChange={(e) => setDirection(e.target.value as "DROP" | "RISE")}>
                <option value="DROP">drops</option>
                <option value="RISE">rises</option>
              </select>
            </label>
            <label>by at least (%)
              <input type="number" step="0.01" min="0.01" max="50" value={threshold} onChange={(e) => setThreshold(e.target.value)} />
            </label>
            <span className="hint">within any 5-minute window</span>
            <button disabled={submitting}>{submitting ? "Publishing…" : "Create alert"}</button>
          </form>
          {formMsg && <p className="msg">{formMsg}</p>}
          {!notifOk && (
            <button className="ghost" onClick={enableNotifications}>Enable browser notifications</button>
          )}

          <h3>Your rules ({rules.length})</h3>
          {rules.length === 0 && <p className="dim">No rules yet.</p>}
          <ul className="rules">
            {rules.map((r) => (
              <li key={r.rule_id}>
                <span><b>{r.symbol}</b> {r.direction === "DROP" ? "↓" : "↑"} {r.threshold_pct}%</span>
                <button className="x" onClick={() => removeRule(r.rule_id)} aria-label="remove">×</button>
              </li>
            ))}
          </ul>
        </div>

        <div className="panel">
          <h2>Alerts fired</h2>
          {alerts.length === 0 && <p className="dim">Nothing yet. Alerts are computed by Flink SQL in Confluent Cloud and arrive here within seconds of the move.</p>}
          <ul className="alerts">
            {alerts.map((a, i) => (
              <li key={`${a.rule_id}-${a.fired_at}-${i}`} className={a.direction === "DROP" ? "down" : "up"}>
                <div className="head">
                  <b>{a.symbol} {a.direction === "DROP" ? "dropped" : "rose"} {fmtPct(a.pct_change)}</b>
                  <time>{fmtTime(a.fired_at)}</time>
                </div>
                <div className="dim">${fmtPrice(a.open_price)} → ${fmtPrice(a.close_price)} · rule: {a.direction.toLowerCase()} {a.threshold_pct}%</div>
              </li>
            ))}
          </ul>
        </div>
      </section>

      <footer>
        Coinbase → <b>Confluent HTTP Source connector</b> → Kafka → <b>Flink SQL</b> (5-min sliding windows, interval join with your rules) → Kafka → this page.
        Every topic is governed by <b>Schema Registry</b>.
      </footer>
    </main>
  );
}
