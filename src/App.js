import React, { useEffect, useRef, useState, useMemo } from "react";
import {
  getSummary, getCategorySpend, getFraudTrend, getTransactions,
  uploadCSV, addTransaction,
  getAmountByGender, getFraudByCategory, getAvgAmountByCategory, getTopMerchants, getAmountHistogram
} from "./api";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer,
  LineChart, Line,
  PieChart, Pie, Cell, Legend,
  AreaChart, Area
} from "recharts";

const API_URL = process.env.REACT_APP_API_URL || "http://127.0.0.1:8000";

export default function App() {
  const [summary, setSummary] = useState(null);
  const [categorySpend, setCategorySpend] = useState([]);
  const [fraudTrend, setFraudTrend] = useState([]);
  const [table, setTable] = useState({ count: 0, items: [] });
  const [file, setFile] = useState(null);
  const [newTx, setNewTx] = useState({
    step: 0, customer: "", age: "unknown", gender: "unknown",
    zipcodeori: "", merchant: "", zipmerchant: "", category: "",
    amount: 0, fraud: 0
  });

  // extra datasets
  const [amountByGender, setAmountByGender] = useState([]);
  const [fraudByCategory, setFraudByCategory] = useState([]);
  const [avgAmountByCategory, setAvgAmountByCategory] = useState([]);
  const [topMerchants, setTopMerchants] = useState([]);
  const [amountHistogram, setAmountHistogram] = useState([]);

  const pollRef = useRef(null);

  // helpers
  const numberFmt = (n) => new Intl.NumberFormat().format(Number(n || 0));
  const compact   = (n) => new Intl.NumberFormat("en", { notation: "compact" }).format(Number(n || 0));
  const currency  = (n) => `$${new Intl.NumberFormat().format(Number(n || 0))}`;
  const pct       = (x) => `${(Number(x || 0) * 100).toFixed(1)}%`;

  // palette
  const PALETTE = ["#6366F1","#22C55E","#F59E0B","#EF4444","#06B6D4","#A855F7","#84CC16","#E879F9","#3B82F6","#F97316"];
  const pickColorByIndex = (i) => PALETTE[i % PALETTE.length];

  // fetch all
  const refreshAll = async () => {
    const [s, c, f, t, gAmt, fCat, aAvg, mTop, hist] = await Promise.all([
      getSummary(),
      getCategorySpend(),
      getFraudTrend(),
      getTransactions({ limit: 20, offset: 0 }),
      getAmountByGender(),
      getFraudByCategory(12),
      getAvgAmountByCategory(12),
      getTopMerchants(10),
      getAmountHistogram(20, "quantile"), // quantile for readability
    ]);

    setSummary(s); setCategorySpend(c); setFraudTrend(f); setTable(t);
    setAmountByGender(gAmt); setFraudByCategory(fCat); setAvgAmountByCategory(aAvg);
    setTopMerchants(mTop); setAmountHistogram(hist);
  };

  // realtime via WS; fallback polling
  useEffect(() => {
    refreshAll();
    const wsUrl = API_URL.replace("http", "ws") + "/ws/changes";
    let ws;
    try {
      ws = new WebSocket(wsUrl);
      ws.onmessage = () => refreshAll();
      ws.onerror = () => startPolling();
      ws.onclose  = () => startPolling();
    } catch {
      startPolling();
    }
    return () => {
      if (ws) ws.close();
      if (pollRef.current) clearInterval(pollRef.current);
    };
    // eslint-disable-next-line
  }, []);

  const startPolling = () => {
    if (pollRef.current) return;
    pollRef.current = setInterval(refreshAll, 5000);
  };

  const onUpload = async () => {
    if (!file) return;
    await uploadCSV(file);
    setFile(null);
    await refreshAll();
  };

  const onAddTx = async (e) => {
    e.preventDefault();
    const payload = {
      ...newTx,
      step: Number(newTx.step),
      amount: Number(newTx.amount),
      fraud: Number(newTx.fraud)
    };
    await addTransaction(payload);
    await refreshAll();
  };

  // ---- NEW: compute Lorenz curve from histogram ----
  const lorenzData = useMemo(() => {
    if (!amountHistogram?.length) return [];
    const sorted = [...amountHistogram].sort((a, b) => a.mid - b.mid);
    const totalCount  = sorted.reduce((s, d) => s + (d.count || 0), 0);
    const totalAmount = sorted.reduce((s, d) => s + (d.mid * (d.count || 0)), 0);

    let cumCount = 0, cumAmount = 0;
    const pts = sorted.map(d => {
      const c = d.count || 0;
      cumCount  += c;
      cumAmount += d.mid * c;
      return { p: cumCount / totalCount, L: cumAmount / totalAmount };
    });
    return [{ p: 0, L: 0 }, ...pts, { p: 1, L: 1 }];
  }, [amountHistogram]);

  return (
    <div style={{ padding: 24, fontFamily: "Inter, system-ui, Arial" }}>
      <h1>Financial Data Dashboard (MongoDB)</h1>

      {/* Controls */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 16 }}>
        <div style={{ background: "#fff", padding: 12, borderRadius: 8 }}>
          <b>Upload CSV to Database</b><br/>
          <input type="file" onChange={e => setFile(e.target.files?.[0] || null)} />
          <button onClick={onUpload} disabled={!file} style={{ marginLeft: 8 }}>Upload</button>
        </div>

        <form onSubmit={onAddTx} style={{ background: "#fff", padding: 12, borderRadius: 8, display: "flex", gap: 8, alignItems: "center" }}>
          <b>Add Transaction</b>
          <input placeholder="step" style={{width:80}} value={newTx.step} onChange={e=>setNewTx({...newTx, step:e.target.value})}/>
          <input placeholder="customer" value={newTx.customer} onChange={e=>setNewTx({...newTx, customer:e.target.value})}/>
          <input placeholder="gender" style={{width:120}} value={newTx.gender} onChange={e=>setNewTx({...newTx, gender:e.target.value})}/>
          <input placeholder="merchant" value={newTx.merchant} onChange={e=>setNewTx({...newTx, merchant:e.target.value})}/>
          <input placeholder="category" value={newTx.category} onChange={e=>setNewTx({...newTx, category:e.target.value})}/>
          <input placeholder="amount" style={{width:120}} value={newTx.amount} onChange={e=>setNewTx({...newTx, amount:e.target.value})}/>
          <select value={newTx.fraud} onChange={e=>setNewTx({...newTx, fraud:e.target.value})}>
            <option value={0}>Legit</option>
            <option value={1}>Fraud</option>
          </select>
          <button type="submit">Add</button>
        </form>
      </div>

      {/* Summary cards */}
      {summary && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 24 }}>
          <Card title="Total Transactions" value={numberFmt(summary.total_transactions)} />
          <Card title="Total Amount" value={`$${numberFmt(summary.total_amount.toFixed(2))}`} />
          <Card title="Fraud Cases" value={numberFmt(summary.fraud_cases)} />
          <Card title="Unique Customers" value={numberFmt(summary.unique_customers)} />
        </div>
      )}

      {/* Row 1 */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24, marginBottom: 24 }}>
        <ChartCard title="Spending by Category">
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={categorySpend} margin={{ top: 20, right: 30, left: 40, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="category" interval={0} tick={{ angle: -45, textAnchor: "end" }} height={80}
                     label={{ value: "Category", position: "insideBottom", offset: -60 }} />
              <YAxis tickFormatter={compact} label={{ value: "Total Amount ($)", angle: -90, position: "insideLeft" }} />
              <Tooltip formatter={(v) => currency(v)} />
              <Bar dataKey="amount" fill="#3B82F6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Fraud Trend by Step">
          <ResponsiveContainer width="100%" height={420}>
            <LineChart data={fraudTrend} margin={{ top: 20, right: 24, left: 16, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="step" ticks={fraudTrend.map(d => d.step).filter((v, i) => i === 0 || v % 10 === 0)}
                     tick={{ fontSize: 14 }} height={60} label={{ value: "Step", position: "insideBottom", offset: -30 }} />
              <YAxis domain={[0, (max) => Math.ceil((max || 0) * 1.2)]} allowDecimals={false}
                     tick={{ fontSize: 14 }} label={{ value: "Fraud Count", angle: -90, position: "insideLeft" }} />
              <Tooltip />
              <Line type="monotone" dataKey="fraud" stroke="#2563eb" strokeWidth={3} dot={{ r: 2 }} activeDot={{ r: 5 }} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 2 */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24, marginBottom: 24 }}>
        <ChartCard title="Total Spend by Gender">
          <ResponsiveContainer width="100%" height={420}>
            <BarChart data={amountByGender} margin={{ top: 20, right: 24, left: 16, bottom: 50 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="gender" tick={{ fontSize: 14 }} label={{ value: "Gender", position: "insideBottom", offset: -25 }} />
              <YAxis tickFormatter={compact} label={{ value: "Total Amount ($)", angle: -90, position: "insideLeft" }} />
              <Tooltip formatter={(v) => currency(v)} />
              <Bar dataKey="amount" fill="#22C55E" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Fraud Rate by Category (Top 12)">
          <ResponsiveContainer width="100%" height={420}>
            <BarChart data={fraudByCategory} margin={{ top: 20, right: 24, left: 16, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="category" interval={0} tick={{ angle: -40, textAnchor: "end", fontSize: 12 }} height={80}
                     label={{ value: "Category", position: "insideBottom", offset: -60 }} />
              <YAxis domain={[0, (max) => (max || 0) * 1.2]} tickFormatter={(v) => pct(v)}
                     label={{ value: "Fraud Rate", angle: -90, position: "insideLeft" }} />
              <Tooltip formatter={(v) => [pct(v), "fraud rate"]} />
              <Bar dataKey="fraud_rate" fill="#F59E0B" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 3 */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24, marginBottom: 24 }}>
        <ChartCard title="Average Amount by Category (Top 12)">
          <ResponsiveContainer width="100%" height={420}>
            <BarChart data={avgAmountByCategory} margin={{ top: 20, right: 24, left: 16, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="category" interval={0} tick={{ angle: -40, textAnchor: "end", fontSize: 12 }} height={80}
                     label={{ value: "Category", position: "insideBottom", offset: -60 }} />
              <YAxis tickFormatter={compact} label={{ value: "Avg Amount ($)", angle: -90, position: "insideLeft" }} />
              <Tooltip formatter={(v) => currency(v)} />
              <Bar dataKey="avg_amount" fill="#06B6D4" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Donut pie */}
        <ChartCard title="Top Merchants by Spend (Top 10)">
          <ResponsiveContainer width="100%" height={420}>
            <PieChart>
              <Tooltip
                formatter={(v, _n, p) => [currency(v), p?.payload?.merchant || "merchant"]}
                contentStyle={{ borderRadius: 8 }}
              />
              <Legend
                layout="vertical"
                align="right"
                verticalAlign="middle"
                formatter={(value, entry) => `${value} — ${compact(entry.payload.amount)}`}
              />
              <Pie
                data={topMerchants}
                dataKey="amount"
                nameKey="merchant"
                cx="45%"
                cy="50%"
                innerRadius={80}
                outerRadius={140}
                startAngle={90}
                endAngle={-270}
                paddingAngle={1}
                label={({ percent }) => `${(percent * 100).toFixed(0)}%`}
                labelLine={false}
              >
                {topMerchants.map((_, i) => (
                  <Cell key={`slice-${i}`} fill={pickColorByIndex(i)} />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 4 — REPLACED with Lorenz Curve */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 24, marginBottom: 24 }}>
        <ChartCard title="Lorenz Curve — Spend Concentration">
          <ResponsiveContainer width="100%" height={420}>
            <AreaChart data={lorenzData} margin={{ top: 20, right: 24, left: 16, bottom: 50 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="p"
                tickFormatter={(v) => `${Math.round(v * 100)}%`}
                label={{ value: "Cumulative share of transactions", position: "insideBottom", offset: -30 }}
              />
              <YAxis
                domain={[0, 1]}
                tickFormatter={(v) => `${Math.round(v * 100)}%`}
                label={{ value: "Cumulative share of amount", angle: -90, position: "insideLeft" }}
              />
              <Tooltip
                formatter={(v, name) =>
                  [`${(v * 100).toFixed(1)}%`, name === "L" ? "Share of amount" : "Share of transactions"]
                }
                labelFormatter={(v) => `${(v * 100).toFixed(1)}% of transactions`}
              />
              {/* Lorenz curve */}
              <Area type="monotone" dataKey="L" stroke="#0ea5e9" fill="#bae6fd" strokeWidth={3} dot={false} />
              {/* Line of equality y=x */}
              <Line type="linear" dataKey="p" stroke="#94a3b8" strokeDasharray="4 4" dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Recent table */}
      <div style={{ background:"#fff", padding:16, borderRadius:8 }}>
        <h3>Recent Transactions</h3>
        <table width="100%" cellPadding="8" style={{ borderCollapse:"collapse" }}>
          <thead><tr style={{ background:"#f6f6f6" }}>
            <th>step</th><th>customer</th><th>gender</th><th>merchant</th><th>category</th><th>amount</th><th>fraud</th>
          </tr></thead>
          <tbody>
            {table.items.map((r, i) => (
              <tr key={i} style={{ borderTop:"1px solid #eee" }}>
                <td>{r.step}</td><td>{r.customer}</td><td>{r.gender}</td>
                <td>{r.merchant}</td><td>{r.category}</td>
                <td>${numberFmt(r.amount)}</td>
                <td style={{ color: r.fraud ? "#d63031" : "#2d3436" }}>{r.fraud}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p>Total rows: {numberFmt(table.count)}</p>
      </div>
    </div>
  );
}

function Card({ title, value }) {
  return (
    <div style={{ background:"#fff", padding:16, borderRadius:8, boxShadow:"0 2px 10px rgba(0,0,0,0.05)" }}>
      <div style={{ fontSize:12, color:"#666" }}>{title}</div>
      <div style={{ fontSize:24, fontWeight:700 }}>{value}</div>
    </div>
  );
}

function ChartCard({ title, children }) {
  return (
    <div style={{ height:460, background:"#fff", padding:16, borderRadius:8, boxShadow:"0 2px 10px rgba(0,0,0,0.05)" }}>
      <h3>{title}</h3>
      <div style={{ height:"85%" }}>{children}</div>
    </div>
  );
}
