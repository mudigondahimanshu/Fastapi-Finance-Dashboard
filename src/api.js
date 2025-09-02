import axios from "axios";

const API_URL = process.env.REACT_APP_API_URL || "http://127.0.0.1:8000";
const API_KEY = process.env.REACT_APP_API_KEY || "";

/** Axios instance */
export const api = axios.create({
  baseURL: API_URL,
  headers: { "X-API-Key": API_KEY },
  timeout: 60000, // default 60s to reduce spurious timeouts
});

/** Optional: change the key at runtime (e.g., after user input) */
export const setApiKey = (key) => {
  api.defaults.headers["X-API-Key"] = key || "";
};

/* -------- Core endpoints -------- */
export const getSummary        = () => api.get("/summary").then(r => r.data);
export const getCategorySpend  = () => api.get("/category-spend").then(r => r.data);
export const getFraudTrend     = () => api.get("/fraud-trend").then(r => r.data);
export const getTransactions   = (params = {}) =>
  api.get("/transactions", { params }).then(r => r.data);

/* File upload (give it a longer timeout; Axios sets the multipart boundary for us) */
export const uploadCSV = (file) => {
  const form = new FormData();
  form.append("file", file);
  return api.post("/ingest/csv", form, { timeout: 300000 }).then(r => r.data); // up to 5 min
};

export const addTransaction = (payload) =>
  api.post("/transactions", payload).then(r => r.data);

/* -------- Extra charts -------- */
export const getAmountByGender      = () => api.get("/amount-by-gender").then(r => r.data);
export const getFraudByCategory     = (limit = 12) =>
  api.get("/fraud-by-category", { params: { limit } }).then(r => r.data);
export const getAvgAmountByCategory = (limit = 12) =>
  api.get("/avg-amount-by-category", { params: { limit } }).then(r => r.data);
export const getTopMerchants        = (limit = 10) =>
  api.get("/top-merchants", { params: { limit } }).then(r => r.data);

/* Histogram: use quantile mode for a readable chart; allow more time */
export const getAmountHistogram = (bins = 20, mode = "quantile") =>
  api.get("/amount-histogram", { params: { bins, mode }, timeout: 120000 }).then(r => r.data);
