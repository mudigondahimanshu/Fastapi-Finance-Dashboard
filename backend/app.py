import os, io, re, json, asyncio
from typing import Optional
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query, Header, HTTPException, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse  # <-- NEW
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import csv  # <-- NEW

# --- env & db ---
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "financial")
COLL_NAME = os.getenv("MONGO_COLLECTION", "transactions")
API_KEY = os.getenv("API_KEY", "dev-key")

client = AsyncIOMotorClient(MONGODB_URI)
db = client[DB_NAME]
coll = db[COLL_NAME]

# --- app ---
app = FastAPI(title="Financial Data API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# --- auth dependency ---
async def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- startup: indexes for speed ---
@app.on_event("startup")
async def startup():
    await coll.create_index("step")
    await coll.create_index("category")
    await coll.create_index("fraud")
    await coll.create_index("customer")
    await coll.create_index("merchant")    # for Top Merchants
    await coll.create_index("amount")      # NEW: speeds up histogram/amount scans
    await coll.create_index([("customer", 1), ("step", 1)])

# --- cleaning helpers ---
def _strip_quotes(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    return s.str.replace(r"^['\"]|['\"]$", "", regex=True)

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(r"[^0-9a-zA-Z]+", "_", regex=True).str.strip("_")
    )
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = _strip_quotes(df[c])

    if "step" in df:   df["step"] = pd.to_numeric(df["step"], errors="coerce").fillna(0).astype(int)
    if "amount" in df: df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    if "fraud" in df:  df["fraud"] = pd.to_numeric(df["fraud"], errors="coerce").fillna(0).astype(int)

    age_map = {"0":"<=18","1":"19-25","2":"26-35","3":"36-45","4":"46-55","5":"56-65","6":"65+","U":"unknown","":"unknown"}
    if "age" in df: df["age"] = df["age"].replace(age_map).fillna("unknown")

    gender_map = {"M":"male","F":"female","E":"enterprise","U":"unknown","":"unknown"}
    if "gender" in df: df["gender"] = df["gender"].replace(gender_map).fillna("unknown")

    if "category" in df: df["category"] = df["category"].str.replace(r"^es_", "", regex=True)

    for z in ["zipcodeori","zipmerchant"]:
        if z in df: df[z] = df[z].astype(str)
    return df

# --- models ---
class TxIn(BaseModel):
    step: int
    customer: str
    age: Optional[str] = None
    gender: Optional[str] = None
    zipcodeori: Optional[str] = None
    merchant: Optional[str] = None
    zipmerchant: Optional[str] = None
    category: Optional[str] = None
    amount: float
    fraud: int

# --- routes ---
@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest/csv")
async def ingest_csv(
    file: UploadFile = File(...),
    dep: None = Depends(require_api_key)
):
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))
    df = clean_df(df)
    recs = df.to_dict("records")
    if not recs:
        return {"inserted": 0}
    inserted = 0
    BATCH = 50_000
    for i in range(0, len(recs), BATCH):
        chunk = recs[i:i+BATCH]
        res = await coll.insert_many(chunk, ordered=False)
        inserted += len(res.inserted_ids)
    return {"inserted": inserted}

@app.post("/transactions")
async def add_transaction(
    tx: TxIn,
    dep: None = Depends(require_api_key)
):
    doc = tx.model_dump()
    await coll.insert_one(doc)
    return {"ok": True}

@app.get("/transactions")
async def list_transactions(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    category: Optional[str] = None,
    gender: Optional[str] = None,
    fraud: Optional[int] = Query(None, ge=0, le=1),
):
    q = {}
    if category: q["category"] = {"$regex": f"^{re.escape(category)}$", "$options": "i"}
    if gender:   q["gender"]   = {"$regex": f"^{re.escape(gender)}$", "$options": "i"}
    if fraud is not None: q["fraud"] = fraud

    total = await coll.count_documents(q)
    cursor = coll.find(q).skip(offset).limit(limit)
    items = await cursor.to_list(length=limit)
    for it in items: it.pop("_id", None)
    return {"count": total, "items": items}

@app.get("/summary")
async def summary():
    pipeline = [
        {"$group": {
            "_id": None,
            "total_transactions": {"$sum": 1},
            "total_amount": {"$sum": "$amount"},
            "fraud_cases": {"$sum": "$fraud"},
            "customers": {"$addToSet": "$customer"}
        }},
        {"$project": {
            "_id": 0,
            "total_transactions": 1,
            "total_amount": 1,
            "fraud_cases": 1,
            "unique_customers": {"$size": "$customers"}
        }}
    ]
    out = await coll.aggregate(pipeline).to_list(length=1)
    return out[0] if out else {"total_transactions":0,"total_amount":0,"fraud_cases":0,"unique_customers":0}

@app.get("/category-spend")
async def category_spend():
    pipeline = [
        {"$group": {"_id": "$category", "amount": {"$sum": "$amount"}}},
        {"$project": {"_id": 0, "category": "$_id", "amount": 1}},
        {"$sort": {"amount": -1}}
    ]
    return await coll.aggregate(pipeline).to_list(length=10_000)

@app.get("/fraud-trend")
async def fraud_trend():
    pipeline = [
        {"$group": {"_id": "$step", "fraud": {"$sum": "$fraud"}}},
        {"$project": {"_id": 0, "step": "$_id", "fraud": 1}},
        {"$sort": {"step": 1}}
    ]
    return await coll.aggregate(pipeline).to_list(length=10_000)

# -----------------------
# Endpoints for 5 extra graphs
# -----------------------

@app.get("/amount-by-gender")
async def amount_by_gender():
    pipeline = [
        {"$group": {"_id": "$gender", "amount": {"$sum": "$amount"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "gender": {"$ifNull": ["$_id", "unknown"]}, "amount": 1, "count": 1}},
        {"$sort": {"amount": -1}}
    ]
    return await coll.aggregate(pipeline).to_list(length=1000)

@app.get("/fraud-by-category")
async def fraud_by_category(limit: int = Query(12, ge=1, le=50)):
    pipeline = [
        {"$group": {
            "_id": "$category",
            "fraud_count": {"$sum": "$fraud"},
            "total_tx": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "category": {"$ifNull": ["$_id", "unknown"]},
            "fraud_count": 1,
            "fraud_rate": {
                "$cond": [{"$gt": ["$total_tx", 0]},
                          {"$divide": ["$fraud_count", "$total_tx"]},
                          0]
            }
        }},
        {"$sort": {"fraud_count": -1}},
        {"$limit": limit}
    ]
    return await coll.aggregate(pipeline).to_list(length=1000)

@app.get("/avg-amount-by-category")
async def avg_amount_by_category(limit: int = Query(12, ge=1, le=50)):
    pipeline = [
        {"$group": {
            "_id": "$category",
            "avg_amount": {"$avg": "$amount"},
            "count": {"$sum": 1}
        }},
        {"$project": {"_id": 0, "category": {"$ifNull": ["$_id", "unknown"]}, "avg_amount": 1, "count": 1}},
        {"$sort": {"avg_amount": -1}},
        {"$limit": limit}
    ]
    return await coll.aggregate(pipeline).to_list(length=1000)

@app.get("/top-merchants")
async def top_merchants(limit: int = Query(10, ge=1, le=50)):
    pipeline = [
        {"$group": {"_id": "$merchant", "amount": {"$sum": "$amount"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "merchant": {"$ifNull": ["$_id", "unknown"]}, "amount": 1, "count": 1}},
        {"$sort": {"amount": -1}},
        {"$limit": limit}
    ]
    return await coll.aggregate(pipeline).to_list(length=1000)

@app.get("/amount-histogram")
async def amount_histogram(
    bins: int = Query(20, ge=5, le=60),
    mode: str = Query("fast", pattern="^(fast|quantile)$")  # new
):
    """
    mode=fast     -> equal-width bins (current behavior; very fast but skewed plots)
    mode=quantile -> quantile/auto bins (much more readable for skewed data)
    """
    if mode == "quantile":
        pipeline = [
            {"$match": {"amount": {"$gte": 0}}},
            {"$bucketAuto": {
                "groupBy": "$amount",
                "buckets": bins,
                "output": {"count": {"$sum": 1}}
            }},
            {"$project": {
                "_id": 0,
                "bin_min": "$_id.min",
                "bin_max": "$_id.max",
                "mid": {"$divide": [{"$add": ["$_id.min", "$_id.max"]}, 2]},
                "count": 1
            }},
            {"$sort": {"mid": 1}}
        ]
        return await coll.aggregate(pipeline, allowDiskUse=True).to_list(length=1000)

    # --- fast equal-width path (what you have now) ---
    mm = await coll.aggregate([
        {"$match": {"amount": {"$gte": 0}}},
        {"$group": {"_id": None, "min": {"$min": "$amount"}, "max": {"$max": "$amount"}}}
    ]).to_list(1)
    if not mm:
        return []
    mn = float(mm[0]["min"])
    mx = float(mm[0]["max"])
    width = max((mx - mn) / float(bins), 1.0)
    pipeline = [
        {"$match": {"amount": {"$gte": 0}}},
        {"$project": {"bin": {"$floor": {"$divide": [{"$subtract": ["$amount", mn]}, width]}}}},
        {"$group": {"_id": "$bin", "count": {"$sum": 1}}},
        {"$project": {
            "_id": 0,
            "count": 1,
            "mid": {"$add": [mn, {"$multiply": [{"$add": ["$_id", 0.5]}, width]}]}
        }},
        {"$sort": {"mid": 1}}
    ]
    return await coll.aggregate(pipeline, allowDiskUse=True).to_list(length=1000)

# -----------------------
# EVERYTHING endpoints
# -----------------------

@app.get("/everything")
async def everything(
    limit: int = Query(1000, ge=1, le=100_000),
    offset: int = Query(0, ge=0),
):
    """
    Returns all documents (paginated). Removes Mongo _id for clean JSON.
    """
    total = await coll.estimated_document_count()
    cursor = coll.find({}, {"_id": 0}).skip(offset).limit(limit)
    items = await cursor.to_list(length=limit)
    return {"count": total, "limit": limit, "offset": offset, "items": items}

@app.get("/everything.ndjson")
async def everything_ndjson():
    """
    Streams the entire collection as NDJSON (one JSON object per line).
    """
    async def gen():
        async for doc in coll.find({}, {"_id": 0}):
            yield json.dumps(doc) + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson")

@app.get("/everything.csv")
async def everything_csv():
    """
    Streams the entire collection as CSV. Header inferred from first document.
    """
    async def gen():
        first = await coll.find_one({}, {"_id": 0})
        if not first:
            yield ""
            return

        fields = list(first.keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

        # write first record
        writer.writerow(first)
        out = buf.getvalue()
        if out:
            yield out
            buf.seek(0); buf.truncate(0)

        # stream the rest
        async for doc in coll.find({}, {"_id": 0}).skip(1):
            writer.writerow(doc)
            out = buf.getvalue()
            if out:
                yield out
                buf.seek(0); buf.truncate(0)

    headers = {"Content-Disposition": 'attachment; filename="everything.csv"'}
    return StreamingResponse(gen(), media_type="text/csv", headers=headers)

# --- realtime via MongoDB change streams ---
@app.websocket("/ws/changes")
async def ws_changes(ws: WebSocket):
    await ws.accept()
    try:
        # Atlas supports change streams by default.
        # Local Mongo requires replica set (--replSet) to use watch().
        async with coll.watch() as stream:
            async for _ in stream:
                await ws.send_json({"event": "changed"})
    except Exception as e:
        # Fallback: tell client to poll
        await ws.send_json({"event": "error", "message": str(e)})
