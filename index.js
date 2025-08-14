// index.js

const express   = require("express");
const axios     = require("axios");
const qs        = require("qs");
const fs        = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

// ——— 1) Date-range suffix ———
const LAST_YEAR = "?start_date=2024-01-01&end_date=2024-12-31";
const THIS_YEAR = "?start_date=2025-01-01&end_date=2025-12-31";
const LAST_AND_THIS_YEAR = "?start_date=2024-01-01&end_date=2025-12-31";
const ALL_TIME = "?start_date=2022-01-01&end_date=2025-12-31";

// ——— 2) Map each QBO report to its Snowflake table + any URL suffix ———
const REPORTS = {
  AgedReceivables: {
    table:  "AGED_RECEIVABLES",
    suffix: ""
  },
  ItemSales: {
    table:  "ITEM_SALES",
    suffix: LAST_YEAR
  },
  TransactionList: {
    table:  "TRANSACTION_LIST",
    suffix: LAST_AND_THIS_YEAR
  },
  ProfitAndLoss: {
    table:  "P_AND_L",
    suffix: ""
  }
  // add more reports here as needed…
};

// ——— 3) Helper: promisify Snowflake execute ———
function execAsync({ sqlText, binds = [] }) {
  return new Promise((resolve, reject) => {
    sfConn.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => err ? reject(err) : resolve({ stmt, rows })
    });
  });
}

// ——— 4) Global error handling ———
process.on("unhandledRejection", (reason, promise) =>
  console.error("❌ Unhandled Rejection at:", promise, "reason:", reason)
);
process.on("uncaughtException", err => {
  console.error("❌ Uncaught Exception:", err);
  process.exit(1);
});

// ——— 5) Load environment variables ———
const {
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI,
  SF_ACCOUNT,
  SF_REGION,
  SF_USER,
  SF_PWD,
  SF_WAREHOUSE,
  SF_DATABASE,
  SF_SCHEMA,
  SF_ROLE,
  TOKEN_PATH = "./tokens.json",
  PORT = 3000
} = process.env;

// Validate required env
[
  CLIENT_ID, CLIENT_SECRET, REDIRECT_URI,
  SF_ACCOUNT, SF_USER, SF_PWD,
  SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
].forEach((v, i) => {
  if (!v) {
    console.error(`❌ Missing env var at index ${i}`);
    process.exit(1);
  }
});

// ——— 6) Connect to Snowflake ———
const sfConn = snowflake.createConnection({
  account:   SF_ACCOUNT,
  username:  SF_USER,
  password:  SF_PWD,
  warehouse: SF_WAREHOUSE,
  database:  SF_DATABASE,
  schema:    SF_SCHEMA,
  role:      SF_ROLE
});

sfConn.connect(err => {
  if (err) {
    console.error("❌ Snowflake connection error", err);
    process.exit(1);
  }
  console.log("✅ Connected to Snowflake");
});

// ——— 7) Express setup ———
const app = express();
app.use(express.json());
app.use((req, res, next) => {
  console.log(`[req] ${req.method} ${req.url}`);
  next();
});

// ——— 8) Token load/save helpers ———
async function loadTokens() {
  try {
    return JSON.parse(await fs.readFile(TOKEN_PATH, "utf8"));
  } catch {
    return {};
  }
}
async function saveTokens(tokens) {
  await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), "utf8");
}

// ——— 9) OAuth routes ———

// Home → link to connect
app.get("/", (req, res) =>
  res.send('<a href="/connect">Connect to QuickBooks</a>')
);

// Redirect to QuickBooks for consent
app.get("/connect", (req, res) => {
  const params = qs.stringify({
    client_id:     CLIENT_ID,
    response_type: "code",
    scope:         "com.intuit.quickbooks.accounting openid",
    redirect_uri:  REDIRECT_URI,
    state:         "xyz123"
  });
  res.redirect(`https://appcenter.intuit.com/connect/oauth2?${params}`);
});

// OAuth callback
app.get("/callback", async (req, res) => {
  const { code, realmId, error } = req.query;
  if (error || !code) return res.status(400).send("Authentication failed.");
  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type:   "authorization_code",
        code,
        redirect_uri: REDIRECT_URI
      }),
      {
        headers: {
          "Content-Type":  "application/x-www-form-urlencoded",
          Authorization:   `Basic ${Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString("base64")}`
        }
      }
    );
    await saveTokens({
      realm_id:      realmId,
      access_token:  tokenRes.data.access_token,
      refresh_token: tokenRes.data.refresh_token
    });
    res.send("<h1>Authentication successful.</h1>");
  } catch (e) {
    console.error("❌ Token exchange error", e.response?.data || e);
    res.status(500).send("Token exchange failed.");
  }
});

// ——— 10) Test Snowflake route ———
app.get("/test-sf", async (req, res) => {
  try {
    const { rows } = await execAsync({ sqlText: "SELECT 1 AS ping" });
    console.log("[test-sf] ping result:", rows);
    res.json(rows);
  } catch (e) {
    console.error("❌ Ping error", e);
    res.status(500).send(`Ping failed: ${e.message}`);
  }
});

// ——— 11) ingestReport helper ———
async function ingestReport(reportName, tokens) {
  const meta = REPORTS[reportName];
  if (!meta) throw new Error(`Unknown report "${reportName}"`);

  const url = `https://quickbooks.api.intuit.com/v3/company/${
    tokens.realm_id
  }/reports/${reportName}${meta.suffix}`;

  console.log(`[report] fetching ${reportName}`);
  const qbRes = await axios.get(url, {
    headers: { Authorization: `Bearer ${tokens.access_token}` }
  });

  console.log(`[report] truncating ${meta.table}`);
  await execAsync({
    sqlText: `TRUNCATE TABLE ${SF_DATABASE}.${SF_SCHEMA}.${meta.table}`
  });

  console.log(`[report] inserting into ${meta.table}`);
  await execAsync({
    sqlText: `
      INSERT INTO ${SF_DATABASE}.${SF_SCHEMA}.${meta.table} (RAW, LOADED_AT)
      SELECT PARSE_JSON(?), CURRENT_TIMESTAMP()
    `,
    binds: [ JSON.stringify(qbRes.data) ]
  });

  console.log(`[report] ${reportName} → ${meta.table} done`);
}

// ——— 12) Main report route ———
app.get("/report/:name?", async (req, res) => {
  console.log("[report] start");
  try {
    // a) DB ping
    await execAsync({ sqlText: "SELECT 1" });
    console.log("[report] db ping OK");

    // b) Refresh tokens
    const tokens = await loadTokens();
    if (!tokens.refresh_token) return res.status(401).send("Not connected.");
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type:    "refresh_token",
        refresh_token: tokens.refresh_token
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization:  `Basic ${Buffer.from(
            `${CLIENT_ID}:${CLIENT_SECRET}`
          ).toString("base64")}`
        }
      }
    );
    tokens.access_token  = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
    console.log("[report] tokens refreshed");

    const namesToRun = Object.keys(REPORTS);

    // d) run them in order
    for (const name of namesToRun) {
      await ingestReport(name, tokens);
    }

    res.send(`✅ Ingested: ${namesToRun.join(", ")}`);
  } catch (err) {
    console.error("[report] error", err);
    res.status(500).send(`Error ingesting reports: ${err.message}`);
  }
});

// ——— 13) Start server ———
app.listen(PORT, "0.0.0.0", () =>
  console.log(`Listening on http://0.0.0.0:${PORT}`)
);
