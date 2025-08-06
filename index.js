// index.js
const express   = require("express");
const axios     = require("axios");
const qs        = require("qs");
const fs        = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

// ——— report→table whitelist ———
const REPORT_TABLES = {
  AgedReceivables: "AGED_RECEIVABLES",
  ItemSales:       "ITEM_SALES"
};

// always append this to every report call
const DEFAULT_DATE_PARAMS = "?start_duedate=2020-01-01&end_duedate=2030-12-30";

// ——— Snowflake helper ———
function execAsync({ sqlText, binds = [] }) {
  return new Promise((resolve, reject) => {
    sfConn.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) =>
        err ? reject(err) : resolve({ stmt, rows })
    });
  });
}

// ——— Global error handling ———
process.on("unhandledRejection", (reason, promise) =>
  console.error("Unhandled Rejection at:", promise, "reason:", reason)
);
process.on("uncaughtException", err => {
  console.error("Uncaught Exception:", err);
  process.exit(1);
});

// ——— Env vars ———
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

// ——— Connect to Snowflake ———
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

// ——— Token storage ———
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

// ——— Express setup ———
const app = express();
app.use(express.json());
app.use((req, res, next) => {
  console.log(`[req] ${req.method} ${req.url}`);
  next();
});

// ——— OAuth & home routes ———
app.get("/", (req, res) =>
  res.send('<a href="/connect">Connect to QuickBooks</a>')
);

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
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization:  `Basic ${Buffer.from(
            `${CLIENT_ID}:${CLIENT_SECRET}`
          ).toString("base64")}`
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

// ——— Snowflake ping (test) ———
app.get("/test-sf", async (req, res) => {
  try {
    const { rows } = await execAsync({ sqlText: "SELECT 1 AS ping" });
    res.json(rows);
  } catch (e) {
    console.error("❌ Ping error", e);
    res.status(500).send(`Ping failed: ${e.message}`);
  }
});

// ——— Core ingestion logic ———
async function ingestReport(reportName) {
  const tableName = REPORT_TABLES[reportName];
  if (!tableName) {
    throw new Error(`Unknown report: ${reportName}`);
  }

  // 1) DB ping
  await execAsync({ sqlText: "SELECT 1" });

  // 2) Refresh tokens
  const tokens = await loadTokens();
  if (!tokens.refresh_token) {
    throw new Error("Not connected (no refresh token).");
  }
  const refreshRes = await axios.post(
    "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
    qs.stringify({
      grant_type:     "refresh_token",
      refresh_token:  tokens.refresh_token
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

  // 3) Fetch QBO report (always with date params)
  const qbUrl = 
    `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}` +
    `/reports/${reportName}${DEFAULT_DATE_PARAMS}`;

  const qbRes = await axios.get(qbUrl, {
    headers: { Authorization: `Bearer ${tokens.access_token}` }
  });

  // 4) Truncate target table
  await execAsync({
    sqlText: `TRUNCATE TABLE ${SF_DATABASE}.${SF_SCHEMA}.${tableName}`
  });

  // 5) Insert JSON payload
  await execAsync({
    sqlText: `
      INSERT INTO ${SF_DATABASE}.${SF_SCHEMA}.${tableName} (RAW, LOADED_AT)
      SELECT PARSE_JSON(?), CURRENT_TIMESTAMP()
    `,
    binds: [JSON.stringify(qbRes.data)]
  });

  console.log(`✅ ${reportName} → ${tableName} ingested`);
}

// ——— Single-report endpoint (unchanged) ———
app.get("/report/:name", async (req, res) => {
  try {
    await ingestReport(req.params.name);
    res.send(`✅ ${req.params.name} ingested`);
  } catch (e) {
    console.error(`[report] error`, e);
    res.status(500).send(`Error ingesting ${req.params.name}: ${e.message}`);
  }
});

// ——— New: sequential multi-report endpoint ———
app.get("/reports", async (req, res) => {
  const names = Object.keys(REPORT_TABLES);
  console.log("[reports] will ingest:", names);

  try {
    for (const name of names) {
      console.log(`[reports] starting: ${name}`);
      await ingestReport(name);
      console.log(`[reports] finished: ${name}`);
    }
    console.log("[reports] all reports ingested");
    res.send(`✅ Ingested reports: ${names.join(", ")}`);
  } catch (err) {
    console.error("[reports] error", err);
    res.status(500).send(`Error ingesting reports: ${err.message}`);
  }
});

// ——— Start server ———
app.listen(PORT, "0.0.0.0", () =>
  console.log(`Listening on http://0.0.0.0:${PORT}`)
);
