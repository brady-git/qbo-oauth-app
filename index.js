// index.js
const express   = require("express");
const axios     = require("axios");
const qs        = require("qs");
const fs        = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

// --- Environment variables ---
const {
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI,
  SF_ACCOUNT,
  SF_USER,
  SF_PWD,
  SF_WAREHOUSE,
  SF_DATABASE,
  SF_SCHEMA,
  SF_REGION,
  SF_ROLE,
  TOKEN_PATH = "./tokens.json",
  PORT = 3000
} = process.env;

// --- Validate required environment variables ---
const requiredEnv = {
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI,
  SF_ACCOUNT,
  SF_USER,
  SF_PWD,
  SF_WAREHOUSE,
  SF_DATABASE,
  SF_SCHEMA,
  SF_REGION,
  SF_ROLE
};
Object.entries(requiredEnv).forEach(([key, value]) => {
  if (!value) {
    console.error(`❌ Missing env var: ${key}`);
    process.exit(1);
  }
});

const app = express();

// --- Simple request logger ---
app.use((req, res, next) => {
  console.log(`[req] ${req.method} ${req.url}`);
  next();
});

// --- Snowflake error logger ---
function logSfError(err, context = "connection") {
  const resp = err.response || {};
  console.error(`❌ Snowflake ${context} error:`, {
    code: err.code,
    status: resp.status,
    statusText: resp.statusText,
    url: err.config?.url
  });
}

// --- Snowflake connection ---
const sfConn = snowflake.createConnection({
  account:   SF_ACCOUNT,
  region:    SF_REGION,
  username:  SF_USER,
  password:  SF_PWD,
  warehouse: SF_WAREHOUSE,
  database:  SF_DATABASE,
  schema:    SF_SCHEMA,
  role:      SF_ROLE
});
sfConn.connect(err => {
  if (err) {
    logSfError(err, "connection");
    process.exit(1);
  }
  console.log("✅ Snowflake connection established");
});

// --- Supported reports ---
const reports = { AgedReceivables: "" };

// --- Token I/O ---
async function loadTokens() {
  try { return JSON.parse(await fs.readFile(TOKEN_PATH, "utf8")); }
  catch { return {}; }
}
async function saveTokens(tokens) {
  await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), "utf8");
}

// --- Routes ---

// Home
app.get("/", (req, res) => res.send('<a href="/connect">Connect to QuickBooks</a>'));

// OAuth start
app.get("/connect", (req, res) => {
  const params = qs.stringify({
    client_id: CLIENT_ID,
    response_type: "code",
    scope: "com.intuit.quickbooks.accounting openid",
    redirect_uri: REDIRECT_URI,
    state: "xyz123"
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
      qs.stringify({ grant_type: "authorization_code", code, redirect_uri: REDIRECT_URI }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded", Authorization: `Basic ${Buffer.from(
            `${CLIENT_ID}:${CLIENT_SECRET}`
          ).toString("base64")}` } }
    );
    const tokens = { realm_id: realmId, access_token: tokenRes.data.access_token, refresh_token: tokenRes.data.refresh_token };
    await saveTokens(tokens);
    res.send("<h1>Authentication successful.</h1>");
  } catch (e) {
    console.error("❌ Token exchange error", e.response?.data || e);
    res.status(500).send("Token exchange failed.");
  }
});

// Test Snowflake: DESCRIBE AGED_RECEIVABLES for debug
app.get("/test-sf", (req, res) => {
  sfConn.execute({
    sqlText: `DESCRIBE TABLE ${SF_DATABASE}.${SF_SCHEMA}.AGED_RECEIVABLES`,
    complete: (err, stmt, rows) => {
      if (err) return res.status(500).send(`DESCRIBE failed: ${err.message}`);
      console.log("[test-sf] DESCRIBE rows:", rows);
      res.json(rows);
    }
  });
});

// Report ingestion
app.get("/report/:name", async (req, res) => {
  console.log("[report] handler start");
  const name = req.params.name;
  if (!(name in reports)) return res.status(400).send("Unsupported report.");

  const tokens = await loadTokens();
  if (!tokens.refresh_token) return res.status(401).send("Not connected.");

  try {
    console.log("[report] Refreshing token…");
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "refresh_token", refresh_token: tokens.refresh_token }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded", Authorization: `Basic ${Buffer.from(
            `${CLIENT_ID}:${CLIENT_SECRET}`
          ).toString("base64")}` } }
    );
    tokens.access_token = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
    console.log("[report] Token refresh successful");
  } catch (e) {
    console.error("❌ Token refresh failed", e.response?.data || e);
    return res.status(500).send("Token refresh failed.");
  }

  let qbData;
  try {
    console.log(`[report] Fetching ${name}`);
    const resp = await axios.get(
      `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${name}`,
      { headers: { Authorization: `Bearer ${tokens.access_token}` } }
    );
    qbData = resp.data;
    console.log("[report] Fetch successful");
  } catch (e) {
    console.error("❌ QuickBooks fetch error", e.response?.data || e);
    return res.status(500).send("Fetch failed.");
  }

  const jsonString = JSON.stringify(qbData);
  // DEBUG: log current Snowflake context
  sfConn.execute({
    sqlText: "SELECT CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS schema, CURRENT_ROLE() AS role",
    complete: (err, stmt, rows) => {
      if (err) console.error("[report] context query error:", err.message);
      else console.log("[report] context:", rows);
    }
  });

  console.log("[report] about to insert JSON length:", jsonString.length);
  const insertSql = `INSERT INTO ${SF_DATABASE}.${SF_SCHEMA}.AGED_RECEIVABLES (RAW) SELECT PARSE_JSON(?);`;

  const stmt = sfConn.execute({
    sqlText: insertSql,
    binds: [jsonString],
    timeout: 60000,
    complete: (err, stmt) => {
      console.log("[report] insert callback fired");
      if (err) {
        logSfError(err, "insert");
        return res.status(500).send(`Insert failed: ${err.message}`);
      }
      const count = typeof stmt.getNumUpdatedRows === 'function' ? stmt.getNumUpdatedRows() : '(unknown)';
      console.log("[report] rows updated:", count);
      res.send("✅ Report ingested.");
    }
  });
  stmt.on("error", err => console.error("[report] stmt error:", err));
});

// Start server
app.listen(PORT, "0.0.0.0", () => console.log(`Listening on http://0.0.0.0:${PORT}`));
