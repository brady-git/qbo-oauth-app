// index.js
const express   = require("express");
const axios     = require("axios");
const qs        = require("qs");
const fs        = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

const app        = express();
const port       = process.env.PORT || 3000;
const TOKEN_PATH = process.env.TOKEN_PATH || "./tokens.json";

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

// --- Validate env vars ---
[
  "CLIENT_ID","CLIENT_SECRET","REDIRECT_URI",
  "SF_ACCOUNT","SF_USER","SF_PWD",
  "SF_WAREHOUSE","SF_DATABASE","SF_SCHEMA","SF_REGION","SF_ROLE"
].forEach(name => {
  if (!process.env[name]) {
    console.error(`❌ Missing env var: ${name}`);
    process.exit(1);
  }
});

// --- Connect to Snowflake ---
const sfConn = snowflake.createConnection({
  account:   process.env.SF_ACCOUNT,
  region:    process.env.SF_REGION,
  username:  process.env.SF_USER,
  password:  process.env.SF_PWD,
  warehouse: process.env.SF_WAREHOUSE,
  database:  process.env.SF_DATABASE,
  schema:    process.env.SF_SCHEMA,
  role:      process.env.SF_ROLE
});
sfConn.connect(err => {
  if (err) {
    logSfError(err, "connection");
    process.exit(1);
  }
  console.log("✅ Snowflake connection established");
});

// --- Which reports are supported ---
const reports = {
  AgedReceivables: ""
};

// --- Token I/O (file-based; consider swapping to DB) ---
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

// --- Routes ---

// Home page
app.get("/", (req, res) => {
  res.send('<a href="/connect">Connect to QuickBooks</a>');
});

// Kick off OAuth flow
app.get("/connect", (req, res) => {
  const params = qs.stringify({
    client_id:     process.env.CLIENT_ID,
    response_type: "code",
    scope:         "com.intuit.quickbooks.accounting openid",
    redirect_uri:  process.env.REDIRECT_URI,
    state:         "xyz123"
  });
  res.redirect(`https://appcenter.intuit.com/connect/oauth2?${params}`);
});

// OAuth callback -> exchange code for tokens
app.get("/callback", async (req, res) => {
  const { code, realmId, error } = req.query;
  if (error || !code) return res.status(400).send("Authentication failed.");

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type:   "authorization_code",
        code,
        redirect_uri: process.env.REDIRECT_URI
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization:   `Basic ${Buffer.from(
            `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET}`
          ).toString("base64")}`
        }
      }
    );

    const tokens = {
      realm_id:      realmId,
      access_token:  tokenRes.data.access_token,
      refresh_token: tokenRes.data.refresh_token
    };
    await saveTokens(tokens);
    res.send("<h1>Authentication successful.</h1>");
  } catch (e) {
    console.error("❌ Token exchange error", e.response?.data || e);
    res.status(500).send("Token exchange failed.");
  }
});

// Quick check that sfConn.execute callbacks are firing
app.get("/test-sf", (req, res) => {
  sfConn.execute({
    sqlText: "SELECT CURRENT_TIMESTAMP() AS now",
    complete: (err, stmt, rows) => {
      if (err) {
        console.error("[test-sf] ❌", err.message);
        return res.status(500).send(`Insert failed: ${err.message}`);
      }
      console.log("[test-sf] ✅", rows);
      return res.json(rows);
    }
  });
});
