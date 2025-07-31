// index.js
const express = require("express");
const axios   = require("axios");
const qs      = require("qs");
const fs      = require("fs").promises;
const util    = require("util");
const snowflake = require("snowflake-sdk");
require("dotenv").config();

const app  = express();
const port = process.env.PORT || 3000;
const TOKEN_PATH = process.env.TOKEN_PATH || "/data/tokens.json";

// OAuth credentials
const { CLIENT_ID: client_id,
        CLIENT_SECRET: client_secret,
        REDIRECT_URI: redirect_uri } = process.env;

// Helper for standardized Snowflake error logging
function logSfError(err, context = "connection") {
  const resp = err.response || {};
  console.error(
    `❌ Snowflake ${context} error:`,
    { code: err.code, status: resp.status, statusText: resp.statusText, url: resp.config?.url }
  );
}

// Preflight check
["SF_ACCOUNT","SF_USER","SF_PWD","SF_WAREHOUSE","SF_DATABASE","SF_SCHEMA","SF_REGION"]
  .forEach(varName => {
    if (!process.env[varName]) {
      console.error(`❌ Missing required env var: ${varName}`);
      process.exit(1);
    }
  });

// Supported reports
const reports = { AgedReceivables: "" };

async function loadTokens() {
  try {
    return JSON.parse(await fs.readFile(TOKEN_PATH, "utf8"));
  } catch {
    return {};
  }
}
async function saveTokens(tokens) {
  try {
    await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), "utf8");
  } catch (err) {
    console.error("❌ Error saving tokens: ", err.message);
  }
}

app.get("/", (req, res) => {
  res.send('<a href="/connect">Connect to QuickBooks</a>');
});

app.get("/connect", (req, res) => {
  const url = "https://appcenter.intuit.com/connect/oauth2?" +
    qs.stringify({
      client_id,
      response_type: "code",
      scope: "com.intuit.quickbooks.accounting openid",
      redirect_uri,
      state: "xyz123"
    });
  res.redirect(url);
});

app.get("/callback", async (req, res) => {
  const { code: authCode, realmId, error } = req.query;
  if (error || !authCode) return res.status(400).send("Authentication failed.");

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "authorization_code", code: authCode, redirect_uri }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(`${client_id}:${client_secret}`).toString("base64")}`
        }
      }
    );
    await saveTokens({
      access_token:  tokenRes.data.access_token,
      refresh_token: tokenRes.data.refresh_token,
      realm_id:      realmId
    });
    res.send("<h1>Authentication successful.</h1>");
  } catch {
    res.status(500).send("Token exchange failed.");
  }
});

app.get("/report/:name", async (req, res) => {
  const name = req.params.name;
  if (!(name in reports)) return res.status(400).send("Unsupported report.");

  // 1) QuickBooks token refresh
  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) return res.status(401).send("Not connected.");
  try {
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "refresh_token", refresh_token: tokens.refresh_token }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(`${client_id}:${client_secret}`).toString("base64")}`
        }
      }
    );
    tokens.access_token  = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
  } catch {
    return res.status(500).send("Token refresh failed.");
  }

  // 2) Fetch the report
  let data;
  try {
    const apiUrl = `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${name}${reports[name]}`;
    data = (await axios.get(apiUrl, {
      headers: { Authorization: `Bearer ${tokens.access_token}` }
    })).data;
  } catch {
    return res.status(500).send("Report fetch failed.");
  }

  // 3) Snowflake: per-request connection
  const conn = snowflake.createConnection({
    account:   process.env.SF_ACCOUNT,
    region:    process.env.SF_REGION,
    username:  process.env.SF_USER,
    password:  process.env.SF_PWD,
    warehouse: process.env.SF_WAREHOUSE,
    database:  process.env.SF_DATABASE,
    schema:    process.env.SF_SCHEMA,
    role:      process.env.SF_ROLE
  });

  // Promisify connect & destroy
  const connectAsync = util.promisify(conn.connect).bind(conn);
  const destroyAsync = util.promisify(conn.destroy).bind(conn);

  // Wrap execute in a promise
  function executeAsync(sqlText, binds) {
    return new Promise((resolve, reject) => {
      conn.execute({
        sqlText,
        binds,
        complete: (err, stmt, rows) => {
          if (err) return reject(err);
          resolve({ stmt, rows });
        }
      });
    });
  }

  try {
    await connectAsync();
  } catch (err) {
    logSfError(err, "connect");
    return res.status(500).send("Snowflake connection failed.");
  }

  try {
    await executeAsync(
      `INSERT INTO ${name.toUpperCase()} (RAW) VALUES (PARSE_JSON(?))`,
      [JSON.stringify(data)]
    );
    res.send("Data loaded.");
  } catch (err) {
    logSfError(err, "insert");
    res.status(500).send("Snowflake load failed.");
  } finally {
    try { await destroyAsync(); }
    catch (_) { /* ignore */ }
  }
});

app.listen(port, () => console.log(`Listening on port ${port}`));
