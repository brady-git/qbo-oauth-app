const express = require("express");
const axios = require("axios");
const qs = require("qs");
const fs = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;
const TOKEN_PATH = "./tokens.json";

// OAuth credentials
const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const redirect_uri = process.env.REDIRECT_URI;

// Helper for standardized Snowflake error logging
function logSfError(err, context = "connection") {
  const resp = err.response || {};
  console.error(
    `❌ Snowflake ${context} error:`,
    {
      code: err.code,
      status: resp.status,
      statusText: resp.statusText,
      url: resp.config?.url
    }
  );
}

// Pre-flight check for required environment variables
["SF_ACCOUNT","SF_USER","SF_PWD","SF_WAREHOUSE","SF_DATABASE","SF_SCHEMA","SF_REGION"].forEach(varName => {
  if (!process.env[varName]) {
    console.error(`❌ Missing required env var: ${varName}`);
    process.exit(1);
  }
});

// Log Snowflake configuration for debugging (locator-only account, region separate)
console.log('Snowflake config:', {
  account: process.env.SF_ACCOUNT,
  region: process.env.SF_REGION,
  user: process.env.SF_USER,
  warehouse: process.env.SF_WAREHOUSE,
  database: process.env.SF_DATABASE,
  schema: process.env.SF_SCHEMA,
  role: process.env.SF_ROLE || '(default)'
});

// Snowflake connection
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

sfConn.connect((err) => {
  if (err) {
    logSfError(err, "connection");
    process.exit(1);
  }
  console.log("✅ Snowflake connection established");
});

// Supported reports
const reports = { AgedReceivables: "" };

async function loadTokens() {
  try {
    const data = await fs.readFile(TOKEN_PATH, "utf8");
    return JSON.parse(data);
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
  const url =
    "https://appcenter.intuit.com/connect/oauth2?" +
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
  if (error || !authCode) {
    return res.status(400).send("Authentication failed.");
  }
  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "authorization_code", code: authCode, redirect_uri }),
      { headers: { 
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(`${client_id}:${client_secret}`).toString("base64")}`
        }
      }
    );
    const tokens = {
      access_token: tokenRes.data.access_token,
      refresh_token: tokenRes.data.refresh_token,
      realm_id: realmId
    };
    await saveTokens(tokens);
    res.send("<h1>Authentication successful.</h1>");
  } catch {
    res.status(500).send("Token exchange failed.");
  }
});

app.get("/report/:name", async (req, res) => {
  const name = req.params.name;
  const defaultParams = reports[name];
  if (defaultParams === undefined) return res.status(400).send("Unsupported report.");

  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) return res.status(401).send("Not connected.");

  try {
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "refresh_token", refresh_token: tokens.refresh_token }),
      { headers: { 
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(`${client_id}:${client_secret}`).toString("base64")}`
        }
      }
    );
    tokens.access_token = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
  } catch {
    return res.status(500).send("Token refresh failed.");
  }

  const apiUrl = `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${name}${defaultParams}`;
  let data;
  try {
    data = (await axios.get(apiUrl, { headers: { Authorization: `Bearer ${tokens.access_token}` }})).data;
  } catch {
    return res.status(500).send("Report fetch failed.");
  }

  sfConn.execute({
    sqlText: `INSERT INTO AGED_RECEIVABLES (RAW) VALUES (PARSE_JSON(?))`,
    binds: [JSON.stringify(data)],
    complete: (err) => {
      if (err) {
        logSfError(err, "insert");
        return res.status(500).send("Snowflake load failed.");
      }
      res.send("Data loaded.");
    }
  });
});

app.listen(port, () => console.log(`Listening on port ${port}`));
