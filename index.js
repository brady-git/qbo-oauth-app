// index.js
const express = require("express");
const axios = require("axios");
const qs = require("qs");
const fs = require("fs").promises;
const snowflake = require("snowflake-sdk");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;
const TOKEN_PATH = process.env.TOKEN_PATH || "./tokens.json";

// Snowflake error logger
tfunction logSfError(err, context = "connection") {
  const resp = err.response || {};
  console.error(`❌ Snowflake ${context} error:`, {
    code: err.code,
    status: resp.status,
    statusText: resp.statusText,
    url: err.config?.url
  });
}

// Validate required env vars
["SF_ACCOUNT","SF_USER","SF_PWD","SF_WAREHOUSE","SF_DATABASE","SF_SCHEMA","SF_REGION"].forEach(name => {
  if (!process.env[name]) {
    console.error(`❌ Missing env var: ${name}`);
    process.exit(1);
  }
});

// Establish Snowflake connection
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

// Supported QuickBooks reports
const reports = {
  AgedReceivables: ""
};

// Token utilities
async function loadTokens() {
  try {
    const str = await fs.readFile(TOKEN_PATH, "utf8");
    return JSON.parse(str);
  } catch {
    return {};
  }
}
async function saveTokens(tokens) {
  await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), "utf8");
}

// Routes
app.get("/", (req, res) => {
  res.send('<a href="/connect">Connect to QuickBooks</a>');
});

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

app.get("/report/:name", async (req, res) => {
  const name = req.params.name;
  if (!(name in reports)) return res.status(400).send("Unsupported report.");

  // Load and validate tokens
  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) {
    return res.status(401).send("Not connected. Visit /connect first.");
  }

  // Refresh access token
  try {
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type:    "refresh_token",
        refresh_token: tokens.refresh_token
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
    tokens.access_token  = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
  } catch (e) {
    console.error("❌ Refresh token failed", e.response?.data || e);
    return res.status(500).send("Token refresh failed.");
  }

  // Fetch report from QuickBooks
  const apiUrl = `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${name}${reports[name]}`;
  let qbData;
  try {
    const response = await axios.get(apiUrl, {
      headers: { Authorization: `Bearer ${tokens.access_token}` }
    });
    qbData = response.data;
  } catch (e) {
    console.error("❌ QuickBooks fetch error", e.response?.data || e);
    return res.status(500).send("Failed to fetch report.");
  }

  // Compact JSON (no pretty-print)
  const jsonString = JSON.stringify(qbData);
  const insertSql = `
    INSERT INTO ${process.env.SF_DATABASE}.${process.env.SF_SCHEMA}.AGED_RECEIVABLES (RAW)
    VALUES (
      PARSE_JSON($$${jsonString}$$)
    );
  `;

  // Execute insert with dollar-quoting
  sfConn.execute({
    sqlText: insertSql,
    complete: (err) => {
      if (err) {
        logSfError(err, "insert");
        return res.status(500).send("Snowflake insert failed.");
      }
      // Optional verification
      sfConn.execute({
        sqlText: `SELECT COUNT(*) AS CNT FROM ${process.env.SF_DATABASE}.${process.env.SF_SCHEMA}.AGED_RECEIVABLES`,
        complete: (err2, stmt, rows) => {
          if (err2) logSfError(err2, "verify");
          console.log("🧮 Total rows:", rows?.[0]?.CNT);
          res.send("Report loaded successfully.");
        }
      });
    }
  });
});

app.listen(port, () => console.log(`Listening on http://localhost:${port}`));
