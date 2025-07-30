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

// Snowflake connection
const sfConn = snowflake.createConnection({
  account:   process.env.SF_ACCOUNT,
  username:  process.env.SF_USER,
  password:  process.env.SF_PWD,
  warehouse: process.env.SF_WAREHOUSE,
  database:  process.env.SF_DATABASE,
  schema:    process.env.SF_SCHEMA,
  role:      process.env.SF_ROLE, // optionals
});

sfConn.connect((err) => {
  if (err) {
    console.error("❌ Snowflake connection failed:", err);
    process.exit(1);
  }
  console.log("✅ Connected to Snowflake");
});

// Report definitions
const reports = {
  AgedReceivables: { defaultParams: "" }
};

// Helper: load tokens from local file
async function loadTokens() {
  try {
    const data = await fs.readFile(TOKEN_PATH, "utf8");
    return JSON.parse(data);
  } catch (err) {
    console.warn("⚠️ No token file found:", err.message);
    return {};
  }
}

// Helper: save tokens to local file
async function saveTokens(tokens) {
  try {
    await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), "utf8");
    console.log("✅ Tokens saved");
  } catch (err) {
    console.error("❌ Failed to save tokens:", err);
  }
}

app.get("/", (req, res) => {
  res.send('<a href="/connect">Connect to QuickBooks</a>');
});

// Step 1: Redirect to QuickBooks for OAuth\ napp.get("/connect", (req, res) => {
  const authUrl = 
    "https://appcenter.intuit.com/connect/oauth2?" +
    qs.stringify({
      client_id,
      response_type: "code",
      scope: "com.intuit.quickbooks.accounting openid",
      redirect_uri,
      state: "xyz123",
    });
  console.log("🔗 Redirecting to:", authUrl);
  res.redirect(authUrl);
});

// Step 2: OAuth callback
app.get("/callback", async (req, res) => {
  const { code: authCode, realmId, error, error_description } = req.query;
  if (error) {
    console.error("❌ OAuth error:", error, error_description);
    return res.status(400).send(`OAuth Error: ${error}`);
  }
  if (!authCode) {
    console.error("❌ Missing auth code in callback");
    return res.status(400).send("Authorization code not returned.");
  }

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type: "authorization_code",
        code: authCode,
        redirect_uri,
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization:
            "Basic " +
            Buffer.from(`${client_id}:${client_secret}`).toString("base64"),
        },
      }
    );

    const tokens = {
      access_token: tokenRes.data.access_token,
      refresh_token: tokenRes.data.refresh_token,
      realm_id: realmId,
    };
    await saveTokens(tokens);
    console.log("✅ OAuth tokens acquired");
    res.redirect("/report/AgedReceivables");
  } catch (err) {
    console.error("❌ Token exchange error:", err.response?.data || err);
    res.status(500).send("Token exchange failed");
  }
});

// Step 3: Fetch report and load to Snowflake
app.get("/report/:reportName", async (req, res) => {
  const reportName = req.params.reportName;
  console.log(`📥 /report/${reportName} invoked`);

  const report = reports[reportName];
  if (!report) {
    console.error("❌ Unsupported report:", reportName);
    return res.status(400).send(`Report ${reportName} not supported.`);
  }

  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) {
    console.error("❌ Missing tokens or realm_id");
    return res.status(401).send("Not connected to QuickBooks.");
  }

  let accessToken;
  try {
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type: "refresh_token",
        refresh_token: tokens.refresh_token,
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization:
            "Basic " +
            Buffer.from(`${client_id}:${client_secret}`).toString("base64"),
        },
      }
    );
    accessToken = refreshRes.data.access_token;
    tokens.access_token = accessToken;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
    console.log("✅ Token refreshed");
  } catch (err) {
    console.error("❌ Token refresh error:", err.response?.data || err);
    return res.status(500).send("Token refresh failed");
  }

  try {
    const url = `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${reportName}${report.defaultParams}`;
    console.log("🔄 Fetching", url);
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
      },
    });

    const payload = response.data;
    sfConn.execute({
      sqlText: `INSERT INTO AGED_RECEIVABLES (RAW) VALUES (PARSE_JSON(?))`,
      binds: [JSON.stringify(payload)],
      complete: (err) => {
        if (err) {
          console.error("❌ Snowflake load error:", err);
          return res.status(500).send("Load to Snowflake failed");
        }
        console.log("✅ Loaded report to Snowflake");
        res.send("Report loaded successfully");
      },
    });
  } catch (err) {
    console.error("❌ Error fetching report:", err.response?.data || err);
    res.status(500).send("Report fetch failed");
  }
});

app.listen(port, () => {
  console.log(`App listening on port ${port}`);
});
