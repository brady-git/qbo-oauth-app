// index.js
const express = require("express");
const axios = require("axios");
const qs = require("qs");
const fs = require("fs").promises;
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;
const TOKEN_PATH = process.env.TOKEN_PATH || "./tokens.json";

const {
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI,
  QBO_API_BASE = "https://quickbooks.api.intuit.com"
} = process.env;

// 1️⃣ Route to redirect user for manual auth
app.get("/connect", (req, res) => {
  const params = qs.stringify({
    client_id: CLIENT_ID,
    response_type: "code",
    redirect_uri: REDIRECT_URI,
    scope: "com.intuit.quickbooks.accounting",
    state: "randomstate123"
  });
  res.redirect(`https://appcenter.intuit.com/connect/oauth2?${params}`);
});

// 2️⃣ OAuth2 callback: exchange code for tokens and persist to disk
app.get("/callback", async (req, res) => {
  const { code, realmId } = req.query;
  if (!code || !realmId) {
    return res.status(400).send("Missing code or realmId");
  }

  try {
    const tokenResp = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type: "authorization_code",
        code,
        redirect_uri: REDIRECT_URI
      }),
      {
        auth: { username: CLIENT_ID, password: CLIENT_SECRET },
        headers: { "Content-Type": "application/x-www-form-urlencoded" }
      }
    );
    const tokens = {
      realmId,
      access_token: tokenResp.data.access_token,
      refresh_token: tokenResp.data.refresh_token,
      expires_in: tokenResp.data.expires_in,          // seconds
      obtained_at: Date.now()
    };
    await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens, null, 2), "utf8");
    res.send(`
      <p>Authentication successful!</p>
      <p>Now visit <a href="/report?reportName=AgedReceivables">/report?reportName=YourReport</a></p>
    `);
  } catch (err) {
    console.error("Token exchange failed:", err.response?.data || err);
    res.status(500).send("OAuth token exchange failed");
  }
});

// 3️⃣ Fetch a QuickBooks report and return the JSON
app.get("/report", async (req, res) => {
  const { reportName } = req.query;
  if (!reportName) {
    return res.status(400).send("Please specify ?reportName=");
  }

  let tokens;
  try {
    tokens = JSON.parse(await fs.readFile(TOKEN_PATH, "utf8"));
  } catch {
    return res.status(400).send("No tokens found. Authenticate via /connect first.");
  }

  try {
    // If needed, check expiry and use refresh_token flow here...

    const resp = await axios.get(
      `${QBO_API_BASE}/v3/company/${tokens.realmId}/reports/${reportName}`,
      { headers: { Authorization: `Bearer ${tokens.access_token}` } }
    );
    res.json(resp.data);
  } catch (err) {
    console.error("Report fetch failed:", err.response?.data || err);
    res.status(500).send("Failed to fetch report");
  }
});

app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
});
