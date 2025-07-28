const express = require("express");
const axios = require("axios");
const qs = require("qs");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;

// QuickBooks OAuth credentials
const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const redirect_uri = process.env.REDIRECT_URI;

let access_token = null;
let realm_id = null;

// Home page – start OAuth flow
app.get("/", (req, res) => {
  res.send(`<a href=\"/connect\">Connect to QuickBooks</a>`);
});

// 1. Kick off OAuth
app.get("/connect", (req, res) => {
  const url =
    "https://appcenter.intuit.com/connect/oauth2?" +
    qs.stringify({
      client_id,
      response_type: "code",
      scope: "com.intuit.quickbooks.accounting",
      redirect_uri,
      state: "xyz123",
    });

  res.redirect(url);
});

// 2. Handle callback + exchange code for tokens
app.get("/callback", async (req, res) => {
  const auth_code = req.query.code;
  realm_id = req.query.realmId;

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type: "authorization_code",
        code: auth_code,
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

    access_token = tokenRes.data.access_token;
    console.log("✅ QuickBooks tokens acquired");
    res.redirect("/report");
  } catch (err) {
    console.error("Token Exchange Error:", err.response?.data || err.message);
    res.status(500).send("Error exchanging token");
  }
});

// 3. Fetch report from QuickBooks
app.get("/report", async (req, res) => {
  if (!access_token || !realm_id) return res.status(401).send("Not connected");

  try {
    const result = await axios.get(
      `https://quickbooks.api.intuit.com/v3/company/${realm_id}/reports/AgedReceivablesSummary`,
      {
        headers: {
          Authorization: `Bearer ${access_token}`,
          Accept: "application/json",
        },
      }
    );

    const payload = result.data;
    res.json(payload);
  } catch (err) {
    console.error("Report API Error:", err.response?.data || err.message);
    res.status(500).send("Error fetching report");
  }
});

// Start server
app.listen(port, () => {
  console.log(`App running on port ${port}`);
});
