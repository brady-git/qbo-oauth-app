const express = require("express");
const axios = require("axios");
const qs = require("qs");
const app = express();
const port = process.env.PORT || 3000;

require("dotenv").config();

const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const redirect_uri = process.env.REDIRECT_URI;

let access_token = null;
let realm_id = null;

app.get("/", (req, res) => {
  res.send(`<a href="/connect">Connect to QuickBooks</a>`);
});

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
    res.redirect("/report");
  } catch (err) {
    console.error("Token Exchange Error:", err.response?.data || err.message);
    res.send("Error exchanging token");
  }
});

app.get("/report", async (req, res) => {
  if (!access_token || !realm_id) return res.send("Not connected");

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

    res.json(result.data);
  } catch (err) {
    console.error("Report API Error:", err.response?.data || err.message);
    res.send("Error fetching report");
  }
});

app.listen(port, () => {
  console.log(`App running on port ${port}`);
});