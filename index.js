const express = require("express");
const axios = require("axios");
const qs = require("qs");
const { Dropbox } = require("dropbox");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;
// Dropbox paths
const DBX_TOKEN_PATH = "/QBO_Reports/aged_receivables/tokens.json";

// OAuth credentials
const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const redirect_uri = process.env.REDIRECT_URI;

// Initialize Dropbox client
const dbx = new Dropbox({ accessToken: process.env.DROPBOX_ACCESS_TOKEN });

let access_token = null;
let realm_id = null;

const reports = {
  AgedReceivables: {
    file: "/QBO_Reports/aged_receivables/aged_receivables.json",
    defaultParams: ""
  }
};

// Load tokens.json from Dropbox
async function loadTokens() {
  try {
    const downloadRes = await dbx.filesDownload({ path: DBX_TOKEN_PATH });
    const dataStr = downloadRes.result.fileBinary.toString("utf8");
    console.log('✅ Loaded tokens from Dropbox');
    return JSON.parse(dataStr);
  } catch (err) {
    console.warn('⚠️ No token file in Dropbox or failed to download:', err.error || err);
    return {};
  }
}

// Save tokens.json to Dropbox
async function saveTokens(accessToken, refreshToken, realm) {
  const payload = { access_token: accessToken, refresh_token: refreshToken, realm_id: realm };
  await dbx.filesUpload({
    path: DBX_TOKEN_PATH,
    contents: JSON.stringify(payload),
    mode: { ".tag": "overwrite" }
  });
  console.log('✅ Saved tokens to Dropbox');
}

app.get("/", (req, res) => res.send('<a href="/connect">Connect to QuickBooks</a>'));

// Step 1 – OAuth flow
app.get("/connect", (req, res) => {
  const url = "https://appcenter.intuit.com/connect/oauth2?" + qs.stringify({
    client_id,
    response_type: "code",
    scope: "com.intuit.quickbooks.accounting openid profile email offline_access",
    redirect_uri,
    state: "xyz123",
  });
  console.log('🔗 Redirecting user to QuickBooks for consent');
  console.log('🔗 Using redirect_uri:', redirect_uri);
  res.redirect(url);
});

// Step 2 – Callback handler
app.get("/callback", async (req, res) => {
  const auth_code = req.query.code;
  realm_id = req.query.realmId;
  console.log('🔄 Received callback, code:', auth_code, 'realm:', realm_id);
  console.log('🔄 Exchanging code for tokens with redirect_uri:', redirect_uri);

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "authorization_code", code: auth_code, redirect_uri }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: "Basic " + Buffer.from(`${client_id}:${client_secret}`).toString("base64"),
        }
      }
    );

    console.log('✅ Token response:', JSON.stringify(tokenRes.data, null, 2));
    access_token = tokenRes.data.access_token;
    await saveTokens(tokenRes.data.access_token, tokenRes.data.refresh_token, realm_id);
    console.log('✅ Acquired and saved initial tokens');
    res.redirect("/report/AgedReceivables");
  } catch (err) {
    console.error('❌ Token Exchange Error:', JSON.stringify(err.response?.data || err.message, null, 2));
    res.status(500).send("Error exchanging token. Check render logs for details.");
  }
});

// Step 3 – Report fetcher & Dropbox uploader
app.get("/report/:reportName", async (req, res) => {
  const reportName = req.params.reportName;
  console.log(`📥 Cron invoked /report/${reportName}`);

  // Load tokens from Dropbox
  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) {
    console.error('❌ Missing stored tokens or realm_id');
    return res.status(401).send("Not connected to QuickBooks.");
  }
  realm_id = tokens.realm_id;

  // Refresh access token
  try {
    console.log('🔄 Refreshing QuickBooks access token');
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({ grant_type: "refresh_token", refresh_token: tokens.refresh_token }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: "Basic " + Buffer.from(`${client_id}:${client_secret}`).toString("base64"),
        }
      }
    );

    console.log('✅ Refresh token response:', JSON.stringify(refreshRes.data, null, 2));
    access_token = refreshRes.data.access_token;
    await saveTokens(refreshRes.data.access_token, refreshRes.data.refresh_token, realm_id);
    console.log('✅ Token refreshed');
  } catch (err) {
    console.error('❌ Token Refresh Error:', JSON.stringify(err.response?.data || err.message, null, 2));
    return res.status(500).send("Error refreshing QuickBooks token");
  }

  // Validate report
  const report = reports[reportName];
  if (!report) {
    console.error(`❌ Unsupported report: ${reportName}`);
    return res.status(400).send(`Report '${reportName}' is not supported.`);
  }

  // Fetch and upload
  try {
    console.log(`📊 Fetching report ${reportName}`);
    const response = await axios.get(
      `https://quickbooks.api.intuit.com/v3/company/${realm_id}/reports/${reportName}${report.defaultParams}`,
      { headers: { Authorization: `Bearer ${access_token}`, Accept: "application/json" } }
    );

    console.log('✅ Report fetched, length:', JSON.stringify(response.data).length);
    const fileContent = JSON.stringify(response.data, null, 2);

    console.log('💾 Uploading to Dropbox at', report.file);
    const uploadRes = await dbx.filesUpload({ path: report.file, contents: fileContent, mode: { ".tag": "overwrite" } });
    console.log('✅ Dropbox upload response', uploadRes.result?.id);

    res.json({ message: `${reportName} uploaded`, dropboxPath: report.file });
  } catch (err) {
    console.error('❌ Error in report flow:', JSON.stringify(err.response?.data || err.message, null, 2));
    res.status(500).send(`Error fetching or uploading ${reportName}`);
  }
});

app.listen(port, () => console.log(`App running on port ${port}`));
