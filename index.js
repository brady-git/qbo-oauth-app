const express = require("express");
const axios = require("axios");
const qs = require("qs");
const { Dropbox } = require("dropbox");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;

// QuickBooks OAuth credentials
const client_id = process.env.CLIENT_ID;
const client_secret = process.env.CLIENT_SECRET;
const redirect_uri = process.env.REDIRECT_URI;

// Dropbox client
const dbx = new Dropbox({ accessToken: process.env.DROPBOX_ACCESS_TOKEN });

let access_token = null;
let realm_id = null;

// Report registry (only AgedReceivables for now)
const reports = {
  AgedReceivables: {
    file: "/aged_receivables.json", // Dropbox path
    defaultParams: "?date_macro=LastMonth"
  }
};

// Home page
app.get("/", (req, res) => {
  res.send(`<a href="/connect">Connect to QuickBooks</a>`);
});

// Step 1 – OAuth flow
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

// Step 2 – Callback handler
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
    res.redirect("/report/AgedReceivables");
  } catch (err) {
    console.error("Token Exchange Error:", err.response?.data || err.message);
    res.status(500).send("Error exchanging token");
  }
});

// Step 3 – Report fetcher & Dropbox uploader
app.get("/report/:reportName", async (req, res) => {
  const reportName = req.params.reportName;

  if (!access_token || !realm_id) {
    return res.status(401).send("Not connected to QuickBooks.");
  }

  const report = reports[reportName];
  if (!report) {
    return res.status(400).send(`Report '${reportName}' is not supported.`);
  }

  try {
    const response = await axios.get(
      `https://quickbooks.api.intuit.com/v3/company/${realm_id}/reports/${reportName}${report.defaultParams}`,
      {
        headers: {
          Authorization: `Bearer ${access_token}`,
          Accept: "application/json",
        },
      }
    );

    const fileContent = JSON.stringify(response.data, null, 2);

    await dbx.filesUpload({
      path: report.file,
      contents: fileContent,
      mode: { ".tag": "overwrite" },
    });

    console.log(`✅ Uploaded ${reportName} to Dropbox at ${report.file}`);
    res.json({
      message: `${reportName} uploaded to Dropbox`,
      dropboxPath: report.file,
    });
  } catch (err) {
    console.error(
      `❌ ${reportName} error:`,
      JSON.stringify(err.response?.data || err.message, null, 2)
    );
    res.status(500).send(`Error fetching or uploading ${reportName}`);
  }
});

// Start server
app.listen(port, () => {
  console.log(`App running on port ${port}`);
});
