app.get("/report/:name", async (req, res) => {
  const name = req.params.name;
  if (!(name in reports)) {
    console.warn(`[report] Unsupported report name: ${name}`);
    return res.status(400).send("Unsupported report.");
  }

  // Load and validate tokens
  const tokens = await loadTokens();
  if (!tokens.refresh_token || !tokens.realm_id) {
    console.warn("[report] No tokens found—user not connected");
    return res.status(401).send("Not connected. Visit /connect first.");
  }

  // Refresh access token
  try {
    console.log("[report] Refreshing QuickBooks access token…");
    const refreshRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type:    "refresh_token",
        refresh_token: tokens.refresh_token
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${Buffer.from(
            `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET}`
          ).toString("base64")}`
        }
      }
    );
    tokens.access_token  = refreshRes.data.access_token;
    tokens.refresh_token = refreshRes.data.refresh_token;
    await saveTokens(tokens);
    console.log("[report] Token refresh successful");
  } catch (e) {
    console.error("❌ Refresh token failed", e.response?.data || e);
    return res.status(500).send("Token refresh failed.");
  }

  // Fetch report from QuickBooks
  const apiUrl = `https://quickbooks.api.intuit.com/v3/company/${tokens.realm_id}/reports/${name}${reports[name]}`;
  let qbData;
  try {
    console.log(`[report] Fetching QuickBooks report: ${name}`);
    const response = await axios.get(apiUrl, {
      headers: { Authorization: `Bearer ${tokens.access_token}` }
    });
    qbData = response.data;
    console.log("[report] QuickBooks fetch successful");
  } catch (e) {
    console.error("❌ QuickBooks fetch error", e.response?.data || e);
    return res.status(500).send("Failed to fetch report.");
  }

  // Prepare and execute Snowflake insert
  const jsonString = JSON.stringify(qbData);
  const insertSql = `
    INSERT INTO ${process.env.SF_DATABASE}.${process.env.SF_SCHEMA}.AGED_RECEIVABLES (RAW)
    VALUES (PARSE_JSON($$${jsonString}$$));
  `;
  console.log("[report] Inserting into Snowflake…");

  sfConn.execute({
    sqlText: insertSql,
    complete: (err) => {
      if (err) {
        logSfError(err, "insert");
        return res.status(500).send("Snowflake insert failed.");
      }
      console.log("[report] Snowflake insert succeeded");
      return res.send("✅ Report successfully ingested.");
    }
  });
});
