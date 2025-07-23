# QBO OAuth App

This is a Node.js Express application that connects to QuickBooks Online using OAuth 2.0 and fetches the AgedReceivablesSummary report.

## Setup

1. Clone the repo.
2. Run `npm install`.
3. Create a `.env` file based on `.env.example` with your Intuit app credentials.
4. Deploy to Render or your preferred platform.

## Routes

- `/connect`: Starts the OAuth flow.
- `/callback`: Handles the OAuth callback and token exchange.
- `/report`: Fetches and returns the AgedReceivablesSummary report.
