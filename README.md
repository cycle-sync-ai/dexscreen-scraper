# DexScreener Top Trader Scraper

A Python-based web scraping application that collects token data from DexScreener and tracks top traders. The application provides REST API endpoints to manage tokens and retrieve top trader information.

## Features

- Scrape token data from DexScreener
- Store and manage tokens in database
- Track top traders for specific tokens
- RESTful API endpoints for data access
- Configurable time periods for top trader analysis

## Tech Stack

- Python
- Quart (async web framework)
- Selenium WebDriver
- BeautifulSoup4
- Prisma (database ORM)
- Chrome WebDriver

## API Endpoints

### Tokens

- `GET /api/tokens` - Retrieve all tokens
- `POST /api/tokens` - Add new token
  ```json
  {
    "token": "TOKEN_NAME",
    "chain": "CHAIN_NAME",
    "address": "TOKEN_ADDRESS"
  }
  ```
- `DELETE /api/tokens/<token_id>` - Delete a token

### Top Traders

- `GET /api/top-traders/<token_address>` - Get top traders for a specific token
  - Query Parameters:
    - period: Time period (30d, 7d, 3d, 1d)
    - limit: Number of traders to return (max 30)

### Scraping

- `POST /api/scrape`- Trigger manual data scraping

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up Chrome WebDriver:

   The application uses ChromeDriver Manager for automatic driver installation

3. Initialize database:

   Configure Prisma connection
   Run database migrations

4. Configure environment variables:

   ```
   DATABASE_PROVIDER=
   DATABASE_URL=
   ```

5. Run the application:

   ```bash
   python main.py
   ```

## Security

WebDriver stealth mode implementation
Request validation
Error handling for API endpoints

# Author

[Github](https://github.com/bigdata5911)
[Discord](https://discord.gg/TawJX4ue)
[Email](mailto:worker.opentext@gmail.com)
