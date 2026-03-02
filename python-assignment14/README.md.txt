Robots.txt Compliance

This project is intended for educational purposes only.
Before scraping any website, make sure to review and comply with its robots.txt file.

The script includes request delays to avoid overloading the server.

https://www.baseball-almanac.com/robots.txt

Scraped structured baseball statistics from Baseball Almanac using Selenium.
Extracted dynamic tables (Player & Pitcher Reviews) while filtering heterogeneous table layouts.

The website does not use traditional pagination (no page numbers or next buttons).
Instead, each year has a separate link.
The scraper collects all year links and iterates through them to retrieve data.

## Requirements

- Python 3.8+
- pandas
- selenium
- webdriver_manager
- sqlite3 (built-in)
- plotly
- streamlit