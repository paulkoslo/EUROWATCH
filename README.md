 # EU Parliament Dashboard

 A simple web application that displays a dashboard of the current Members of the European Parliament (MEPs) by country using the European Parliament Open Data API v2.

## Features
 - Fetches all active MEPs with pagination from the public API
 - Displays total MEP count
 - Renders an interactive bar chart of the top 10 countries by MEP count using Chart.js
 - Renders an interactive pie chart of MEP count by political group using Chart.js
 - Shows a full table of all MEPs with ID, Name, Country, and Political Group
 - **Speeches** tab: fetches speeches from the API, filterable by MEP, shows total speeches count and a table of speech records

 ## Installation
 ```bash
 npm install
 npm start
 ```

 Open your browser at http://localhost:3000 to view the dashboard.

 ## API Endpoints
 - GET /api/meps?lang=EN
   - `lang`: optional language code (default `EN`)
   - Returns JSON with `data` array of MEP objects.
 - GET /api/speeches?personId={id}&limit={n}
   - `personId`: optional MEP identifier to filter speeches by speaker
   - `limit`: maximum number of records (default 50)
   - Returns JSON with `data` array of speech objects and `meta.total` count.