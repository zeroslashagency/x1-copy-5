# Production Scheduling System - Google Sheets Integration

## Overview
This production scheduling system now includes Google Sheets integration for real-time routing data synchronization. Your Google Sheet acts as the master database, and the web app syncs data on-demand or automatically.

## Setup Instructions

### 1. Google Apps Script Setup
1. Open [Google Apps Script](https://script.google.com)
2. Create a new project
3. Replace the default code with the contents of `Code.gs`
4. Update the configuration variables:
   ```javascript
   const SHEET_ID = 'YOUR_SHEET_ID_HERE'; // Your Google Sheet ID
   const SHEET_NAME = 'Routing'; // Your sheet tab name
   ```
5. Deploy as Web App:
   - Click "Deploy" → "New deployment"
   - Type: "Web app"
   - Execute as: "Me"
   - Who has access: "Anyone with the link"
   - Click "Deploy" and copy the Web App URL

### 2. Google Sheet Structure
Your Google Sheet must have these exact column headers:
- `PartNumber`
- `OperationSeq`
- `OperationName`
- `SetupTime_Min`
- `Operator`
- `CycleTime_Min`
- `Minimum_BatchSize`
- `EligibleMachines`

**Note**: `EligibleMachines` should be comma-separated values (e.g., "VMC 1,VMC 2,VMC 3")

### 3. Web App Configuration
The Google Apps Script URL is already configured in the web app:
```javascript
const GOOGLE_SHEETS_API_URL = 'https://script.google.com/macros/s/AKfycbwm_sPgJPGB7MViA1jow68rb-pAZGJKqskXIQdFBnDY_0QfpI9ObiUCI8fFZmrM-qbPGQ/exec';
```

If you need to update this URL with your own deployment, edit the `GOOGLE_SHEETS_API_URL` constant in `index.html`.

## Features

### Manual Sync
- Click the "🔄 Refresh Data" button in the top-right corner
- Status indicators show sync state:
  - 📊 Local Data (using fallback data)
  - 🔄 Syncing... (sync in progress)
  - ✅ Google Sheets (successfully synced)
  - ❌ Sync Failed (error occurred)

### Auto Sync (Optional)
- Automatically syncs every 5 minutes by default
- To disable: Set `AUTO_SYNC_INTERVAL_MINUTES = 0` in `index.html`
- To change interval: Modify the value (in minutes)

### Data Validation
- Validates column headers match expected structure
- Converts data types automatically (numbers, arrays)
- Skips empty rows
- Shows detailed error messages for issues

### Fallback Behavior
- If sync fails, the app continues using local data from `data.js`
- Local data serves as backup/fallback
- No disruption to scheduling functionality

## Usage Workflow

1. **Update Google Sheet**: Add/modify routing data in your Google Sheet
2. **Sync Data**: Click "Refresh Data" or wait for auto-sync
3. **Verify Sync**: Check status indicator shows "✅ Google Sheets"
4. **Schedule Orders**: Use the updated data for production scheduling

## Troubleshooting

### Common Issues
1. **"Invalid column structure" error**: Check that your sheet has all required headers
2. **"Sheet not found" error**: Verify `SHEET_NAME` matches your sheet tab name
3. **"Sync Failed" status**: Check Google Apps Script deployment permissions
4. **No data loading**: Verify `SHEET_ID` is correct in `Code.gs`

### Testing the API
Run the `testAPI()` function in Google Apps Script to verify your setup:
1. Open your Apps Script project
2. Select `testAPI` function
3. Click "Run"
4. Check execution log for results

## Data Flow
```
Google Sheet (Master) → Apps Script API → Web App → Local Storage → Scheduling Engine
```

## Security Notes
- Google Apps Script runs with your permissions
- Web App URL is public but only reads your sheet data
- No sensitive information is exposed through the API
- All data validation happens server-side

## Support
If you encounter issues:
1. Check the browser console for error messages
2. Verify Google Apps Script execution logs
3. Ensure sheet permissions allow script access
4. Test the API endpoint directly in a browser
