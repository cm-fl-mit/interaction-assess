// Google Sheets service for saving annotation data
const { google } = require('googleapis');

class GoogleSheetsService {
  constructor() {
    this.sheets = null;
    this.spreadsheetId = process.env.GOOGLE_SHEET_ID;
    
    // Initialize Google Sheets API
    this.initializeSheets();
  }
  
  async initializeSheets() {
    try {
      // Use service account credentials from environment variable
      const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY || '{}');
      
      const auth = new google.auth.GoogleAuth({
        credentials: credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
      
      this.sheets = google.sheets({ version: 'v4', auth });
      console.log('Google Sheets API initialized');
      
    } catch (error) {
      console.error('Failed to initialize Google Sheets:', error.message);
      console.log('Will save to local database as fallback');
    }
  }
  
  async saveAnnotation(annotation) {
    if (!this.sheets || !this.spreadsheetId) {
      console.log('Google Sheets not available, skipping save');
      return false;
    }
    
    try {
      // Format data for Google Sheets
      const row = [
        annotation.participant_id,
        annotation.slice_id,
        new Date().toISOString(), // timestamp
        JSON.stringify(annotation.interaction_types),
        JSON.stringify(annotation.curiosity_types),
        annotation.annotation_time_seconds,
        JSON.stringify(annotation.routing_validation)
      ];
      
      // Append row to sheet
      const response = await this.sheets.spreadsheets.values.append({
        spreadsheetId: this.spreadsheetId,
        range: 'Sheet1!A:G', // Adjust range as needed
        valueInputOption: 'RAW',
        resource: {
          values: [row]
        }
      });
      
      console.log('Data saved to Google Sheets successfully');
      return true;
      
    } catch (error) {
      console.error('Error saving to Google Sheets:', error.message);
      return false;
    }
  }
  
  async setupHeaders() {
    if (!this.sheets || !this.spreadsheetId) {
      return false;
    }
    
    try {
      // Check if headers already exist
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: this.spreadsheetId,
        range: 'Sheet1!A1:G1'
      });
      
      // If no data, add headers
      if (!response.data.values || response.data.values.length === 0) {
        const headers = [
          'participant_id',
          'slice_id', 
          'timestamp',
          'interaction_types',
          'curiosity_types',
          'annotation_time_seconds',
          'routing_validation'
        ];
        
        await this.sheets.spreadsheets.values.update({
          spreadsheetId: this.spreadsheetId,
          range: 'Sheet1!A1:G1',
          valueInputOption: 'RAW',
          resource: {
            values: [headers]
          }
        });
        
        console.log('Headers added to Google Sheet');
      }
      
      return true;
      
    } catch (error) {
      console.error('Error setting up headers:', error.message);
      return false;
    }
  }
}

module.exports = GoogleSheetsService;