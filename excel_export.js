/**
 * Excel Export Module - Production Scheduler
 * Creates Excel files with 5 separate sheets: Input, Output, Output_2, Client_Out, Setup_Output
 * Uses SheetJS (XLSX) library for browser-based Excel generation
 */

class ExcelExporter {
    constructor() {
        this.version = '1.0.0';
    }

    /**
     * Main export function - creates Excel file with 5 sheets
     * @param {Object} scheduleData - The schedule results object with rows array
     * @param {string} filename - Optional custom filename
     */
    exportToExcel(scheduleData, filename = null) {
        try {
            if (!scheduleData || !scheduleData.rows || scheduleData.rows.length === 0) {
                throw new Error('No data to export');
            }

            // Create workbook
            const workbook = XLSX.utils.book_new();

            // Generate the five sheets in correct order
            const inputSheet = this.createInputSheet(scheduleData);
            const outputSheet = this.createOutputSheet(scheduleData.rows);
            const output2Sheet = this.createOutput2Sheet(scheduleData.rows);
            const clientOutSheet = this.createClientOutSheet(scheduleData.rows);
            const setupOutputSheet = this.createSetupOutputSheet(scheduleData.rows);

            // Add sheets to workbook in correct order
            XLSX.utils.book_append_sheet(workbook, inputSheet, "Input");
            XLSX.utils.book_append_sheet(workbook, outputSheet, "Output");
            XLSX.utils.book_append_sheet(workbook, output2Sheet, "Output_2");
            XLSX.utils.book_append_sheet(workbook, clientOutSheet, "Client_Out");
            XLSX.utils.book_append_sheet(workbook, setupOutputSheet, "Setup_Output");

            // Generate filename if not provided
            if (!filename) {
                const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
                filename = `production_schedule_${timestamp}.xlsx`;
            }

            // Export the file
            XLSX.writeFile(workbook, filename);

            return {
                success: true,
                filename: filename,
                message: 'Excel file exported successfully!'
            };

        } catch (error) {
            console.error('Excel export error:', error);
            return {
                success: false,
                error: error.message,
                message: 'Error exporting to Excel: ' + error.message
            };
        }
    }

    /**
     * Creates the main Output sheet with detailed scheduling information
     * @param {Array} rows - Array of schedule result rows
     * @returns {Object} XLSX worksheet object
     */
    createOutputSheet(rows) {
        const outputData = rows.map(row => ({
            PartNumber: row.PartNumber || '',
            Order_Quantity: row.Order_Quantity || 0,
            Priority: (row.Priority || 'normal').toLowerCase(),
            Batch_ID: row.Batch_ID || '',
            Batch_Qty: row.Batch_Qty || 0,
            OperationSeq: row.OperationSeq || 1,
            OperationName: row.OperationName || '',
            Machine: row.Machine || '',
            Person: row.Person || '',
            SetupStart: this.formatDateForOutput(row.SetupStart),
            SetupEnd: this.formatDateForOutput(row.SetupEnd),
            RunStart: this.formatDateForOutput(row.RunStart),
            RunEnd: this.formatDateForOutput(row.RunEnd),
            Timing: row.Timing || '',
            DueDate: row.DueDate || '',
            BreakdownMachine: '', // Empty as per your format
            Global_Holiday_Periods: '', // Empty as per your format
            Operator: '', // Empty as per your format
            Machine_Availability_STATUS: this.getMachineAvailabilityStatus(row.Machine)
        }));

        // Add TOTAL row at the end if there are multiple operations
        if (rows.length > 0) {
            const totalTiming = this.calculateTotalTiming(rows, rows[0].PartNumber);
            outputData.push({
                PartNumber: 'TOTAL (Timing)',
                Order_Quantity: '',
                Priority: '',
                Batch_ID: '',
                Batch_Qty: '',
                OperationSeq: '',
                OperationName: '',
                Machine: '',
                Person: '',
                SetupStart: '',
                SetupEnd: '',
                RunStart: '',
                RunEnd: '',
                Timing: totalTiming,
                DueDate: '',
                BreakdownMachine: '',
                Global_Holiday_Periods: '',
                Operator: '',
                Machine_Availability_STATUS: ''
            });
        }

        return XLSX.utils.json_to_sheet(outputData);
    }

    /**
     * Creates the Input sheet with input parameters used for scheduling
     * Matches the exact format from Input.csv
     * @param {Object} scheduleData - The schedule data object
     * @returns {Object} XLSX worksheet object
     */
    createInputSheet(scheduleData) {
        // Extract unique input parameters from the schedule data
        const inputData = [];
        
        if (scheduleData.rows && scheduleData.rows.length > 0) {
            // Group by PartNumber to get unique input parameters
            const uniqueParts = {};
            
            scheduleData.rows.forEach(row => {
                if (!uniqueParts[row.PartNumber]) {
                    uniqueParts[row.PartNumber] = {
                        PartNumber: row.PartNumber || '',
                        OperationSeq: row.OperationSeq || 1,
                        Order_Quantity: row.Order_Quantity || 0,
                        priority: (row.Priority || 'normal').toLowerCase(),
                        dueDate: row.DueDate || '',
                        breakdownMachine: row.Machine || '',
                        Breakdown_Machines_date_time: this.getBreakdownDateTime(row.Machine),
                        StartDateTime: this.getStartDateTime(row.SetupStart),
                        Holiday: this.getHolidayInfo(),
                        Setup_Availability_Window: this.getSetupWindow(),
                        Shift_1: this.getShift1(),
                        Shift_2: this.getShift2(),
                        Shift_3: this.getShift3()
                    };
                }
            });
            
            // Convert to array format
            inputData.push(...Object.values(uniqueParts));
        }
        
        // If no data, add a sample row with headers
        if (inputData.length === 0) {
            inputData.push({
                PartNumber: '',
                OperationSeq: '',
                Order_Quantity: '',
                priority: '',
                dueDate: '',
                breakdownMachine: '',
                Breakdown_Machines_date_time: '',
                StartDateTime: '',
                Holiday: '',
                Setup_Availability_Window: '',
                Shift_1: '',
                Shift_2: '',
                Shift_3: ''
            });
        }

        return XLSX.utils.json_to_sheet(inputData);
    }

    /**
     * Creates the Client_Out sheet with simplified information for clients
     * @param {Array} rows - Array of schedule result rows
     * @returns {Object} XLSX worksheet object
     */
    createClientOutSheet(rows) {
        // Group by PartNumber to avoid duplicates for clients
        const clientData = {};
        
        rows.forEach(row => {
            const partNumber = row.PartNumber;
            if (!clientData[partNumber]) {
                clientData[partNumber] = {
                    PartNumber: partNumber,
                    Order_Quantity: row.Order_Quantity || 0,
                    Timing: this.calculateTotalTiming(rows, partNumber),
                    'Start Date': this.getEarliestStartDate(rows, partNumber),
                    'Expected Delivery Date': this.getLatestEndDate(rows, partNumber)
                };
            }
        });

        const clientOutData = Object.values(clientData);
        return XLSX.utils.json_to_sheet(clientOutData);
    }

    /**
     * Creates the Setup_Output sheet focused on setup-related details
     * @param {Array} rows - Array of schedule result rows
     * @returns {Object} XLSX worksheet object
     */
    createSetupOutputSheet(rows) {
        const setupData = rows.map(row => ({
            PartNumber: row.PartNumber || '',
            Order_Quantity: row.Order_Quantity || 0,
            Batch_Qty: row.Batch_Qty || 0,
            OperationSeq: row.OperationSeq || 1,
            Machine: row.Machine || '',
            Person: row.Person || '',
            SetupStart: this.formatDateForSetup(row.SetupStart),
            SetupEnd: this.formatDateForSetup(row.SetupEnd),
            Timing: this.calculateSetupTiming(row.SetupStart, row.SetupEnd)
        }));

        return XLSX.utils.json_to_sheet(setupData);
    }

    /**
     * Creates the Output_2 sheet with simplified machine-focused information
     * Matches the exact format from Output_2.csv
     * @param {Array} rows - Array of schedule result rows
     * @returns {Object} XLSX worksheet object
     */
    createOutput2Sheet(rows) {
        const output2Data = rows.map(row => ({
            'Part Number': row.PartNumber || '',
            'Quantity': row.Order_Quantity || 0,
            'Batch Size': row.Batch_Qty || 0,
            'Date & Time': this.formatDateForOutput(row.RunStart),
            'Machine': row.Machine || '',
            'Expected Delivery Date': this.formatDateForOutput(row.RunEnd)
        }));

        return XLSX.utils.json_to_sheet(output2Data);
    }

    /**
     * Get breakdown date time for a machine
     * @param {string} machine - Machine name
     * @returns {string} Breakdown date time
     */
    getBreakdownDateTime(machine) {
        // Get breakdown data from global application settings
        if (typeof window !== 'undefined' && window.breakdowns && Array.isArray(window.breakdowns)) {
            const breakdown = window.breakdowns.find(b => b.machines && b.machines.includes(machine));
            if (breakdown) {
                return `${breakdown.startDateTime} - ${breakdown.endDateTime}`;
            }
        }
        return '';
    }

    /**
     * Get start date time from setup start
     * @param {string} setupStart - Setup start time
     * @returns {string} Start date time
     */
    getStartDateTime(setupStart) {
        if (!setupStart) return '';
        return this.formatDateForOutput(setupStart);
    }

    /**
     * Get holiday information
     * @returns {string} Holiday info
     */
    getHolidayInfo() {
        // Get holiday data from global application settings
        if (typeof window !== 'undefined' && window.holidays && Array.isArray(window.holidays)) {
            if (window.holidays.length === 0) return 'No holidays configured';
            return window.holidays.map(h => `${h.startDateTime} - ${h.endDateTime} (${h.reason || 'Holiday'})`).join('; ');
        }
        return 'No holidays configured';
    }

    /**
     * Get setup availability window
     * @returns {string} Setup window
     */
    getSetupWindow() {
        // Get setup window from global application settings
        if (typeof window !== 'undefined' && window.advancedSettings) {
            return window.advancedSettings.setupAvailabilityWindow || '06:00-22:00';
        }
        return '06:00-22:00';
    }

    /**
     * Get shift 1 information
     * @returns {string} Shift 1
     */
    getShift1() {
        // Get shift 1 from global application settings
        if (typeof window !== 'undefined' && window.advancedSettings) {
            return window.advancedSettings.shift1 || '06:00-14:00';
        }
        return '06:00-14:00';
    }

    /**
     * Get shift 2 information
     * @returns {string} Shift 2
     */
    getShift2() {
        // Get shift 2 from global application settings
        if (typeof window !== 'undefined' && window.advancedSettings) {
            return window.advancedSettings.shift2 || '14:00-22:00';
        }
        return '14:00-22:00';
    }

    /**
     * Get shift 3 information
     * @returns {string} Shift 3
     */
    getShift3() {
        // Get shift 3 from global application settings
        if (typeof window !== 'undefined' && window.advancedSettings) {
            return window.advancedSettings.prodShift3 || '22:00-06:00';
        }
        return '22:00-06:00';
    }

    /**
     * Helper function to extract breakdown machine information
     * @param {string} machine - Machine name
     * @returns {string} Breakdown machine info
     */
    extractBreakdownMachine(machine) {
        // This would typically come from your breakdown settings
        // For now, return a placeholder
        return machine ? `${machine} - Available` : 'N/A';
    }

    /**
     * Helper function to extract holiday periods
     * @returns {string} Holiday periods info
     */
    extractHolidayPeriods() {
        // This would typically come from your holiday settings
        // For now, return a placeholder
        return 'No holidays configured';
    }

    /**
     * Helper function to get machine availability status
     * @param {string} machine - Machine name
     * @returns {string} Availability status
     */
    getMachineAvailabilityStatus(machine) {
        // This matches your format: "FIXED_VALIDATED | SELECTED: VMC 1"
        return machine ? `FIXED_VALIDATED | SELECTED: ${machine}` : '';
    }

    /**
     * Format date for Output sheet (YYYY-MM-DD HH:mm format)
     * @param {string} dateTimeStr - Date time string
     * @returns {string} Formatted date
     */
    formatDateForOutput(dateTimeStr) {
        if (!dateTimeStr) return '';
        const date = this.parseDateTime(dateTimeStr);
        if (!date) return '';
        
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        const hour = String(date.getHours()).padStart(2, '0');
        const minute = String(date.getMinutes()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hour}:${minute}`;
    }

    /**
     * Format date for Setup sheet (DD-MM-YYYY HH:mm format)
     * @param {string} dateTimeStr - Date time string
     * @returns {string} Formatted date
     */
    formatDateForSetup(dateTimeStr) {
        if (!dateTimeStr) return '';
        const date = this.parseDateTime(dateTimeStr);
        if (!date) return '';
        
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        const hour = String(date.getHours()).padStart(2, '0');
        const minute = String(date.getMinutes()).padStart(2, '0');
        
        return `${day}-${month}-${year} ${hour}:${minute}`;
    }

    /**
     * Calculate total timing for a part number across all operations
     * @param {Array} rows - All schedule rows
     * @param {string} partNumber - Part number to calculate for
     * @returns {string} Total timing
     */
    calculateTotalTiming(rows, partNumber) {
        if (!rows || !Array.isArray(rows) || rows.length === 0) return '0M';
        
        const partRows = rows.filter(row => row && row.PartNumber === partNumber);
        if (partRows.length === 0) return '0M';

        const firstStart = partRows.reduce((earliest, row) => {
            if (!row || !row.SetupStart) return earliest;
            const startTime = this.parseDateTime(row.SetupStart);
            return !earliest || (startTime && startTime < earliest) ? startTime : earliest;
        }, null);

        const lastEnd = partRows.reduce((latest, row) => {
            if (!row || !row.RunEnd) return latest;
            const endTime = this.parseDateTime(row.RunEnd);
            return !latest || (endTime && endTime > latest) ? endTime : latest;
        }, null);

        if (!firstStart || !lastEnd) return '0M';

        const totalMs = lastEnd.getTime() - firstStart.getTime();
        return this.formatDuration(totalMs);
    }

    /**
     * Get earliest start date for a part number
     * @param {Array} rows - All schedule rows
     * @param {string} partNumber - Part number
     * @returns {string} Earliest start date
     */
    getEarliestStartDate(rows, partNumber) {
        if (!rows || !Array.isArray(rows) || rows.length === 0) return '';

        const partRows = rows.filter(row => row && row.PartNumber === partNumber);
        if (partRows.length === 0) return '';

        const earliestStart = partRows.reduce((earliest, row) => {
            if (!row || !row.SetupStart) return earliest;
            const startTime = this.parseDateTime(row.SetupStart);
            return !earliest || (startTime && startTime < earliest) ? startTime : earliest;
        }, null);

        return earliestStart ? this.formatDateTime(earliestStart) : '';
    }

    /**
     * Get latest end date for a part number
     * @param {Array} rows - All schedule rows
     * @param {string} partNumber - Part number
     * @returns {string} Latest end date
     */
    getLatestEndDate(rows, partNumber) {
        const partRows = rows.filter(row => row.PartNumber === partNumber);
        if (partRows.length === 0) return '';

        const latestEnd = partRows.reduce((latest, row) => {
            const endTime = this.parseDateTime(row.RunEnd);
            return !latest || endTime > latest ? endTime : latest;
        }, null);

        return latestEnd ? this.formatDateTime(latestEnd) : '';
    }

    /**
     * Calculate setup timing between start and end
     * @param {string} setupStart - Setup start time
     * @param {string} setupEnd - Setup end time
     * @returns {string} Setup timing
     */
    calculateSetupTiming(setupStart, setupEnd) {
        if (!setupStart || !setupEnd) return '0M';

        const startTime = this.parseDateTime(setupStart);
        const endTime = this.parseDateTime(setupEnd);

        if (!startTime || !endTime) return '0M';

        const totalMs = endTime.getTime() - startTime.getTime();
        return this.formatDuration(totalMs);
    }

    /**
     * Parse date time string to Date object
     * @param {string} dateTimeStr - Date time string
     * @returns {Date|null} Parsed date or null
     */
    parseDateTime(dateTimeStr) {
        if (!dateTimeStr) return null;
        try {
            return new Date(dateTimeStr);
        } catch (error) {
            console.warn('Failed to parse date:', dateTimeStr);
            return null;
        }
    }

    /**
     * Format duration in milliseconds to readable format
     * @param {number} ms - Duration in milliseconds
     * @returns {string} Formatted duration
     */
    formatDuration(ms) {
        if (!ms || ms <= 0) return '0M';
        
        let minutes = Math.round(ms / 60000);
        const days = Math.floor(minutes / 1440);
        minutes -= days * 1440;
        const hours = Math.floor(minutes / 60);
        minutes -= hours * 60;
        
        const parts = [];
        if (days > 0) parts.push(`${days}D`);
        if (hours > 0) parts.push(`${hours}H`);
        if (minutes > 0) parts.push(`${minutes}M`);
        
        return parts.join(' ') || '0M';
    }

    /**
     * Format date to readable string
     * @param {Date} date - Date object
     * @returns {string} Formatted date string
     */
    formatDateTime(date) {
        if (!date) return '';
        
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        const hour = String(date.getHours()).padStart(2, '0');
        const minute = String(date.getMinutes()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hour}:${minute}`;
    }

    /**
     * Export with custom filename
     * @param {Object} scheduleData - Schedule data
     * @param {string} customFilename - Custom filename
     * @returns {Object} Export result
     */
    exportWithCustomFilename(scheduleData, customFilename) {
        return this.exportToExcel(scheduleData, customFilename);
    }

    /**
     * Get export statistics
     * @param {Object} scheduleData - Schedule data
     * @returns {Object} Export statistics
     */
    getExportStats(scheduleData) {
        if (!scheduleData || !scheduleData.rows) {
            return { totalRows: 0, uniqueParts: 0, totalOperations: 0 };
        }

        const uniqueParts = new Set(scheduleData.rows.map(row => row.PartNumber));
        const totalOperations = scheduleData.rows.length;

        return {
            totalRows: scheduleData.rows.length,
            uniqueParts: uniqueParts.size,
            totalOperations: totalOperations,
            sheets: ['Input', 'Output', 'Output_2', 'Client_Out', 'Setup_Output']
        };
    }
}

// Browser-compatible export function
function exportToExcel(scheduleData, filename = null) {
    const exporter = new ExcelExporter();
    return exporter.exportToExcel(scheduleData, filename);
}

// Make available globally in browser
if (typeof window !== 'undefined') {
    window.ExcelExporter = ExcelExporter;
    window.exportToExcel = exportToExcel;
    
    // Debug log to confirm loading
    console.log('Excel export module loaded successfully');
    console.log('ExcelExporter available:', typeof window.ExcelExporter);
}

// Export for use in different environments
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { ExcelExporter, exportToExcel };
}
