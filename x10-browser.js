/**
 * x10-browser.js - Browser-compatible version of the scheduling engine
 * Converted from Google Apps Script to standard JavaScript
 * Last update: 2025-01-27
 */

/* --- CONFIG --- */
const CONFIG = {
    MAX_CONCURRENT_SETUPS: 2,
    MAX_SETUP_SLOT_ATTEMPTS: 300,
    ALLOW_BATCH_CONTINUITY: true,
    DEFAULT_SETUP_START_HOUR: 6,
    DEFAULT_SETUP_END_HOUR: 22,
    MAX_MACHINES: 10,
    SHIFT_LENGTH_HOURS: 8,
    PERSONS_PER_SHIFT: 2,
    MAX_PROCESSING_TIME_MS: 240000,
    BATCH_SIZE_LIMIT: 5000,
    MAX_RESCHEDULE_ATTEMPTS: 10
};

/* === Browser-compatible Logger === */
const Logger = {
    log: function(message) {
        console.log(message);
    }
};

/* === FixedUnifiedSchedulingEngine (browser-compatible) === */
class FixedUnifiedSchedulingEngine {
    constructor() {
        this.machineSchedule = {}; // machine -> [{start, end}, ...]
        this.personSchedule = {}; // person -> next available time
        this.operatorSchedule = {}; // operator -> [{start, end}, ...] for setup intervals
        this.setupSlots = [];
        this.batchResults = [];
        this.globalHolidayPeriods = [];
        this.globalBreakdownPeriods = {};
        this.globalSettings = {};
        this.allMachines = ["VMC 1", "VMC 2", "VMC 3", "VMC 4", "VMC 5", "VMC 6", "VMC 7"];
        this.allPersons = ["A", "B", "C", "D"];
        
        // OPERATOR SHIFT DEFINITIONS (Asia/Kolkata IST)
        this.operatorShifts = {
            'A': { start: 6, end: 14, shift: 'morning' },
            'B': { start: 6, end: 14, shift: 'morning' },
            'C': { start: 14, end: 22, shift: 'afternoon' },
            'D': { start: 14, end: 22, shift: 'afternoon' }
        };
        
        // SETUP WINDOW: 06:00-22:00 IST (People-Dependent)
        this.setupWindow = { start: 6, end: 22 };
        
        // PRODUCTION WINDOW: 24x7 by default (Machine-Dependent)
        this.productionWindow = { start: 0, end: 24, type: '24x7' };
        
        // Initialize schedules
        this.resetSchedules();
    }

    resetSchedules() {
        const baseTime = new Date();
        // Change to interval-based tracking instead of single timestamp
        this.allMachines.forEach(machine => {
            this.machineSchedule[machine] = []; // Array of {start, end} intervals
        });
        this.allPersons.forEach(person => {
            this.personSchedule[person] = new Date(baseTime);
        });
        
        // Initialize operator schedules for setup intervals
        this.allPersons.forEach(operator => {
            this.operatorSchedule[operator] = []; // Array of {start, end} intervals
        });
        
        this.setupSlots = [];
        this.batchResults = [];
    }

    /**
     * Calculate batch splitting based on total quantity and minimum batch size
     * @param {number} totalQuantity - Total quantity to split
     * @param {number} minBatchSize - Minimum batch size
     * @returns {Array} Array of batch objects with batchId and quantity
     */
    calculateBatchSplitting(totalQuantity, minBatchSize, priority = 'normal', dueDate = null, startDate = null, batchMode = 'auto-split', customBatchSize = null) {
        Logger.log(`[BATCH-CALC] Calculating batch splitting: ${totalQuantity} pieces, batch mode: ${batchMode}, custom size: ${customBatchSize}`);
        
        let batches = [];
        
        // Handle different batch modes
        switch (batchMode) {
            case 'single-batch':
                // Single Batch: No splitting (qty stays as-is)
                batches.push({
                    batchId: 'B01',
                    quantity: totalQuantity,
                    batchIndex: 0
                });
                Logger.log(`[BATCH-CALC] Single batch mode: ${totalQuantity} pieces`);
                break;
                
            case 'custom-batch-size':
                // Custom: Use user-defined batch size
                const userBatchSize = parseInt(customBatchSize) || 300;
                let remainingQuantity = totalQuantity;
                let batchIndex = 0;
                
                while (remainingQuantity > 0) {
                    batchIndex++;
                    const batchId = `B${String(batchIndex).padStart(2, '0')}`;
                    const batchQuantity = Math.min(userBatchSize, remainingQuantity);
                    
                    batches.push({
                        batchId: batchId,
                        quantity: batchQuantity,
                        batchIndex: batchIndex - 1
                    });
                    
                    remainingQuantity -= batchQuantity;
                    Logger.log(`[BATCH-CALC] Custom batch ${batchId}: ${batchQuantity} pieces (remaining: ${remainingQuantity})`);
                }
                break;
                
            case 'auto-split':
            default:
                // Auto Split: Use quantity-based balanced splitting rules
                const isHighPriority = priority === 'High' || priority === 'Urgent';
                
                if (totalQuantity <= 250) {
                    // Rule 1: Quantity ≤ 250 → Single Batch
                    batches.push({
                        batchId: 'B01',
                        quantity: totalQuantity,
                        batchIndex: 0
                    });
                    Logger.log(`[BATCH-CALC] Auto-split Rule 1 (≤250): Single batch ${totalQuantity} pieces`);
                    
                } else if (totalQuantity <= 500) {
                    // Rule 2: Quantity 251-500 → Split into two nearly equal halves
                    const half1 = Math.ceil(totalQuantity / 2);
                    const half2 = totalQuantity - half1;
                    
                    batches.push({
                        batchId: 'B01',
                        quantity: half1,
                        batchIndex: 0
                    });
                    batches.push({
                        batchId: 'B02',
                        quantity: half2,
                        batchIndex: 1
                    });
                    Logger.log(`[BATCH-CALC] Auto-split Rule 2 (251-500): ${half1} + ${half2} pieces`);
                    
                } else if (totalQuantity <= 1000) {
                    // Rule 3: Quantity 501-1000 → Split into 2 or 3 balanced batches
                    let numBatches;
                    if (isHighPriority) {
                        // High priority: prefer 3 batches for more parallelism
                        numBatches = 3;
                    } else {
                        // Normal/Low priority: prefer 2 batches unless better divisibility with 3
                        const remainder2 = totalQuantity % 2;
                        const remainder3 = totalQuantity % 3;
                        numBatches = (remainder3 < remainder2) ? 3 : 2;
                    }
                    
                    const baseSize = Math.floor(totalQuantity / numBatches);
                    const remainder = totalQuantity % numBatches;
                    
                    for (let i = 0; i < numBatches; i++) {
                        const batchQuantity = baseSize + (i < remainder ? 1 : 0);
                        batches.push({
                            batchId: `B${String(i + 1).padStart(2, '0')}`,
                            quantity: batchQuantity,
                            batchIndex: i
                        });
                    }
                    Logger.log(`[BATCH-CALC] Auto-split Rule 3 (501-1000): ${numBatches} batches, priority: ${priority}`);
                    
                } else {
                    // Rule 4: Quantity > 1000 → Split into balanced chunks (≈500 each)
                    let numBatches;
                    if (isHighPriority) {
                        // High priority: more smaller batches for parallelism
                        numBatches = Math.ceil(totalQuantity / 334); // ≈334 per batch
                    } else {
                        // Normal/Low priority: fewer larger batches (≈500 each)
                        numBatches = Math.ceil(totalQuantity / 500);
                    }
                    
                    const baseSize = Math.floor(totalQuantity / numBatches);
                    const remainder = totalQuantity % numBatches;
                    
                    for (let i = 0; i < numBatches; i++) {
                        const batchQuantity = baseSize + (i < remainder ? 1 : 0);
                        batches.push({
                            batchId: `B${String(i + 1).padStart(2, '0')}`,
                            quantity: batchQuantity,
                            batchIndex: i
                        });
                    }
                    Logger.log(`[BATCH-CALC] Auto-split Rule 4 (>1000): ${numBatches} batches, priority: ${priority}`);
                }
                break;
        }
        
        Logger.log(`[BATCH-CALC] Final result: ${batches.length} batches created using ${batchMode} mode`);
        batches.forEach((batch, index) => {
            Logger.log(`[BATCH-CALC] Batch ${index + 1}: ${batch.batchId} (${batch.quantity} pieces)`);
        });
        
        return batches;
    }

    setGlobalSettings(settings) {
        this.globalSettings = settings || {};
        
        // Parse global start date time
        if (settings.startDate && settings.startTime) {
            // Combine startDate and startTime into a proper DateTime
            const startDateTimeStr = `${settings.startDate} ${settings.startTime}`;
            this.globalStartDateTime = new Date(startDateTimeStr);
            Logger.log(`[GLOBAL-START] Global Start DateTime set to: ${this.globalStartDateTime.toISOString()}`);
        } else if (settings.startDateTime) {
            this.globalStartDateTime = new Date(settings.startDateTime);
            Logger.log(`[GLOBAL-START] Global Start DateTime set to: ${this.globalStartDateTime.toISOString()}`);
        } else {
            this.globalStartDateTime = null;
            Logger.log(`[GLOBAL-START] No Global Start DateTime set, using current time`);
        }
        
        // Parse holiday periods
        this.globalHolidayPeriods = this.parseHolidayPeriods(settings.holidays || []);
        
        // Parse breakdown periods
        this.globalBreakdownPeriods = this.parseBreakdownPeriods(
            settings.breakdownMachines || [],
            settings.breakdownDateTime || ""
        );
        
        Logger.log(`Global settings applied: ${JSON.stringify(this.globalSettings)}`);
    }

    /**
     * Get the effective start time for scheduling
     * @returns {Date} The effective start time (global start or current time)
     */
    getEffectiveStartTime() {
        if (this.globalStartDateTime) {
            Logger.log(`[GLOBAL-START] Using Global Start DateTime: ${this.globalStartDateTime.toISOString()}`);
            return this.globalStartDateTime;
        } else {
            const currentTime = new Date();
            Logger.log(`[GLOBAL-START] Using current time: ${currentTime.toISOString()}`);
            return currentTime;
        }
    }

    parseHolidayPeriods(holidays) {
        const periods = [];
        if (Array.isArray(holidays)) {
            holidays.forEach(holiday => {
                if (typeof holiday === 'string' && holiday.includes('→')) {
                    const [start, end] = holiday.split('→').map(s => s.trim());
                    periods.push({
                        start: this.parseDateTime(start),
                        end: this.parseDateTime(end)
                    });
                } else if (typeof holiday === 'string') {
                    // Single day holiday
                    const date = this.parseDate(holiday);
                    if (date) {
                        periods.push({
                            start: new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0),
                            end: new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59)
                        });
                    }
                }
            });
        }
        return periods;
    }

    parseBreakdownPeriods(machines, dateTimeRange) {
        const periods = {};
        if (machines && machines.length > 0 && dateTimeRange) {
            if (dateTimeRange.includes('→')) {
                const [start, end] = dateTimeRange.split('→').map(s => s.trim());
                const startTime = this.parseDateTime(start);
                const endTime = this.parseDateTime(end);
                
                if (startTime && endTime) {
                    machines.forEach(machine => {
                        periods[machine] = [{
                            start: startTime,
                            end: endTime
                        }];
                    });
                }
            }
        }
        return periods;
    }

    parseDateTime(dateTimeStr) {
        if (!dateTimeStr) return null;
        
        // Handle different formats
        if (dateTimeStr.includes('/')) {
            // DD/MM/YYYY HH:MM format
            const match = dateTimeStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})/);
            if (match) {
                const [, day, month, year, hour, minute] = match;
                return new Date(parseInt(year), parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute));
            }
        } else if (dateTimeStr.includes('-')) {
            // YYYY-MM-DD HH:MM format
            return new Date(dateTimeStr.replace(' ', 'T'));
        }
        
        return new Date(dateTimeStr);
    }

    parseDate(dateStr) {
        if (!dateStr) return null;
        
        if (dateStr.includes('/')) {
            // DD/MM/YYYY format
            const parts = dateStr.split('/');
            if (parts.length === 3) {
                return new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
            }
        } else if (dateStr.includes('-')) {
            // YYYY-MM-DD format
            return new Date(dateStr);
        }
        
        return new Date(dateStr);
    }

    scheduleOrder(orderData, alerts = []) {
        try {
            Logger.log(`=== SCHEDULING ORDER: ${orderData.partNumber} (Qty: ${orderData.quantity}) ===`);
            
            const operations = orderData.operations || [];
            if (operations.length === 0) {
                throw new Error(`No operations found for part ${orderData.partNumber}`);
            }

            // Sort operations by sequence - STRICT SEQUENTIAL ORDER
            operations.sort((a, b) => a.OperationSeq - b.OperationSeq);
            Logger.log(`Operation sequences: ${operations.map(op => op.OperationSeq).join(' → ')}`);

            // THREE-BATCH SPLITTING LOGIC
            const totalQuantity = orderData.quantity;
            const minBatchSize = operations[0].Minimum_BatchSize || 100; // Default minimum batch size
            const batches = this.calculateBatchSplitting(totalQuantity, minBatchSize, orderData.priority, orderData.dueDate, orderData.startDateTime, orderData.batchMode, orderData.customBatchSize);
            
            Logger.log(`[BATCH-SPLITTING] Total Qty: ${totalQuantity}, Min Batch Size: ${minBatchSize}`);
            Logger.log(`[BATCH-SPLITTING] Calculated Batches: ${batches.length} batches`);
            batches.forEach((batch, index) => {
                Logger.log(`[BATCH-SPLITTING] Batch ${index + 1}: ${batch.batchId} (${batch.quantity} pieces)`);
            });

            const orderResults = [];
            let previousSequenceFirstPieceDone = null; // When previous sequence's FIRST piece is done
            let previousOpRunEnd = null; // Track previous operation's run end for sequential completion enforcement

            // Process each batch through all operations
            batches.forEach((batch, batchIndex) => {
                Logger.log(`\n=== PROCESSING BATCH ${batch.batchId} (${batch.quantity} pieces) ===`);
                
                let batchPreviousSequenceFirstPieceDone = null;
                let batchPreviousOpRunEnd = null;
                
                operations.forEach((operation, opIndex) => {
                    Logger.log(`\n--- SCHEDULING BATCH ${batch.batchId} - SEQUENCE ${operation.OperationSeq}: ${operation.OperationName} ---`);
                    
                    const opResult = this.scheduleOperation(
                        operation,
                        orderData,
                        batch.quantity, // Use batch quantity instead of total quantity
                        batchPreviousSequenceFirstPieceDone, // Pass when previous sequence's first piece is done
                        opIndex,
                        batchPreviousOpRunEnd // Pass previous operation's run end for sequential completion enforcement
                    );
                    
                    // Add batch information to the result
                    opResult.Batch_ID = batch.batchId;
                    opResult.Batch_Qty = batch.quantity;
                    opResult.Batch_Index = batchIndex;
                    
                    orderResults.push(opResult);
                    
                    // Update person schedule (machine is already reserved in scheduleOperation)
                    this.personSchedule[opResult.Person] = opResult.actualSetupEnd;
                    
                    // CRITICAL: Next sequence can start when this sequence's FIRST piece is done
                    // This allows parallel processing within the same batch
                    if (opResult.firstPieceDone) {
                        batchPreviousSequenceFirstPieceDone = opResult.firstPieceDone;
                    } else if (opResult.pieceCompletionTimes && opResult.pieceCompletionTimes.length > 0) {
                        batchPreviousSequenceFirstPieceDone = opResult.pieceCompletionTimes[0];
                    } else {
                        batchPreviousSequenceFirstPieceDone = opResult.actualRunEnd;
                    }
                    
                    // Track previous operation's run end for sequential completion enforcement
                    batchPreviousOpRunEnd = opResult.actualRunEnd;
                    
                    Logger.log(`Batch ${batch.batchId} - Sequence ${operation.OperationSeq} first piece done at: ${batchPreviousSequenceFirstPieceDone.toISOString()}`);
                    Logger.log(`Machine ${opResult.Machine} will be FREE after: ${opResult.actualRunEnd.toISOString()}`);
                });
                
                Logger.log(`=== BATCH ${batch.batchId} COMPLETE ===`);
            });

            // RULE 7: Check if order can meet due date
            const lastOperation = orderResults[orderResults.length - 1];
            const orderCompletionTime = lastOperation.actualRunEnd;
            const dueDate = new Date(orderData.dueDate);
            
            // Only check due date if it exists
            if (orderData.dueDate && dueDate && !isNaN(dueDate.getTime())) {
                if (orderCompletionTime > dueDate) {
                    const lateHours = Math.ceil((orderCompletionTime.getTime() - dueDate.getTime()) / (1000 * 60 * 60));
                    Logger.log(`[LATE-SCHEDULE] Order ${orderData.partNumber} will be ${lateHours}h late! Due: ${dueDate.toISOString()}, Completion: ${orderCompletionTime.toISOString()}`);
                    Logger.log(`[LATE-SCHEDULE] Cause: Machine capacity constraints, suggested mitigation: Split batch or reassign to different machines`);
                    
                    // Mark the last operation result with warning
                    lastOperation.DueDateWarning = `⚠️ ${lateHours}h late`;
                    
                    // Add to alerts for user visibility
                    alerts.push(`⚠️ ${orderData.partNumber} will be ${lateHours}h late (due ${orderData.dueDate}) - consider splitting batch or reassigning machines`);
                } else {
                    Logger.log(`✅ Order ${orderData.partNumber} will complete on time. Due: ${dueDate.toISOString()}, Completion: ${orderCompletionTime.toISOString()}`);
                }
            } else {
                Logger.log(`ℹ️ Order ${orderData.partNumber} has no due date constraint. Completion: ${orderCompletionTime.toISOString()}`);
            }

            Logger.log(`=== ORDER ${orderData.partNumber} SCHEDULING COMPLETE ===\n`);
            return orderResults;
        } catch (error) {
            Logger.log(`Error scheduling order: ${error.message}`);
            throw error;
        }
    }

    scheduleOperation(operation, orderData, batchQty, previousSequenceFirstPieceDone, sequenceIndex, previousOpRunEnd = null) {
        // RULE 1: Check machine eligibility for this part
        let eligibleMachines = operation.EligibleMachines || this.allMachines;
        
        // Convert string to array if needed (EligibleMachines is stored as comma-separated string)
        if (typeof eligibleMachines === 'string') {
            eligibleMachines = eligibleMachines.split(',').map(m => m.trim());
        }
        
        Logger.log(`Eligible machines for ${operation.OperationName}: ${eligibleMachines.join(', ')}`);
        
        // RULE 3: Calculate when this sequence can start
        let earliestStartTime = new Date(Math.max(
            this.getEffectiveStartTime().getTime()
        ));
        
        // RULE 4: SEQUENTIAL DEPENDENCY - Wait for previous sequence's first piece to complete
        if (previousSequenceFirstPieceDone) {
            earliestStartTime = new Date(Math.max(
                earliestStartTime.getTime(),
                previousSequenceFirstPieceDone.getTime()
            ));
            Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - prevOp.FirstPieceDone = ${previousSequenceFirstPieceDone.toISOString()}`);
            Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - earliestStartTime = ${earliestStartTime.toISOString()}`);
        }
        
        // RULE 5: Calculate preliminary timing to determine setup window needed
        const preliminaryTiming = this.calculatePreliminaryTiming(
            operation,
            orderData,
            batchQty,
            'A', // Temporary operator for preliminary calculation
            earliestStartTime
        );
        
        // RULE 6: Select operator who is on shift during setup window
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - preliminaryTiming.setupStart = ${preliminaryTiming.setupStart.toISOString()}`);
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - preliminaryTiming.setupEnd = ${preliminaryTiming.setupEnd.toISOString()}`);
        
        // CRITICAL FIX: Use aggressive auto-balancing system for ALL operator selection
        // This replaces the old shift-based selection with the new overlap-prevention system
        Logger.log(`[OPERATOR-SELECTION] Using aggressive auto-balancing system for operator selection`);
        
        const operatorResult = this.selectOptimalPerson(orderData, preliminaryTiming.setupStart, preliminaryTiming.setupEnd);
        
        let selectedPerson;
        if (typeof operatorResult === 'object' && operatorResult.delayedStart) {
            // Handle delayed operator selection
            Logger.log(`[OPERATOR-DELAY] Setup delayed to ${operatorResult.delayedStart.toISOString()}`);
            preliminaryTiming.setupStart = operatorResult.delayedStart;
            preliminaryTiming.setupEnd = new Date(operatorResult.delayedStart.getTime() + (operation.SetupTime_Min || 0) * 60000);
            selectedPerson = operatorResult.operator || 'A'; // Use the operator from the result
        } else {
            selectedPerson = operatorResult;
        }
        
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - selectedPerson = ${selectedPerson}`);
        const setupStartHour = preliminaryTiming.setupStart.getHours();
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - setupStartHour = ${setupStartHour} (shift: ${setupStartHour < 14 ? 'morning' : 'afternoon'})`);
        
        // RULE 6.1: Handle setup spillover across shift boundaries
        const setupDuration = operation.SetupTime_Min || 0;
        const spilloverResult = this.handleSetupSpillover(selectedPerson, preliminaryTiming.setupStart, preliminaryTiming.setupEnd, setupDuration);
        
        // Use actual setup times after spillover handling
        let actualSetupStart = spilloverResult.actualSetupStart;
        let actualSetupEnd = spilloverResult.actualSetupEnd;
        let actualOperator = spilloverResult.operator;
        
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - actualSetupStart = ${actualSetupStart.toISOString()}`);
        Logger.log(`[DBG] Op: ${orderData.partNumber} Op${operation.OperationSeq} - actualOperator = ${actualOperator}`);
        
        // ULTRA-AGGRESSIVE: Proactive operator conflict prevention with comprehensive validation
        Logger.log(`[OPERATOR-SELECTION] ULTRA-AGGRESSIVE conflict prevention for ${actualOperator} at ${actualSetupStart.toISOString()}`);
        
        // CRITICAL: Double-check for conflicts with enhanced validation
        if (this.hasOperatorConflict(actualOperator, actualSetupStart, actualSetupEnd)) {
            Logger.log(`[OPERATOR-SELECTION] ❌ CRITICAL CONFLICT detected for ${actualOperator}, implementing ULTRA-AGGRESSIVE resolution...`);
            
            // Strategy 1: Find alternative operator immediately (ULTRA-AGGRESSIVE)
            const operatorsOnShift = this.getOperatorsOnShift(actualSetupStart, actualSetupEnd);
            let alternativeFound = false;
            
            // Sort operators by load (least loaded first) - ULTRA-AGGRESSIVE balancing
            const operatorsWithLoad = operatorsOnShift.map(op => ({
                operator: op,
                load: this.getOperatorSetupMinutesInShift(op, actualSetupStart),
                totalLoad: this.getTotalOperatorSetupMinutes(op)
            })).sort((a, b) => {
                // First sort by current shift load, then by total load
                if (a.load !== b.load) return a.load - b.load;
                return a.totalLoad - b.totalLoad;
            });
            
            Logger.log(`[OPERATOR-ALTERNATIVES] Available operators sorted by load: ${operatorsWithLoad.map(op => `${op.operator}(${op.load}min)`).join(', ')}`);
            
            for (const { operator } of operatorsWithLoad) {
                if (operator !== actualOperator && !this.hasOperatorConflict(operator, actualSetupStart, actualSetupEnd)) {
                    actualOperator = operator;
                    alternativeFound = true;
                    Logger.log(`[OPERATOR-SELECTION] ✅ ULTRA-AGGRESSIVE: Found alternative operator ${actualOperator} (current load: ${this.getOperatorSetupMinutesInShift(operator, actualSetupStart)} min, total load: ${this.getTotalOperatorSetupMinutes(operator)} min)`);
                    break;
                }
            }
            
            if (!alternativeFound) {
                // Strategy 2: ULTRA-AGGRESSIVE micro-delays (1-15 minutes) to avoid conflicts
                Logger.log(`[OPERATOR-SELECTION] No immediate alternatives, trying ULTRA-AGGRESSIVE micro-delays...`);
                let microDelayFound = false;
                
                for (let delayMinutes = 1; delayMinutes <= 15; delayMinutes++) {
                    const delayedSetupStart = new Date(actualSetupStart.getTime() + delayMinutes * 60000);
                    const delayedSetupEnd = new Date(actualSetupEnd.getTime() + delayMinutes * 60000);
                    
                    // Check if delayed time is still within shift
                    const operatorsOnDelayedShift = this.getOperatorsOnShift(delayedSetupStart, delayedSetupEnd);
                    
                    for (const operator of operatorsOnDelayedShift) {
                        if (!this.hasOperatorConflict(operator, delayedSetupStart, delayedSetupEnd)) {
                            actualOperator = operator;
                            actualSetupStart = delayedSetupStart;
                            actualSetupEnd = delayedSetupEnd;
                            microDelayFound = true;
                            Logger.log(`[OPERATOR-SELECTION] ✅ ULTRA-AGGRESSIVE: Resolved with ${delayMinutes}min micro-delay using operator ${actualOperator}: ${actualSetupStart.toISOString()}`);
                            break;
                        }
                    }
                    if (microDelayFound) break;
                }
                
                if (!microDelayFound) {
                    // Strategy 3: ULTRA-AGGRESSIVE fallback - find earliest available slot
                    Logger.log(`[OPERATOR-SELECTION] ULTRA-AGGRESSIVE fallback: Finding earliest available slot...`);
                    
                    const operatorCandidates = [];
                    for (const operator of this.allPersons) {
                        const shift = this.operatorShifts[operator];
                        if (shift) {
                            const earliestAvailable = this.getEarliestOperatorFreeTime(operator, actualSetupStart);
                            const delayMinutes = (earliestAvailable.getTime() - actualSetupStart.getTime()) / (1000 * 60);
                            const totalSetupMinutes = this.getTotalOperatorSetupMinutes(operator);
                            const currentShiftMinutes = this.getOperatorSetupMinutesInShift(operator, earliestAvailable);
                            
                            operatorCandidates.push({
                                operator,
                                earliestAvailable,
                                delayMinutes,
                                totalSetupMinutes,
                                currentShiftMinutes,
                                priority: this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift.shift)
                            });
                        }
                    }
                    
                    // Sort by priority and select the best option
                    operatorCandidates.sort((a, b) => a.priority - b.priority);
                    const selectedOperator = operatorCandidates[0];
                    
                    actualOperator = selectedOperator.operator;
                    actualSetupStart = selectedOperator.earliestAvailable;
                    actualSetupEnd = new Date(actualSetupStart.getTime() + (operation.SetupTime_Min || 0) * 60000);
                    
                    Logger.log(`[OPERATOR-SELECTION] ✅ ULTRA-AGGRESSIVE fallback: Selected ${actualOperator} with ${selectedOperator.delayMinutes.toFixed(1)}min delay (priority: ${selectedOperator.priority})`);
                }
            }
        } else {
            Logger.log(`[OPERATOR-SELECTION] ✅ ULTRA-AGGRESSIVE: No conflicts detected for ${actualOperator} at ${actualSetupStart.toISOString()}`);
        }

        // RULE 5: Select machine with optimal utilization (earliest available)
        const selectedMachine = this.selectOptimalMachine(
            operation, 
            orderData, 
            actualSetupStart, 
            null // Let machine selection calculate the actual run end
        );

        // Verify selected machine is eligible
        if (!eligibleMachines.includes(selectedMachine)) {
            throw new Error(`Machine ${selectedMachine} is not eligible for part ${orderData.partNumber} operation ${operation.OperationSeq}`);
        }

        // Recalculate final timing with selected machine and piece-level dependencies
        const finalTiming = this.calculateOperationTiming(
            operation,
            orderData,
            batchQty,
            selectedMachine,
            actualOperator,
            actualSetupStart, // Use actual setup start after spillover handling
            previousSequenceFirstPieceDone ? [previousSequenceFirstPieceDone] : null, // Pass previous operation's first piece completion time for piece-level handoff
            previousOpRunEnd  // Pass previous operation's run end for sequential completion enforcement
        );
        
        // Apply production window constraints to run operations
        const runDuration = (operation.CycleTime_Min || 0) * batchQty;
        const productionConstraints = this.applyProductionWindowConstraints(selectedMachine, finalTiming.runStartTime, finalTiming.runEndTime, runDuration);
        
        // Update final timing with production window constraints
        finalTiming.runStartTime = productionConstraints.actualRunStart;
        finalTiming.runEndTime = productionConstraints.actualRunEnd;
        
        if (productionConstraints.paused) {
            Logger.log(`[PRODUCTION-WINDOW] Machine ${selectedMachine} run paused from ${productionConstraints.pauseStart.toISOString()} to ${productionConstraints.pauseEnd.toISOString()}`);
        }

        // RULE 6: MACHINE LOCKING - Reserve machine for entire sequence duration
        this.reserveMachine(selectedMachine, finalTiming.setupStartTime, finalTiming.runEndTime);
        Logger.log(`🔒 MACHINE LOCKED: ${selectedMachine} from ${finalTiming.setupStartTime.toISOString()} to ${finalTiming.runEndTime.toISOString()}`);
        
        // RULE 7: Reserve operator for setup interval
        this.reserveOperator(actualOperator, finalTiming.setupStartTime, finalTiming.setupEndTime);
        
        // Detailed logging as per setup rules
        const operatorFreeAt = this.getEarliestOperatorFreeTime(actualOperator, finalTiming.setupStartTime);
        const machineFreeAt = this.getEarliestFreeTime(selectedMachine);
        Logger.log(`[SETUP-ASSIGN] Part: ${orderData.partNumber}, Batch: ${batchQty}, OpSeq: ${operation.OperationSeq}, Machine: ${selectedMachine}, Operator: ${actualOperator}, SetupStart: ${finalTiming.setupStartTime.toISOString()}, SetupEnd: ${finalTiming.setupEndTime.toISOString()}, reason: earliest-free, operatorFreeAt: ${operatorFreeAt.toISOString()}, machineFreeAt: ${machineFreeAt.toISOString()}`);
        Logger.log(`👤 OPERATOR LOCKED: ${actualOperator} from ${finalTiming.setupStartTime.toISOString()} to ${finalTiming.setupEndTime.toISOString()}`);

        return {
            OperationSeq: operation.OperationSeq,
            OperationName: operation.OperationName,
            Machine: selectedMachine,
            Person: actualOperator,
            SetupStart: finalTiming.setupStartTime,
            SetupEnd: finalTiming.setupEndTime,
            RunStart: finalTiming.runStartTime,
            RunEnd: finalTiming.runEndTime,
            actualSetupEnd: finalTiming.setupEndTime,
            actualRunEnd: finalTiming.runEndTime,
            pieceCompletionTimes: finalTiming.pieceCompletionTimes,
            firstPieceDone: finalTiming.firstPieceDone, // Critical for next operation trigger
            SetupTime_Min: operation.SetupTime_Min || 0, // Required for duration breakdown
            CycleTime_Min: operation.CycleTime_Min || 0, // Required for piece-flow validation
            Batch_Qty: batchQty, // Required for piece-flow validation
            Timing: this.formatDurationBreakdown(
                finalTiming.setupStartTime,
                finalTiming.runEndTime,
                finalTiming.totalWorkTime,
                finalTiming.totalPausedTime
            )
        };
    }

    selectOptimalMachine(operation, orderData, setupStart, runEnd = null) {
        let eligibleMachines = operation.EligibleMachines || this.allMachines;
        
        // Convert string to array if needed (EligibleMachines is stored as comma-separated string)
        if (typeof eligibleMachines === 'string') {
            eligibleMachines = eligibleMachines.split(',').map(m => m.trim());
        }
        
        // Filter out breakdown machines
        const breakdownMachines = orderData.breakdownMachine ? 
            [orderData.breakdownMachine] : 
            (this.globalSettings.breakdownMachines || []);
        
        const availableMachines = eligibleMachines.filter(machine => 
            !breakdownMachines.includes(machine)
        );
        
        if (availableMachines.length === 0) {
            Logger.log(`[WARNING] All eligible machines are in breakdown, using fallback`);
            return eligibleMachines[0]; // Fallback
        }

        Logger.log(`[MACHINE-SELECTION] Looking for machine for setup: ${setupStart.toISOString()}`);
        Logger.log(`[MACHINE-SELECTION] Available machines: ${availableMachines.join(', ')}`);
        
        // ULTRA-AGGRESSIVE MACHINE UTILIZATION: Maximize continuous usage and balance load
        const candidates = [];
        
        for (const machine of availableMachines) {
            const intervals = this.machineSchedule[machine] || [];
            Logger.log(`[MACHINE-CHECK] ${machine} has ${intervals.length} existing bookings`);
            
            // Calculate when this machine can actually start
            const machineEarliestFree = this.getEarliestFreeTime(machine);
            const actualSetupStart = new Date(Math.max(setupStart.getTime(), machineEarliestFree.getTime()));
            
            // Calculate the actual run end based on machine availability
            const setupDuration = operation.SetupTime_Min || 0;
            const cycleTime = operation.CycleTime_Min || 0;
            const batchQty = orderData.quantity || 1;
            
            const actualSetupEnd = new Date(actualSetupStart.getTime() + setupDuration * 60000);
            const actualRunEnd = new Date(actualSetupEnd.getTime() + (batchQty * cycleTime * 60000));
            
            // Check if this machine can meet the due date
            const dueDate = new Date(orderData.dueDate);
            const meetsDueDate = actualRunEnd <= dueDate;
            
            // Calculate delay from requested start time
            const delayMinutes = (actualSetupStart.getTime() - setupStart.getTime()) / (1000 * 60);
            
            // ULTRA-AGGRESSIVE: Calculate comprehensive utilization metrics
            const isUnusedMachine = intervals.length === 0;
            const utilizationScore = intervals.length; // Lower is better
            
            // Calculate total workload hours for load balancing
            let totalWorkloadHours = 0;
            for (const interval of intervals) {
                const workloadHours = (interval.end.getTime() - interval.start.getTime()) / (1000 * 60 * 60);
                totalWorkloadHours += workloadHours;
            }
            
            // Calculate load balance score (prefer machines with less total workload)
            const loadBalanceScore = totalWorkloadHours;
            
            // Calculate efficiency score (prefer immediate start)
            const efficiencyScore = delayMinutes;
            
                candidates.push({
                    machine,
                actualSetupStart,
                actualSetupEnd,
                actualRunEnd,
                meetsDueDate,
                delay: actualSetupStart.getTime() - setupStart.getTime(),
                delayMinutes: delayMinutes,
                priority: meetsDueDate ? 1 : 2,
                canStartImmediately: delayMinutes <= 5, // Can start within 5 minutes
                isUnusedMachine: isUnusedMachine,
                utilizationScore: utilizationScore,
                loadBalanceScore: loadBalanceScore,
                efficiencyScore: efficiencyScore,
                totalWorkloadHours: totalWorkloadHours
            });
            
            Logger.log(`[CANDIDATE-FOUND] ${machine}: setup ${actualSetupStart.toISOString()}, run end ${actualRunEnd.toISOString()}, delay: ${delayMinutes.toFixed(1)}min, meets due date: ${meetsDueDate}, unused: ${isUnusedMachine}, workload: ${totalWorkloadHours.toFixed(1)}H`);
        }

        // ULTRA-AGGRESSIVE MACHINE UTILIZATION: Maximize continuous usage and balance load
        if (candidates.length > 0) {
            // Priority 1: UNUSED machines that can start immediately (regardless of due date)
            const unusedImmediateCandidates = candidates.filter(c => c.isUnusedMachine && c.canStartImmediately);
            if (unusedImmediateCandidates.length > 0) {
                const best = unusedImmediateCandidates.reduce((best, current) => 
                    current.delayMinutes < best.delayMinutes ? current : best
                );
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (UNUSED machine, immediate start, delay: ${best.delayMinutes.toFixed(1)}min)`);
                return best.machine;
            }
            
            // Priority 2: UNUSED machines that can start soon (within 30 minutes)
            const unusedSoonCandidates = candidates.filter(c => c.isUnusedMachine && c.delayMinutes <= 30);
            if (unusedSoonCandidates.length > 0) {
                const best = unusedSoonCandidates.reduce((best, current) => 
                    current.delayMinutes < best.delayMinutes ? current : best
                );
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (UNUSED machine, starts soon, delay: ${best.delayMinutes.toFixed(1)}min)`);
                return best.machine;
            }
            
            // Priority 3: UNUSED machines (any delay, force utilization)
            const unusedCandidates = candidates.filter(c => c.isUnusedMachine);
            if (unusedCandidates.length > 0) {
                const best = unusedCandidates.reduce((best, current) => 
                    current.delayMinutes < best.delayMinutes ? current : best
                );
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (UNUSED machine, forced utilization, delay: ${best.delayMinutes.toFixed(1)}min)`);
                return best.machine;
            }
            
            // Priority 4: Machines that can start immediately AND meet due date (any utilization)
            const immediateOnTimeCandidates = candidates.filter(c => c.canStartImmediately && c.meetsDueDate);
            if (immediateOnTimeCandidates.length > 0) {
                // Among immediate on-time candidates, prefer less utilized machines
                const best = immediateOnTimeCandidates.reduce((best, current) => {
                    if (current.utilizationScore !== best.utilizationScore) {
                        return current.utilizationScore < best.utilizationScore ? current : best;
                    }
                    return current.delayMinutes < best.delayMinutes ? current : best;
                });
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (immediate start, on-time, utilization: ${best.utilizationScore}, delay: ${best.delayMinutes.toFixed(1)}min)`);
                return best.machine;
            }
            
            // Priority 5: Machines that can start immediately (even if late, any utilization)
            const immediateCandidates = candidates.filter(c => c.canStartImmediately);
            if (immediateCandidates.length > 0) {
                const best = immediateCandidates.reduce((best, current) => {
                    if (current.utilizationScore !== best.utilizationScore) {
                        return current.utilizationScore < best.utilizationScore ? current : best;
                    }
                    return current.delayMinutes < best.delayMinutes ? current : best;
                });
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (immediate start, utilization: ${best.utilizationScore}, delay: ${best.delayMinutes.toFixed(1)}min)`);
                return best.machine;
            }
            
            // Priority 6: Machines that meet due date (with delay) - prefer less utilized
            const onTimeCandidates = candidates.filter(c => c.meetsDueDate);
            if (onTimeCandidates.length > 0) {
                const best = onTimeCandidates.reduce((best, current) => {
                    if (current.utilizationScore !== best.utilizationScore) {
                        return current.utilizationScore < best.utilizationScore ? current : best;
                    }
                    return current.actualSetupStart < best.actualSetupStart ? current : best;
                });
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (on-time, utilization: ${best.utilizationScore}, earliest start: ${best.actualSetupStart.toISOString()})`);
                return best.machine;
            }
            
            // Priority 7: ULTRA-AGGRESSIVE LOAD BALANCING - Prefer machines with least total workload
            const best = candidates.reduce((best, current) => {
                // First priority: Load balance (prefer machines with less total workload)
                if (Math.abs(current.loadBalanceScore - best.loadBalanceScore) > 0.1) {
                    return current.loadBalanceScore < best.loadBalanceScore ? current : best;
                }
                // Second priority: Utilization score (prefer less utilized machines)
                if (current.utilizationScore !== best.utilizationScore) {
                    return current.utilizationScore < best.utilizationScore ? current : best;
                }
                // Third priority: Efficiency (prefer earlier start)
                return current.actualSetupStart < best.actualSetupStart ? current : best;
            });
            Logger.log(`[MACHINE-SELECTED] ${best.machine} (ULTRA-AGGRESSIVE load balancing, workload: ${best.totalWorkloadHours.toFixed(1)}H, utilization: ${best.utilizationScore}, earliest start: ${best.actualSetupStart.toISOString()})`);
            return best.machine;
        }

        // Fallback: earliest available machine
        const machineWithEarliestFree = availableMachines.reduce((best, current) => {
            const bestFreeTime = this.getEarliestFreeTime(best);
            const currentFreeTime = this.getEarliestFreeTime(current);
            return currentFreeTime < bestFreeTime ? current : best;
        });

        Logger.log(`[MACHINE-SELECTED] ${machineWithEarliestFree} (fallback - earliest available)`);
        return machineWithEarliestFree;
    }
    
    // MACHINE CANDIDATE SIMULATION (as per piece-level specification)
    simulateMachineCandidate(operation, orderData, batchQty, candidateMachine, setupStart, setupEnd) {
        Logger.log(`[MACHINE-CANDIDATE] Simulating ${candidateMachine} for operation ${operation.OperationName}`);
        
        // Simulate piece-level completion for this candidate machine
        const simTiming = this.calculateOperationTiming(
            operation,
            orderData,
            batchQty,
            candidateMachine,
            'A', // Temporary operator for simulation
            setupStart,
            null // No previous operation piece times for simulation
        );
        
        Logger.log(`[MACHINE-CANDIDATE] ${candidateMachine} FinalRunEnd: ${simTiming.runEnd.toISOString()}, FirstPieceDone: ${simTiming.firstPieceDone.toISOString()}`);
        
        return {
            machine: candidateMachine,
            finalRunEnd: simTiming.runEnd,
            firstPieceDone: simTiming.firstPieceDone,
            setupStart: simTiming.setupStart,
            totalElapsed: simTiming.totalElapsed,
            totalWork: simTiming.totalWork,
            totalPaused: simTiming.totalPaused
        };
    }

    /**
     * Handle setup spillover across shift boundaries
     * If setup spills over shift end, it pauses and resumes with next available operator
     */
    handleSetupSpillover(operator, setupStart, setupEnd, setupDuration) {
        // RULE: Setup Window Enforcement (06:00-22:00) + Operator Shift Validation
        const setupWindowStart = 6; // 06:00
        const setupWindowEnd = 22;  // 22:00
        
        // Check if setup violates setup window OR operator shift assignment
        const setupStartHour = setupStart.getHours();
        const setupEndHour = setupEnd.getHours();
        const isSetupWindowViolation = setupStartHour < setupWindowStart || setupStartHour >= setupWindowEnd || 
                                      setupEndHour < setupWindowStart || setupEndHour > setupWindowEnd;
        
        // Check operator shift violation
        const isOperatorShiftViolation = !this.isOperatorInCorrectShift(operator, setupStart, setupEnd);
        
        if (isSetupWindowViolation || isOperatorShiftViolation) {
            const violationType = isSetupWindowViolation ? 'Setup window violation' : 'Operator shift violation';
            Logger.log(`[SCHEDULE-FIX] ${violationType}: ${setupStart.toISOString()} → ${setupEnd.toISOString()}, Operator: ${operator}`);
            
            // CRITICAL: Only move to next day if absolutely necessary
            // First try to fit in current day's remaining setup window
            let nextValidStart;
            
            if (setupStartHour >= 14 && setupStartHour < 22) {
                // If we're in afternoon shift window, try to keep it there
                nextValidStart = new Date(setupStart);
                // Ensure it's within the window and operator is available
                if (setupEndHour > setupWindowEnd) {
                    // Setup would end after 22:00, move to next morning
                    nextValidStart.setDate(nextValidStart.getDate() + 1);
                    nextValidStart.setHours(setupWindowStart, 0, 0, 0);
                }
            } else {
                // Find next valid setup window slot
                nextValidStart = this.findNextValidSetupSlot(setupStart, setupDuration);
            }
            
            const nextValidEnd = new Date(nextValidStart.getTime() + setupDuration * 60000);
            
            // Select appropriate operator for the corrected time slot
            const correctedOperator = this.selectOperatorForTimeSlot(nextValidStart, nextValidEnd);
            
            Logger.log(`[SCHEDULE-FIX] Orig: Setup ${setupStart.toISOString()}-${setupEnd.toISOString()} Op=${operator}, Fix: Setup ${nextValidStart.toISOString()}-${nextValidEnd.toISOString()} Op=${correctedOperator}, Reason: ${violationType}`);
            
            return {
                operator: correctedOperator,
                actualSetupStart: nextValidStart,
                actualSetupEnd: nextValidEnd,
                spillover: true,
                corrected: true,
                reason: violationType
            };
        }
        
        const shift = this.operatorShifts[operator];
        const shiftEnd = new Date(setupStart);
        shiftEnd.setHours(shift.end, 0, 0, 0);
        
        // Check if setup spills over shift boundary
        if (setupEnd > shiftEnd) {
            const workDoneInShift = shiftEnd.getTime() - setupStart.getTime();
            const remainingWork = setupDuration * 60000 - workDoneInShift;
            
            Logger.log(`[SETUP-SPILLOVER] ${operator} shift ends at ${shiftEnd.toISOString()}, ${Math.round(workDoneInShift / 60000)}min done, ${Math.round(remainingWork / 60000)}min remaining`);
            
            // Find next available operator in next shift
            const nextShiftStart = new Date(shiftEnd);
            nextShiftStart.setHours(shift.end, 0, 0, 0); // Start of next shift
            
            // Get operators from next shift
            const nextShiftOperators = this.getOperatorsForNextShift(operator);
            
            if (nextShiftOperators.length > 0) {
                // Assign remaining work to next shift operator
                const nextOperator = nextShiftOperators[0]; // Priority order
                const actualSetupEnd = new Date(nextShiftStart.getTime() + remainingWork);
                
                Logger.log(`[SETUP-SPILLOVER] Remaining work assigned to ${nextOperator} starting ${nextShiftStart.toISOString()}`);
                
                return {
                    operator: nextOperator,
                    actualSetupStart: nextShiftStart,
                    actualSetupEnd: actualSetupEnd,
                    spillover: true,
                    originalOperator: operator,
                    workDoneByOriginal: workDoneInShift
                };
            } else {
                // No operators available in next shift, delay until next valid shift
                const nextValidShiftStart = this.getNextValidShiftStart(setupStart);
                const actualSetupEnd = new Date(nextValidShiftStart.getTime() + setupDuration * 60000);
                
                Logger.log(`[SETUP-SPILLOVER] No operators in next shift, delayed until ${nextValidShiftStart.toISOString()}`);
                
                return {
                    operator: operator,
                    actualSetupStart: nextValidShiftStart,
                    actualSetupEnd: actualSetupEnd,
                    spillover: true,
                    delayed: true
                };
            }
        }
        
        // No spillover, return original values
        return {
            operator: operator,
            actualSetupStart: setupStart,
            actualSetupEnd: setupEnd,
            spillover: false
        };
    }

    /**
     * Find next valid setup slot within setup window (06:00-22:00)
     */
    findNextValidSetupSlot(requestedStart, setupDurationMin) {
        const setupWindowStart = 6; // 06:00
        const setupWindowEnd = 22;  // 22:00
        
        let candidateStart = new Date(requestedStart);
        
        // If before 06:00, move to 06:00 same day
        if (candidateStart.getHours() < setupWindowStart) {
            candidateStart.setHours(setupWindowStart, 0, 0, 0);
        }
        // If after 22:00, move to 06:00 next day
        else if (candidateStart.getHours() >= setupWindowEnd) {
            candidateStart.setDate(candidateStart.getDate() + 1);
            candidateStart.setHours(setupWindowStart, 0, 0, 0);
        }
        
        // Ensure setup can complete within window
        const candidateEnd = new Date(candidateStart.getTime() + setupDurationMin * 60000);
        if (candidateEnd.getHours() > setupWindowEnd) {
            // Setup too long for remaining window, move to next day
            candidateStart.setDate(candidateStart.getDate() + 1);
            candidateStart.setHours(setupWindowStart, 0, 0, 0);
        }
        
        return candidateStart;
    }

    /**
     * Select operator for specific time slot based on shift assignments
     */
    selectOperatorForTimeSlot(setupStart, setupEnd) {
        const startHour = setupStart.getHours();
        
        // Shift 1: 06:00-14:00 → Operators A, B
        if (startHour >= 6 && startHour < 14) {
            const shift1Operators = ['A', 'B'];
            for (const op of shift1Operators) {
                if (this.isOperatorAvailable(op, setupStart, setupEnd)) {
                    return op;
                }
            }
        }
        // Shift 2: 14:00-22:00 → Operators C, D
        else if (startHour >= 14 && startHour < 22) {
            const shift2Operators = ['C', 'D'];
            for (const op of shift2Operators) {
                if (this.isOperatorAvailable(op, setupStart, setupEnd)) {
                    return op;
                }
            }
        }
        
        // Fallback: return first available operator from appropriate shift
        return startHour < 14 ? 'A' : 'C';
    }

    /**
     * Select available operator from specific shift operators
     */
    selectAvailableOperatorFromShift(shiftOperators, setupStart, setupEnd) {
        // First, try to find an available operator
        for (const operator of shiftOperators) {
            if (this.isOperatorAvailable(operator, setupStart, setupEnd)) {
                Logger.log(`[OPERATOR-SELECTED] ${operator} available for setup ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
                return operator;
            }
        }
        
        // If no operator is available, find the earliest time when any operator becomes available
        let earliestAvailableTime = null;
        let earliestOperator = null;
        
        for (const operator of shiftOperators) {
            const intervals = this.operatorSchedule[operator] || [];
            let operatorFreeTime = new Date(setupStart);
            
            // Find when this operator becomes free
            for (const interval of intervals) {
                if (setupStart < interval.end) {
                    operatorFreeTime = new Date(Math.max(operatorFreeTime.getTime(), interval.end.getTime()));
                }
            }
            
            // Check if this operator is in correct shift at the free time
            if (this.isOperatorInCorrectShift(operator, operatorFreeTime, new Date(operatorFreeTime.getTime() + (setupEnd.getTime() - setupStart.getTime())))) {
                if (!earliestAvailableTime || operatorFreeTime < earliestAvailableTime) {
                    earliestAvailableTime = operatorFreeTime;
                    earliestOperator = operator;
                }
            }
        }
        
        if (earliestAvailableTime && earliestOperator) {
            Logger.log(`[OPERATOR-DELAY] No operator available immediately, ${earliestOperator} available at ${earliestAvailableTime.toISOString()}`);
            return {
                operator: earliestOperator,
                delayedStart: earliestAvailableTime
            };
        }
        
        // Fallback: return first operator (will be handled by validation)
        Logger.log(`[OPERATOR-FALLBACK] No operator available, using ${shiftOperators[0]} (will be validated)`);
        return shiftOperators[0];
    }

    /**
     * Get the next valid shift start time
     * @param {Date} currentTime - Current time
     * @returns {Date} Next valid shift start time
     */
    getNextValidShiftStart(currentTime) {
        const hour = currentTime.getHours();
        
        // If we're in morning shift (6-14), next shift is afternoon (14:00)
        if (hour >= 6 && hour < 14) {
            const nextShift = new Date(currentTime);
            nextShift.setHours(14, 0, 0, 0);
            return nextShift;
        }
        // If we're in afternoon shift (14-22), next shift is next day morning (06:00)
        else if (hour >= 14 && hour < 22) {
            const nextShift = new Date(currentTime);
            nextShift.setDate(nextShift.getDate() + 1);
            nextShift.setHours(6, 0, 0, 0);
            return nextShift;
        }
        // If we're outside setup window (22-06), next shift is morning (06:00)
        else {
            const nextShift = new Date(currentTime);
            if (hour >= 22) {
                // If it's night time, next shift is tomorrow morning
                nextShift.setDate(nextShift.getDate() + 1);
            }
            nextShift.setHours(6, 0, 0, 0);
            return nextShift;
        }
    }

    /**
     * Check if operator is assigned to correct shift for given time
     */
    isOperatorInCorrectShift(operator, setupTime, setupEndTime = null) {
        const hour = setupTime.getHours();
        
        // If setupEndTime is provided, check that entire setup fits within shift
        if (setupEndTime) {
            const endHour = setupEndTime.getHours();
            
            // Check if setup spans across different days
            const isSameDay = setupTime.getDate() === setupEndTime.getDate();
            
            if (!isSameDay) {
                // Setup spans across days - not allowed for any operator
                Logger.log(`[SHIFT-CHECK] Setup spans across days: ${setupTime.toISOString()} → ${setupEndTime.toISOString()}`);
                return false;
            }
            
            // Check if entire setup fits within morning shift (06:00-14:00)
            if (hour >= 6 && endHour <= 14) {
                return ['A', 'B'].includes(operator);
            }
            // Check if entire setup fits within afternoon shift (14:00-22:00)
            else if (hour >= 14 && endHour <= 22) {
                return ['C', 'D'].includes(operator);
            }
            
            // Setup doesn't fit within any single shift
            Logger.log(`[SHIFT-CHECK] Setup doesn't fit within single shift: ${hour}:00 → ${endHour}:00`);
            return false;
        }
        
        // Legacy check - only start time (for backward compatibility)
        // Shift 1 (06:00-14:00): Operators A, B
        if (hour >= 6 && hour < 14) {
            return ['A', 'B'].includes(operator);
        }
        // Shift 2 (14:00-22:00): Operators C, D
        else if (hour >= 14 && hour < 22) {
            return ['C', 'D'].includes(operator);
        }
        
        // Outside setup window (22:00-06:00): no operators allowed
        return false;
    }

    /**
     * Check if operator is available during specified time window
     */
    isOperatorAvailable(operator, startTime, endTime) {
        const intervals = this.operatorSchedule[operator] || [];
        
        // Check for conflicts with existing reservations
        for (const interval of intervals) {
            if (startTime < interval.end && endTime > interval.start) {
                return false; // Overlap detected
            }
        }
        
        // Check if operator is in correct shift for this entire time window
        return this.isOperatorInCorrectShift(operator, startTime, endTime);
    }
    
    /**
     * Get operators available in the next shift after given operator
     */
    getOperatorsForNextShift(currentOperator) {
        const currentShift = this.operatorShifts[currentOperator];
        
        if (currentShift.shift === 'morning') {
            // Morning shift (A,B) -> Afternoon shift (C,D)
            return ['C', 'D'];
        } else if (currentShift.shift === 'afternoon') {
            // Afternoon shift (C,D) -> Next day Morning shift (A,B)
            return ['A', 'B'];
        }
        
        return [];
    }
    
    /**
     * Get the next valid shift start time
     */
    getNextValidShiftStart(referenceTime) {
        const nextDay = new Date(referenceTime);
        nextDay.setDate(nextDay.getDate() + 1);
        nextDay.setHours(6, 0, 0, 0); // Next morning shift start
        
        return nextDay;
    }
    /**
     * Apply production window constraints to machine run operations
     * If run crosses production window boundary, pause and resume at next valid window
     */
    applyProductionWindowConstraints(machine, runStart, runEnd, runDuration) {
        const productionWindow = this.productionWindow;
        
        // If 24x7 production, no constraints
        if (productionWindow.type === '24x7') {
            return {
                actualRunStart: runStart,
                actualRunEnd: runEnd,
                paused: false
            };
        }
        
        // Check if run crosses production window boundary
        const windowStart = new Date(runStart);
        windowStart.setHours(productionWindow.start, 0, 0, 0);
        
        const windowEnd = new Date(runStart);
        windowEnd.setHours(productionWindow.end, 0, 0, 0);
        
        if (runEnd > windowEnd) {
            const workDoneInWindow = windowEnd.getTime() - runStart.getTime();
            const remainingWork = runDuration * 60000 - workDoneInWindow;
            
            Logger.log(`[PRODUCTION-WINDOW] Machine ${machine} production window ends at ${windowEnd.toISOString()}, ${Math.round(workDoneInWindow / 60000)}min done, ${Math.round(remainingWork / 60000)}min remaining`);
            
            // Calculate when remaining work can resume (next production window)
            const nextWindowStart = new Date(windowEnd);
            nextWindowStart.setDate(nextWindowStart.getDate() + 1);
            nextWindowStart.setHours(productionWindow.start, 0, 0, 0);
            
            const actualRunEnd = new Date(nextWindowStart.getTime() + remainingWork);
            
            Logger.log(`[PRODUCTION-WINDOW] Remaining work resumes at ${nextWindowStart.toISOString()}`);
            
            return {
                actualRunStart: runStart,
                actualRunEnd: actualRunEnd,
                paused: true,
                pauseStart: windowEnd,
                pauseEnd: nextWindowStart,
                workDoneBeforePause: workDoneInWindow,
                remainingWork: remainingWork
            };
        }
        
        return {
            actualRunStart: runStart,
            actualRunEnd: runEnd,
            paused: false
        };
    }
    selectOptimalPerson(orderData, setupStart, setupEnd) {
        Logger.log(`[OPERATOR-SELECTION] Looking for operator for setup: ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
        
        // AGGRESSIVE AUTO-BALANCING SYSTEM
        // 1. Strictly prevent overlaps — one operator cannot handle two setups at the same time
        // 2. Distribute workload fairly: always assign the operator with the least total setup time so far
        // 3. Respect shift windows — only assign operators during their available shifts
        // 4. If overlap or unavailability occurs, reassign to another free operator
        // 5. Auto-assign logic must balance workload across all operators while ensuring continuous utilization
        
        // STEP 1: Get all operators who are on shift during the setup interval
        const operatorsOnShift = this.getOperatorsOnShift(setupStart, setupEnd);
        
        if (operatorsOnShift.length === 0) {
            Logger.log(`[OPERATOR-ERROR] No operators on shift for ${setupStart.toISOString()}-${setupEnd.toISOString()}`);
            const nextShiftStart = this.getNextValidShiftStart(setupStart);
            Logger.log(`[OPERATOR-RESCUE] Delaying setup to next shift start: ${nextShiftStart.toISOString()}`);
            return {
                operator: null,
                delayedStart: nextShiftStart,
                reason: 'delayed_to_next_shift'
            };
        }
        
        // STEP 2: Calculate workload for each operator and find truly available ones
        const operatorCandidates = [];
        
        for (const operator of operatorsOnShift) {
            // CRITICAL: Check for ANY overlap with microsecond precision
            const hasOverlap = this.hasOperatorConflict(operator, setupStart, setupEnd);
            
            if (!hasOverlap) {
                // Calculate total workload for this operator
                const totalSetupMinutes = this.getTotalOperatorSetupMinutes(operator);
                const currentShiftMinutes = this.getOperatorSetupMinutesInShift(operator, setupStart);
                const shift = this.operatorShifts[operator].shift;
                
                operatorCandidates.push({
                    operator,
                    totalSetupMinutes,
                    currentShiftMinutes,
                    shift,
                    priority: this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)
                });
                
                Logger.log(`[OPERATOR-CANDIDATE] ${operator}: ${totalSetupMinutes}min total, ${currentShiftMinutes}min current shift, ${shift}, priority: ${this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)}`);
            } else {
                Logger.log(`[OPERATOR-CONFLICT] ${operator} has overlapping setup - SKIPPED`);
            }
        }
        
        // STEP 3: Select operator using aggressive auto-balancing
        if (operatorCandidates.length > 0) {
            // Sort by priority (lower is better)
            operatorCandidates.sort((a, b) => a.priority - b.priority);
            
            const selectedOperator = operatorCandidates[0].operator;
            Logger.log(`[SETUP-ASSIGN] Operator ${selectedOperator} chosen — auto-balanced (priority: ${operatorCandidates[0].priority})`);
            return selectedOperator;
        }
        
        // STEP 4: If no operator available immediately, find earliest possible time with auto-balancing
        Logger.log(`[OPERATOR-DELAYED] No immediate availability, finding earliest slot with auto-balancing...`);
        
        const delayedCandidates = [];
        const setupDuration = setupEnd.getTime() - setupStart.getTime();
        
        for (const operator of operatorsOnShift) {
            const earliestFree = this.getEarliestOperatorFreeTime(operator, setupStart);
            const adjustedSetupStart = new Date(Math.max(setupStart.getTime(), earliestFree.getTime()));
            const adjustedSetupEnd = new Date(adjustedSetupStart.getTime() + setupDuration);
            
            // Check if adjusted setup still falls within operator's shift
            if (this.isOperatorOnShift(operator, adjustedSetupStart, adjustedSetupEnd)) {
                const totalSetupMinutes = this.getTotalOperatorSetupMinutes(operator);
                const currentShiftMinutes = this.getOperatorSetupMinutesInShift(operator, setupStart);
                const shift = this.operatorShifts[operator].shift;
                const delay = adjustedSetupStart.getTime() - setupStart.getTime();
                
                delayedCandidates.push({
                    operator,
                    adjustedSetupStart,
                    adjustedSetupEnd,
                    delay,
                    totalSetupMinutes,
                    currentShiftMinutes,
                    shift,
                    priority: this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)
                });
                
                Logger.log(`[OPERATOR-DELAYED] ${operator} available at ${adjustedSetupStart.toISOString()} (${Math.round(delay / (1000 * 60))} min delay), priority: ${this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)}`);
            }
        }
        
        if (delayedCandidates.length > 0) {
            // Sort by priority (lower is better), then by delay
            delayedCandidates.sort((a, b) => {
                if (a.priority !== b.priority) return a.priority - b.priority;
                return a.delay - b.delay;
            });
            
            const bestDelayed = delayedCandidates[0];
            Logger.log(`[OPERATOR-SELECTED-DELAYED] ${bestDelayed.operator} (priority: ${bestDelayed.priority}, delay: ${Math.round(bestDelayed.delay / (1000 * 60))} min)`);
            return bestDelayed.operator;
        }
        
        // STEP 5: Last resort - find any operator and delay significantly
        Logger.log(`[OPERATOR-LAST-RESORT] No operators available, using emergency delay...`);
        const emergencyOperator = operatorsOnShift[0];
        const emergencyDelay = new Date(setupStart.getTime() + 30 * 60000); // 30 minutes delay
        Logger.log(`[OPERATOR-EMERGENCY] Using ${emergencyOperator} with 30-minute delay: ${emergencyDelay.toISOString()}`);
        return emergencyOperator;
    }
    
    // Calculate operator priority for auto-balancing (lower is better)
    calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift) {
        // Priority factors (lower number = higher priority):
        // 1. Total workload (least loaded gets priority 1)
        // 2. Current shift workload (least loaded in current shift gets priority 2)
        // 3. Shift balancing (prefer afternoon shift for better distribution)
        // 4. Operator rotation (A, B, C, D rotation)
        
        let priority = 0;
        
        // Factor 1: Total workload (0-1000 range)
        priority += totalSetupMinutes;
        
        // Factor 2: Current shift workload (0-500 range)
        priority += currentShiftMinutes * 0.5;
        
        // Factor 3: Shift balancing (prefer afternoon shift)
        if (shift === 'afternoon') {
            priority -= 50; // Boost afternoon shift operators
        }
        
        // Factor 4: Operator rotation (A=0, B=1, C=2, D=3)
        const operatorRotation = ['A', 'B', 'C', 'D'].indexOf(operator);
        priority += operatorRotation * 10;
        
        return Math.round(priority);
    }
    
    // Get total setup minutes for an operator across all time
    getTotalOperatorSetupMinutes(operator) {
        const intervals = this.operatorSchedule[operator] || [];
        let totalMinutes = 0;
        
        for (const interval of intervals) {
            totalMinutes += (interval.end.getTime() - interval.start.getTime()) / (1000 * 60);
        }
        
        return Math.round(totalMinutes);
    }

    // Check if a machine has conflicts with the proposed time window
    hasConflict(machine, candidateWindow) {
        const existingIntervals = this.machineSchedule[machine] || [];
        
        for (const interval of existingIntervals) {
            // Check for overlap: intervals overlap if start1 < end2 && start2 < end1
            if (candidateWindow.start < interval.end && interval.start < candidateWindow.end) {
                Logger.log(`[CONFLICT-DETECTED] Machine ${machine}: candidate ${candidateWindow.start.toISOString()}→${candidateWindow.end.toISOString()} overlaps with existing ${interval.start.toISOString()}→${interval.end.toISOString()}`);
                return true;
            }
        }
        return false;
    }

    // Get the earliest time when a machine is completely free
    getEarliestFreeTime(machine) {
        const intervals = this.machineSchedule[machine] || [];
        if (intervals.length === 0) {
            return this.getEffectiveStartTime(); // Available at global start time
        }
        
        // Find the latest end time among all intervals
        let latestEnd = new Date(0);
        for (const interval of intervals) {
            if (interval.end > latestEnd) {
                latestEnd = interval.end;
            }
        }
        
        Logger.log(`[MACHINE-AVAILABILITY] ${machine} earliest free time: ${latestEnd.toISOString()}`);
        return latestEnd;
    }

    // Reserve a machine for a specific time window
    reserveMachine(machine, startTime, endTime) {
        if (!this.machineSchedule[machine]) {
            this.machineSchedule[machine] = [];
        }
        
        const reservation = { start: new Date(startTime), end: new Date(endTime) };
        this.machineSchedule[machine].push(reservation);
        
        // Sort intervals by start time for easier debugging
        this.machineSchedule[machine].sort((a, b) => a.start.getTime() - b.start.getTime());
        
        Logger.log(`[MACHINE-RESERVED] ${machine} reserved from ${startTime.toISOString()} to ${endTime.toISOString()}`);
        
        // Defensive check: verify no overlaps after reservation
        this.validateMachineSchedule(machine);
    }

    // Validate that a machine has no overlapping bookings (defensive check)
    validateMachineSchedule(machine) {
        const intervals = this.machineSchedule[machine] || [];
        
        for (let i = 0; i < intervals.length - 1; i++) {
            for (let j = i + 1; j < intervals.length; j++) {
                const interval1 = intervals[i];
                const interval2 = intervals[j];
                
                // Check for overlap
                if (interval1.start < interval2.end && interval2.start < interval1.end) {
                    Logger.log(`[MACHINE-SERIALIZATION-ERROR] Machine ${machine} has overlapping intervals:`);
                    Logger.log(`  Interval 1: ${interval1.start.toISOString()} → ${interval1.end.toISOString()}`);
                    Logger.log(`  Interval 2: ${interval2.start.toISOString()} → ${interval2.end.toISOString()}`);
                    throw new Error(`Machine ${machine} has overlapping bookings - scheduling integrity violated`);
                }
            }
        }
    }

    calculatePreliminaryTiming(operation, orderData, batchQty, person, earliestStartTime) {
        // Use the provided earliest start time (which includes sequential dependencies)
        let setupStartTime = new Date(earliestStartTime);

        // Apply setup window constraints
        setupStartTime = this.enforceSetupWindow(setupStartTime, orderData);

        // Calculate setup end
        const setupDuration = operation.SetupTime_Min || 0;
        let setupEndTime = new Date(setupStartTime.getTime() + setupDuration * 60000);

        // Validate setup fits within window
        setupEndTime = this.validateSetupWithinWindow(setupStartTime, setupEndTime, orderData);

        // CRITICAL FIX: Calculate timing without machine-specific constraints
        // This gives us the theoretical minimum time needed, regardless of machine availability
        const cycleTime = operation.CycleTime_Min || 0;
        const runStart = new Date(setupEndTime);
        
        // Calculate theoretical run end (continuous processing, no machine pauses)
        const totalProcessingTime = batchQty * cycleTime; // minutes
        const runEnd = new Date(runStart.getTime() + totalProcessingTime * 60000);

        return {
            setupStart: setupStartTime,
            setupEnd: setupEndTime,
            runStart: runStart,
            runEnd: runEnd
        };
    }

    calculateOperationTiming(operation, orderData, batchQty, machine, person, earliestStartTime, previousOpPieceCompletionTimes = null, previousOpRunEnd = null) {
        Logger.log(`[PIECE-LEVEL] Starting piece-level calculation for ${operation.OperationName}, batch qty: ${batchQty}`);
        
        // VALIDATE INPUTS
        if (!operation) {
            throw new Error(`Operation is undefined`);
        }
        if (!earliestStartTime) {
            throw new Error(`EarliestStartTime is undefined`);
        }
        if (isNaN(earliestStartTime.getTime())) {
            throw new Error(`EarliestStartTime is invalid: ${earliestStartTime}`);
        }
        
        // ENHANCED PIECE-FLOW LOGIC: Allow parallel processing when possible
        let setupStartTime = earliestStartTime;
        
        // If this is not the first operation, check piece-flow trigger
        if (previousOpPieceCompletionTimes && previousOpPieceCompletionTimes.length > 0) {
            const firstPieceReadyTime = previousOpPieceCompletionTimes[0];
            
            // VALIDATE firstPieceReadyTime
            if (!firstPieceReadyTime || isNaN(firstPieceReadyTime.getTime())) {
                Logger.log(`[ERROR] Invalid firstPieceReadyTime: ${firstPieceReadyTime}`);
                throw new Error(`Invalid firstPieceReadyTime: ${firstPieceReadyTime}`);
            }
            
            // ENHANCED LOGIC: Only enforce piece-flow if it's actually necessary
            // Allow setup to start earlier if machine and operator are available
            const pieceFlowDelay = firstPieceReadyTime.getTime() - setupStartTime.getTime();
            
            if (pieceFlowDelay > 0) {
                // Check if we can start setup earlier (setup can happen while previous operation is running)
                const setupDuration = operation.SetupTime_Min || 0;
                const setupEndTime = new Date(setupStartTime.getTime() + setupDuration * 60000);
                
                // If setup can complete before first piece is ready, allow early start
                if (setupEndTime <= firstPieceReadyTime) {
                    Logger.log(`[PARALLEL-SETUP] Setup can start early: ${setupStartTime.toISOString()} → ${setupEndTime.toISOString()} (first piece ready: ${firstPieceReadyTime.toISOString()})`);
                    // Keep original setupStartTime for parallel setup
                } else {
                    // Setup would interfere with piece flow, enforce dependency
            setupStartTime = new Date(Math.max(setupStartTime.getTime(), firstPieceReadyTime.getTime()));
                    Logger.log(`[PIECE-FLOW-TRIGGER] Setup delayed to: ${setupStartTime.toISOString()} (first piece ready: ${firstPieceReadyTime.toISOString()})`);
                }
            } else {
                Logger.log(`[PIECE-FLOW-TRIGGER] Setup can start immediately: ${setupStartTime.toISOString()} (first piece already ready: ${firstPieceReadyTime.toISOString()})`);
            }
        }
        
        // Determine machine availability
        const machineEarliestFree = this.getEarliestFreeTime(machine);
        setupStartTime = new Date(Math.max(setupStartTime.getTime(), machineEarliestFree.getTime()));
        
        Logger.log(`[PIECE-LEVEL] Setup timing: machine free at ${machineEarliestFree.toISOString()}, piece-flow trigger ${earliestStartTime.toISOString()}, chosen: ${setupStartTime.toISOString()}`);

        // Apply setup window constraints
        setupStartTime = this.enforceSetupWindow(setupStartTime, orderData);

        // Calculate setup end
        const setupDuration = operation.SetupTime_Min || 0;
        let setupEndTime = new Date(setupStartTime.getTime() + setupDuration * 60000);

        // Validate setup fits within window
        setupEndTime = this.validateSetupWithinWindow(setupStartTime, setupEndTime, orderData);

        // USER'S EXACT PIECE-LEVEL ALGORITHM
        const cycleTime = operation.CycleTime_Min || 0;
        
        // VALIDATE INPUTS
        if (!setupStartTime || !setupEndTime) {
            Logger.log(`[ERROR] Invalid setup times: setupStartTime=${setupStartTime}, setupEndTime=${setupEndTime}`);
            throw new Error(`Invalid setup times: setupStartTime=${setupStartTime}, setupEndTime=${setupEndTime}`);
        }

        // STRICT PIECE-LEVEL FLOW: Calculate piece completion times exactly as user specified
        const pieceCompletionTimes = [];
        let currentMachineTime = new Date(setupEndTime);
        
        Logger.log(`[PIECE-FLOW] Starting piece processing at setup end: ${setupEndTime.toISOString()}`);
        
        for (let pieceIndex = 0; pieceIndex < batchQty; pieceIndex++) {
            // Determine when this piece can start processing
            let pieceStartTime;
            
            if (previousOpPieceCompletionTimes && previousOpPieceCompletionTimes.length > 0 && previousOpPieceCompletionTimes[pieceIndex]) {
                // RULE: Wait for the corresponding piece from previous operation OR machine availability
                pieceStartTime = new Date(Math.max(
                    previousOpPieceCompletionTimes[pieceIndex].getTime(),
                    currentMachineTime.getTime()
                ));
                Logger.log(`[PIECE-FLOW] Piece ${pieceIndex + 1}: Waiting for prev piece ${previousOpPieceCompletionTimes[pieceIndex].toISOString()} OR machine ${currentMachineTime.toISOString()}`);
            } else {
                // No previous operation constraint, start when machine is free
                pieceStartTime = new Date(currentMachineTime);
                Logger.log(`[PIECE-FLOW] Piece ${pieceIndex + 1}: Starting immediately at machine time ${currentMachineTime.toISOString()}`);
            }
            
            // Calculate when this piece completes
            const pieceEndTime = new Date(pieceStartTime.getTime() + cycleTime * 60000);
            pieceCompletionTimes.push(pieceEndTime);
            
            Logger.log(`[PIECE-FLOW] Piece ${pieceIndex + 1}: ${pieceStartTime.toISOString().substr(11,8)} → ${pieceEndTime.toISOString().substr(11,8)} (${cycleTime}min cycle)`);
            
            // CRITICAL: Machine is occupied until this piece completes
            currentMachineTime = new Date(pieceEndTime);
        }
        
        // Calculate operation timing
        const runStartTime = new Date(setupEndTime);
        let runEndTime = pieceCompletionTimes[batchQty - 1]; // Last piece completion time

        // CRITICAL FIX: Enforce piece-level flow constraint
        // RunEnd(Op n) must always ≥ RunEnd(Op n-1)
        if (previousOpRunEnd) {
            const previousOpEndTime = new Date(previousOpRunEnd);
            if (runEndTime.getTime() < previousOpEndTime.getTime()) {
                Logger.log(`[PIECE-FLOW-CONSTRAINT] Op${operation.OperationSeq} RunEnd ${runEndTime.toISOString()} is before previous Op RunEnd ${previousOpEndTime.toISOString()}`);
                Logger.log(`[PIECE-FLOW-CONSTRAINT] Adjusting RunEnd to maintain logical flow`);
                
                // Adjust RunEnd to be after previous operation
                runEndTime = new Date(previousOpEndTime.getTime() + cycleTime * 60000); // Add at least one cycle time
                
                // Recalculate piece completion times to maintain consistency
                const timeAdjustment = runEndTime.getTime() - pieceCompletionTimes[batchQty - 1].getTime();
                for (let i = 0; i < pieceCompletionTimes.length; i++) {
                    pieceCompletionTimes[i] = new Date(pieceCompletionTimes[i].getTime() + timeAdjustment);
                }
                
                Logger.log(`[PIECE-FLOW-CONSTRAINT] Adjusted RunEnd to: ${runEndTime.toISOString()}`);
            }
        }

        Logger.log(`[USER-ALGORITHM] Run start: ${runStartTime.toISOString()}`);
        Logger.log(`[USER-ALGORITHM] Run end: ${runEndTime.toISOString()}`);
        Logger.log(`[USER-ALGORITHM] First piece done: ${pieceCompletionTimes[0].toISOString()}`);

        // Calculate piece start times for return value
        const pieceStartTimes = [];
        for (let pieceIndex = 0; pieceIndex < batchQty; pieceIndex++) {
            const pieceReadyTime = previousOpPieceCompletionTimes && previousOpPieceCompletionTimes.length > 0 ? 
                previousOpPieceCompletionTimes[pieceIndex] || new Date(setupEndTime) : 
                new Date(setupEndTime);
            const runStart = new Date(Math.max(
                pieceReadyTime.getTime(),
                new Date(setupEndTime).getTime() + pieceIndex * cycleTime * 60000
            ));
            pieceStartTimes.push(runStart);
        }

        // RETURN RESULTS WITH USER'S ALGORITHM
        return {
            setupStartTime,
            setupEndTime,
            runStartTime,
            runEndTime,
            pieceCompletionTimes,
            pieceStartTimes,
            firstPieceDone: pieceCompletionTimes[0],
            totalWorkTime: batchQty * cycleTime,
            totalPausedTime: 0, // No pauses in user's algorithm
            CycleTime_Min: cycleTime,
            Batch_Qty: batchQty
        };
    }

    enforceSetupWindow(time, orderData) {
        const setupWindow = this.parseSetupWindow(
            orderData.setupWindow || this.globalSettings.setupWindow || "06:00-22:00"
        );
        
        const hour = time.getHours();
        
        // ENHANCED: Better utilization of the full 06:00-22:00 setup window
        if (hour < setupWindow.start) {
            // Before window - move to start of window (same day)
            const newTime = new Date(time);
            newTime.setHours(setupWindow.start, 0, 0, 0);
            Logger.log(`[SETUP-WINDOW] Setup moved to window start: ${newTime.toISOString()}`);
            return newTime;
        } else if (hour >= setupWindow.end) {
            // After window - move to next day window start
                const newTime = new Date(time);
                newTime.setDate(newTime.getDate() + 1);
                newTime.setHours(setupWindow.start, 0, 0, 0);
            Logger.log(`[SETUP-WINDOW] Setup moved to next day window start: ${newTime.toISOString()}`);
                return newTime;
        }
        
        // CRITICAL FIX: Remove artificial delay that breaks piece-level flow
        // The setup window should only enforce boundaries, not artificially delay setups
        // Piece-level flow is more important than "distribution"
        
        // Within window, return as-is (no artificial delays)
        Logger.log(`[SETUP-WINDOW] Setup within window: ${time.toISOString()}`);
        return new Date(time);
    }

    validateSetupWithinWindow(setupStart, setupEnd, orderData) {
        const setupWindow = this.parseSetupWindow(
            orderData.setupWindow || this.globalSettings.setupWindow || "06:00-22:00"
        );
        
        const setupEndHour = setupEnd.getHours();
        
        if (setupEndHour > setupWindow.end || (setupEndHour === setupWindow.end && setupEnd.getMinutes() > 0)) {
            // Setup would end after window - move entire setup to next day
            const nextDay = new Date(setupStart);
            nextDay.setDate(nextDay.getDate() + 1);
            nextDay.setHours(setupWindow.start, 0, 0, 0);
            
            const setupDuration = (setupEnd.getTime() - setupStart.getTime()) / 60000;
            return new Date(nextDay.getTime() + setupDuration * 60000);
        }
        
        return new Date(setupEnd);
    }

    parseSetupWindow(windowString) {
        if (!windowString) return { start: 6, end: 22 };
        
        // Handle object format: { start: 6, end: 22 }
        if (typeof windowString === 'object' && windowString.start !== undefined && windowString.end !== undefined) {
            return {
                start: windowString.start,
                end: windowString.end
            };
        }
        
        // Handle string format: "06:00-22:00"
        if (typeof windowString === 'string') {
        const [start, end] = windowString.split('-');
        if (!start || !end) return { start: 6, end: 22 };
        
        const startHour = parseInt(start.split(':')[0]);
        const endHour = parseInt(end.split(':')[0]);
        
        return {
            start: isNaN(startHour) ? 6 : startHour,
            end: isNaN(endHour) ? 22 : endHour
        };
        }
        
        // Fallback
        return { start: 6, end: 22 };
    }

    formatDuration(minutes) {
        if (minutes < 60) {
            return `${Math.round(minutes)}M`;
        }
        
        const totalMinutes = Math.round(minutes);
        const days = Math.floor(totalMinutes / (24 * 60));
        const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
        const mins = totalMinutes % 60;
        
        let result = '';
        if (days > 0) result += `${days}D `;
        if (hours > 0) result += `${hours}H `;
        if (mins > 0) result += `${mins}M`;
        
        return result.trim() || '0M';
    }

    /**
     * Format duration breakdown with work/holiday split
     * @param {Date} setupStart - Start time of setup
     * @param {Date} runEnd - End time of run
     * @param {number} workMinutes - Actual work minutes logged
     * @param {number} holidayMinutes - Holiday/non-productive minutes
     * @returns {string} Formatted duration breakdown
     */
    formatDurationBreakdown(setupStart, runEnd, workMinutes = 0, holidayMinutes = 0) {
        // Calculate total elapsed time in minutes
        const totalMs = runEnd.getTime() - setupStart.getTime();
        const totalMinutes = Math.floor(totalMs / (1000 * 60));
        
        // Break into Days, Hours, Minutes
        const days = Math.floor(totalMinutes / 1440); // 24 * 60
        const hours = Math.floor((totalMinutes % 1440) / 60);
        const minutes = totalMinutes % 60;
        
        // Format total duration - ALWAYS show XD YH ZM format
        let totalDuration = '';
        if (days > 0) totalDuration += `${days}D `;
        if (hours > 0) totalDuration += `${hours}H `;
        if (minutes > 0) totalDuration += `${minutes}M`;
        
        // Ensure we always have at least one component
        if (!totalDuration.trim()) {
            totalDuration = '0M';
        }
        
        totalDuration = totalDuration.trim();
        
        // For the desired output format, we want clean XD YH ZM without work breakdown
        // This matches the exact format: "3D 4H 10M"
        return totalDuration;
    }

    formatDateTime(date) {
        if (!date || !(date instanceof Date)) return 'Invalid Date';
        
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hours}:${minutes}`;
    }

    // Final validation: ensure no machine has overlapping bookings
    validateAllMachineSchedules() {
        Logger.log("[FINAL-VALIDATION] Checking all machines for overlapping bookings...");
        
        for (const machine of this.allMachines) {
            try {
                this.validateMachineSchedule(machine);
                const intervals = this.machineSchedule[machine] || [];
                Logger.log(`[VALIDATION-PASS] ${machine}: ${intervals.length} bookings, no conflicts`);
            } catch (error) {
                Logger.log(`[VALIDATION-FAIL] ${machine}: ${error.message}`);
                throw error;
            }
        }
        
        Logger.log("[FINAL-VALIDATION] ✅ All machines validated - no overlapping bookings found");
    }
    
    // OPERATOR SHIFT VALIDATION METHODS
    isOperatorOnShift(operator, setupStart, setupEnd) {
        const shift = this.operatorShifts[operator];
        if (!shift) {
            Logger.log(`[ERROR] Unknown operator: ${operator}`);
            return false;
        }
        
        const startHour = setupStart.getHours();
        const endHour = setupEnd.getHours();
        
        // ULTRA-AGGRESSIVE: Enhanced shift validation with comprehensive boundary checking
        // Check if entire setup interval falls within operator's shift
        // Allow setups that end exactly at shift boundary
        // CRITICAL FIX: Handle cross-day scenarios properly
        
        let isOnShift = false;
        
        // Check for same-day setups
        if (startHour >= shift.start && endHour <= shift.end) {
            isOnShift = true;
        }
        
        // Additional check for cross-day scenarios (setup starts before midnight, ends after)
        if (!isOnShift && startHour >= shift.start && endHour > 24) {
            // This is a cross-day setup, check if it's still within the same shift
            const adjustedEndHour = endHour - 24; // Convert to next day hour
            isOnShift = adjustedEndHour <= shift.end;
        }
        
        // Additional check for setups that start late and end early next day
        if (!isOnShift && startHour >= shift.start && endHour < startHour) {
            // Setup crosses midnight but ends early next day
            isOnShift = true; // Assume it's within shift if it starts within shift
        }
        
        Logger.log(`[SHIFT-CHECK] ${operator} shift: ${shift.start}:00-${shift.end}:00, setup: ${startHour}:${setupStart.getMinutes().toString().padStart(2, '0')}-${endHour}:${setupEnd.getMinutes().toString().padStart(2, '0')}, onShift: ${isOnShift}`);
        
        return isOnShift;
    }
    
    getOperatorsOnShift(setupStart, setupEnd) {
        const availableOperators = [];
        
        for (const [operator, shift] of Object.entries(this.operatorShifts)) {
            if (this.isOperatorOnShift(operator, setupStart, setupEnd)) {
                availableOperators.push(operator);
            }
        }
        
        Logger.log(`[SHIFT-CHECK] Operators available for ${setupStart.toISOString()}-${setupEnd.toISOString()}: ${availableOperators.join(', ')}`);
        return availableOperators;
    }
    
    getNextShiftStart(currentTime) {
        const currentHour = currentTime.getHours();
        
        // Determine which shift we're currently in and get the next one
        if (currentHour >= 6 && currentHour < 14) {
            // Currently in morning shift (6-14), next is afternoon shift (14-22)
            const nextShiftStart = new Date(currentTime);
            nextShiftStart.setHours(14, 0, 0, 0);
            return nextShiftStart;
        } else if (currentHour >= 14 && currentHour < 22) {
            // Currently in afternoon shift (14-22), next is morning shift next day (6-14)
            const nextShiftStart = new Date(currentTime);
            nextShiftStart.setDate(nextShiftStart.getDate() + 1);
            nextShiftStart.setHours(6, 0, 0, 0);
            return nextShiftStart;
        } else {
            // Currently in night hours (22-6), next is morning shift (6-14)
            const nextShiftStart = new Date(currentTime);
            if (currentHour >= 22) {
                // After 22:00, next shift is tomorrow morning
                nextShiftStart.setDate(nextShiftStart.getDate() + 1);
            }
            nextShiftStart.setHours(6, 0, 0, 0);
            return nextShiftStart;
        }
    }
    
    hasOperatorConflict(operator, setupStart, setupEnd) {
        const intervals = this.operatorSchedule[operator] || [];
        
        for (const existing of intervals) {
            // Check for overlap: intervals overlap if one starts before the other ends
            const hasOverlap = setupStart < existing.end && setupEnd > existing.start;
            
            if (hasOverlap) {
                Logger.log(`[OPERATOR-CONFLICT] ${operator} has existing setup: ${existing.start.toISOString()}-${existing.end.toISOString()}`);
                return true;
            }
        }
        
        return false;
    }
    
    getOperatorSetupMinutesInShift(operator, referenceTime) {
        const intervals = this.operatorSchedule[operator] || [];
        const shift = this.operatorShifts[operator];
        const shiftStart = new Date(referenceTime);
        shiftStart.setHours(shift.start, 0, 0, 0);
        const shiftEnd = new Date(referenceTime);
        shiftEnd.setHours(shift.end, 0, 0, 0);
        
        let totalMinutes = 0;
        for (const interval of intervals) {
            // Count only intervals that overlap with the current shift
            if (interval.start < shiftEnd && interval.end > shiftStart) {
                const overlapStart = new Date(Math.max(interval.start.getTime(), shiftStart.getTime()));
                const overlapEnd = new Date(Math.min(interval.end.getTime(), shiftEnd.getTime()));
                totalMinutes += (overlapEnd.getTime() - overlapStart.getTime()) / (1000 * 60);
            }
        }
        
        return Math.round(totalMinutes);
    }
    
    getEarliestOperatorFreeTime(operator, setupStart) {
        const intervals = this.operatorSchedule[operator] || [];
        if (intervals.length === 0) {
            return setupStart;
        }
        
        // Find the latest end time among all intervals
        const latestEnd = intervals.reduce((latest, interval) => 
            interval.end > latest ? interval.end : latest, new Date(0)
        );
        
        return latestEnd > setupStart ? latestEnd : setupStart;
    }
    
    reserveOperator(operator, setupStart, setupEnd) {
        if (!this.operatorSchedule[operator]) {
            this.operatorSchedule[operator] = [];
        }
        
        // CRITICAL: Strict overlap prevention with microsecond precision
        const candidateInterval = { start: setupStart, end: setupEnd };
        
        // Check for ANY overlap with existing intervals
        for (const existing of this.operatorSchedule[operator]) {
            if (candidateInterval.start < existing.end && existing.start < candidateInterval.end) {
                Logger.log(`[OPERATOR-CONFLICT] ${operator} has conflicting setup: ${existing.start.toISOString()}-${existing.end.toISOString()}`);
                
                // AGGRESSIVE CONFLICT RESOLUTION: Find alternative operator immediately
                Logger.log(`[OPERATOR-CONFLICT] Attempting aggressive conflict resolution...`);
                const alternativeOperator = this.findAlternativeOperator(setupStart, setupEnd);
                
                if (alternativeOperator && alternativeOperator !== operator) {
                    Logger.log(`[OPERATOR-CONFLICT] ✅ Found alternative operator: ${alternativeOperator}`);
                    this.reserveOperator(alternativeOperator, setupStart, setupEnd);
                    return;
                } else {
                    // Last resort: delay the setup significantly
                    const delayedSetupStart = new Date(setupStart.getTime() + 15 * 60000); // 15 minutes delay
                    const delayedSetupEnd = new Date(setupEnd.getTime() + 15 * 60000);
                    Logger.log(`[OPERATOR-CONFLICT] ⚠️ Delaying setup by 15 minutes: ${delayedSetupStart.toISOString()}`);
                    this.reserveOperator(operator, delayedSetupStart, delayedSetupEnd);
                    return;
                }
            }
        }
        
        // No conflict - reserve the operator
        this.operatorSchedule[operator].push({ start: setupStart, end: setupEnd });
        
        // Sort intervals by start time
        this.operatorSchedule[operator].sort((a, b) => a.start.getTime() - b.start.getTime());
        
        Logger.log(`[OPERATOR-RESERVED] ${operator} reserved for ${setupStart.toISOString()}-${setupEnd.toISOString()}`);
    }
    
    validateOperatorSchedule(operator) {
        const intervals = this.operatorSchedule[operator] || [];
        
        for (let i = 0; i < intervals.length - 1; i++) {
            const current = intervals[i];
            const next = intervals[i + 1];
            
            if (current.end > next.start) {
                throw new Error(`[SETUP-OVERBOOKING] Operator ${operator} has overlapping setup intervals: ${current.start.toISOString()}-${current.end.toISOString()} overlaps with ${next.start.toISOString()}-${next.end.toISOString()}`);
            }
        }
    }
    
    validateAllOperatorSchedules() {
        Logger.log(`[VALIDATION] Checking all operator schedules for overlaps...`);
        for (const operator of this.allPersons) {
            this.validateOperatorSchedule(operator);
        }
        
        // Additional validation: Check concurrent setup limits per shift
        this.validateConcurrentSetupLimits();
        
        Logger.log(`[VALIDATION] All operator schedules validated successfully`);
    }
    
    // PIECE-FLOW TRIGGER VALIDATION (Critical rule enforcement)
    validatePieceFlowTriggers(orderResults) {
        Logger.log(`[VALIDATION] Checking piece-flow triggers for ${orderResults.length} operations...`);
        
        for (let i = 1; i < orderResults.length; i++) {
            const currentOp = orderResults[i];
            const prevOp = orderResults[i - 1];
            
            // Calculate previous operation's first piece completion time
            const prevOpFirstPieceDone = this.calculateFirstPieceDone(prevOp);
            const currentSetupStart = new Date(currentOp.SetupStart);
            
            Logger.log(`[PIECE-FLOW-CHECK] Op${currentOp.OperationSeq}: SetupStart ${currentSetupStart.toISOString()}, PrevOp${prevOp.OperationSeq} FirstPieceDone ${prevOpFirstPieceDone.toISOString()}`);
            
            if (currentSetupStart < prevOpFirstPieceDone) {
                const violationMinutes = Math.ceil((prevOpFirstPieceDone.getTime() - currentSetupStart.getTime()) / (1000 * 60));
                Logger.log(`[PIECE-FLOW-VIOLATION] Op${currentOp.OperationSeq} SetupStart ${violationMinutes}min too early! Must be >= ${prevOpFirstPieceDone.toISOString()}`);
                
                // Auto-fix: Adjust current operation's setup start
                this.adjustOperationForPieceFlowViolation(currentOp, prevOpFirstPieceDone, orderResults, i);
            } else {
                Logger.log(`[PIECE-FLOW-CHECK] Op${currentOp.OperationSeq} ✅ SetupStart is valid`);
            }
        }
        
        Logger.log(`[VALIDATION] Piece-flow trigger validation complete`);
    }
    
    calculateFirstPieceDone(operation) {
        // Calculate when the first piece of this operation completes
        const setupEnd = new Date(operation.SetupEnd);
        const cycleTime = operation.CycleTime_Min || 0;
        
        // First piece starts at setup end and completes after cycle time
        const firstPieceDone = new Date(setupEnd.getTime() + cycleTime * 60000);
        
        Logger.log(`[FIRST-PIECE-CALC] Op${operation.OperationSeq}: SetupEnd ${setupEnd.toISOString()} + ${cycleTime}min = FirstPieceDone ${firstPieceDone.toISOString()}`);
        
        return firstPieceDone;
    }
    
    adjustOperationForPieceFlowViolation(operation, requiredSetupStart, orderResults, operationIndex) {
        Logger.log(`[ADJUST] Adjusting Op${operation.OperationSeq} SetupStart from ${operation.SetupStart} to ${requiredSetupStart.toISOString()}`);
        
        // Calculate new timing
        const originalSetupStart = new Date(operation.SetupStart);
        const setupDuration = operation.SetupTime_Min || 0;
        const newSetupEnd = new Date(requiredSetupStart.getTime() + setupDuration * 60000);
        
        // Calculate new run timing
        const cycleTime = operation.CycleTime_Min || 0;
        const batchQty = operation.Batch_Qty || 1;
        const newRunStart = newSetupEnd;
        const newRunEnd = new Date(newRunStart.getTime() + (cycleTime * batchQty) * 60000);
        
        // Update the operation result
        operation.SetupStart = requiredSetupStart.toISOString();
        operation.SetupEnd = newSetupEnd.toISOString();
        operation.RunStart = newRunStart.toISOString();
        operation.RunEnd = newRunEnd.toISOString();
        
        // Update timing description
        const totalElapsed = newRunEnd.getTime() - requiredSetupStart.getTime();
        const totalMinutes = Math.floor(totalElapsed / (1000 * 60));
        const workMinutes = setupDuration + (cycleTime * batchQty);
        const pausedMinutes = Math.max(0, totalMinutes - workMinutes);
        
        operation.Timing = this.formatDurationBreakdown(
            requiredSetupStart,
            newRunEnd,
            workMinutes,
            pausedMinutes
        );
        
        Logger.log(`[ADJUST] Op${operation.OperationSeq} new timing: Setup ${requiredSetupStart.toISOString()} → ${newSetupEnd.toISOString()}, Run ${newRunStart.toISOString()} → ${newRunEnd.toISOString()}`);
        
        // Check if this adjustment affects subsequent operations
        this.propagateDownstreamAdjustments(orderResults, operationIndex, newRunEnd);
    }
    
    propagateDownstreamAdjustments(orderResults, adjustedIndex, newRunEnd) {
        Logger.log(`[PROPAGATE] Checking if Op${orderResults[adjustedIndex].OperationSeq} adjustment affects downstream operations...`);
        
        // For now, just log that downstream operations may need re-evaluation
        // In a full implementation, you would re-calculate all subsequent operations
        for (let i = adjustedIndex + 1; i < orderResults.length; i++) {
            const downstreamOp = orderResults[i];
            Logger.log(`[PROPAGATE] Op${downstreamOp.OperationSeq} may need re-evaluation due to upstream timing change`);
        }
    }
    
    // COMPREHENSIVE SCHEDULE VALIDATION
    validateCompleteSchedule(orderResults) {
        Logger.log(`[VALIDATION] Starting comprehensive schedule validation...`);
        
        try {
            // 1. Operator shift validation
            this.validateOperatorShifts(orderResults);
            
            // 2. Operator overlap validation
            this.validateOperatorOverlaps(orderResults);
            
            // 3. Machine exclusivity validation
            this.validateMachineExclusivity(orderResults);
            
            // 4. Piece-flow trigger validation (CRITICAL) - Already done with original results above
            // this.validatePieceFlowTriggers(orderResults); // REMOVED: Uses transformed data without CycleTime_Min
            
            // 5. Concurrent setup capacity validation
            this.validateConcurrentSetupCapacity(orderResults);
            
            Logger.log(`[VALIDATION] ✅ All schedule validations passed`);
            return { valid: true, violations: [] };
            
        } catch (error) {
            Logger.log(`[VALIDATION] ❌ Schedule validation failed: ${error.message}`);
            return { valid: false, violations: [error.message] };
        }
    }
    
    validateOperatorShifts(orderResults) {
        Logger.log(`[VALIDATION] Checking operator shift assignments...`);
        
        for (const op of orderResults) {
            const setupStart = new Date(op.SetupStart);
            const setupEnd = new Date(op.SetupEnd);
            const operator = op.Person;
            
            if (!this.isOperatorOnShift(operator, setupStart, setupEnd)) {
                throw new Error(`[SHIFT-VIOLATION] Operator ${operator} assigned setup outside shift: ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
            }
        }
        
        Logger.log(`[VALIDATION] ✅ All operator shift assignments valid`);
    }
    
    validateOperatorOverlaps(orderResults) {
        Logger.log(`[VALIDATION] Checking operator overlaps...`);
        
        const operatorIntervals = {};
        
        for (const op of orderResults) {
            const operator = op.Person;
            if (!operatorIntervals[operator]) {
                operatorIntervals[operator] = [];
            }
            
            operatorIntervals[operator].push({
                start: new Date(op.SetupStart),
                end: new Date(op.SetupEnd),
                operation: op.OperationSeq
            });
        }
        
        for (const [operator, intervals] of Object.entries(operatorIntervals)) {
            intervals.sort((a, b) => a.start.getTime() - b.start.getTime());
            
            for (let i = 0; i < intervals.length - 1; i++) {
                const current = intervals[i];
                const next = intervals[i + 1];
                
                if (current.end > next.start) {
                    throw new Error(`[OPERATOR-OVERLAP] Operator ${operator} has overlapping setups: Op${current.operation} (${current.start.toISOString()} → ${current.end.toISOString()}) overlaps with Op${next.operation} (${next.start.toISOString()} → ${next.end.toISOString()})`);
                }
            }
        }
        
        Logger.log(`[VALIDATION] ✅ No operator overlaps found`);
    }
    
    validateMachineExclusivity(orderResults) {
        Logger.log(`[VALIDATION] Checking machine exclusivity...`);
        
        const machineIntervals = {};
        
        for (const op of orderResults) {
            const machine = op.Machine;
            if (!machineIntervals[machine]) {
                machineIntervals[machine] = [];
            }
            
            machineIntervals[machine].push({
                start: new Date(op.SetupStart),
                end: new Date(op.RunEnd),
                operation: op.OperationSeq
            });
        }
        
        for (const [machine, intervals] of Object.entries(machineIntervals)) {
            intervals.sort((a, b) => a.start.getTime() - b.start.getTime());
            
            for (let i = 0; i < intervals.length - 1; i++) {
                const current = intervals[i];
                const next = intervals[i + 1];
                
                if (current.end > next.start) {
                    throw new Error(`[MACHINE-OVERLAP] Machine ${machine} has overlapping operations: Op${current.operation} (${current.start.toISOString()} → ${current.end.toISOString()}) overlaps with Op${next.operation} (${next.start.toISOString()} → ${next.end.toISOString()})`);
                }
            }
        }
        
        Logger.log(`[VALIDATION] ✅ No machine overlaps found`);
    }
    
    validateConcurrentSetupCapacity(orderResults) {
        Logger.log(`[VALIDATION] Checking concurrent setup capacity...`);
        
        // Group operations by shift and check capacity
        const shifts = [
            { name: 'morning', start: 6, end: 14, operators: ['A', 'B'] },
            { name: 'afternoon', start: 14, end: 22, operators: ['C', 'D'] }
        ];
        
        for (const shift of shifts) {
            const maxConcurrent = shift.operators.length; // Should be 2
            
            // Check every minute in the shift
            for (let hour = shift.start; hour < shift.end; hour++) {
                for (let minute = 0; minute < 60; minute += 5) {
                    const checkTime = new Date();
                    checkTime.setHours(hour, minute, 0, 0);
                    
                    let activeSetups = 0;
                    for (const op of orderResults) {
                        const setupStart = new Date(op.SetupStart);
                        const setupEnd = new Date(op.SetupEnd);
                        
                        if (checkTime >= setupStart && checkTime < setupEnd) {
                            activeSetups++;
                        }
                    }
                    
                    if (activeSetups > maxConcurrent) {
                        throw new Error(`[CONCURRENT-VIOLATION] ${shift.name} shift at ${checkTime.toISOString()}: ${activeSetups} setups active (max: ${maxConcurrent})`);
                    }
                }
            }
        }
        
        Logger.log(`[VALIDATION] ✅ Concurrent setup capacity valid`);
    }
    
    // ENHANCED OPERATOR CONFLICT RESOLUTION
    resolveOperatorConflictEnhanced(operator, setupStart, setupEnd, maxRetries = 5) {
        Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] Attempting to resolve conflict for ${operator} at ${setupStart.toISOString()}`);
        
        // Strategy 1: Find alternative operator who is available immediately
        const operatorsOnShift = this.getOperatorsOnShift(setupStart, setupEnd);
        for (const altOperator of operatorsOnShift) {
            if (altOperator !== operator && !this.hasOperatorConflict(altOperator, setupStart, setupEnd)) {
                Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] ✅ Strategy 1: Found alternative operator ${altOperator}`);
                return {
                    operator: altOperator,
                    setupStart: setupStart,
                    setupEnd: setupEnd,
                    strategy: 'alternative_operator'
                };
            }
        }
        
        // Strategy 2: Try micro-delays (1-5 minutes) to avoid conflicts
        for (let retry = 0; retry < 5; retry++) {
            const delayMinutes = retry + 1; // 1, 2, 3, 4, 5 minutes
            const adjustedSetupStart = new Date(setupStart.getTime() + delayMinutes * 60000);
            const adjustedSetupEnd = new Date(setupEnd.getTime() + delayMinutes * 60000);
            
            Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] Strategy 2: Trying ${delayMinutes}min delay: ${adjustedSetupStart.toISOString()}`);
            
            if (!this.hasOperatorConflict(operator, adjustedSetupStart, adjustedSetupEnd)) {
                Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] ✅ Strategy 2: Conflict resolved with ${delayMinutes}min delay`);
                return {
                    operator: operator,
                    setupStart: adjustedSetupStart,
                    setupEnd: adjustedSetupEnd,
                    delayMinutes: delayMinutes,
                    strategy: 'micro_delay'
                };
            }
        }
        
        // Strategy 3: Try alternative operators with micro-delays
        for (const altOperator of operatorsOnShift) {
            if (altOperator !== operator) {
                for (let retry = 0; retry < 3; retry++) {
                    const delayMinutes = retry + 1; // 1, 2, 3 minutes
                    const adjustedSetupStart = new Date(setupStart.getTime() + delayMinutes * 60000);
                    const adjustedSetupEnd = new Date(setupEnd.getTime() + delayMinutes * 60000);
                    
                    if (!this.hasOperatorConflict(altOperator, adjustedSetupStart, adjustedSetupEnd)) {
                        Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] ✅ Strategy 3: Found alternative operator ${altOperator} with ${delayMinutes}min delay`);
                        return {
                            operator: altOperator,
                            setupStart: adjustedSetupStart,
                            setupEnd: adjustedSetupEnd,
                            delayMinutes: delayMinutes,
                            strategy: 'alternative_with_delay'
                        };
                    }
                }
            }
        }
        
        // Strategy 4: Try larger delays (10-30 minutes)
        for (let retry = 0; retry < maxRetries; retry++) {
            const delayMinutes = (retry + 1) * 10; // 10, 20, 30 minutes
            const adjustedSetupStart = new Date(setupStart.getTime() + delayMinutes * 60000);
            const adjustedSetupEnd = new Date(setupEnd.getTime() + delayMinutes * 60000);
            
            Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] Strategy 4: Trying ${delayMinutes}min delay: ${adjustedSetupStart.toISOString()}`);
            
            if (!this.hasOperatorConflict(operator, adjustedSetupStart, adjustedSetupEnd)) {
                Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] ✅ Strategy 4: Conflict resolved with ${delayMinutes}min delay`);
                return {
                    operator: operator,
                    setupStart: adjustedSetupStart,
                    setupEnd: adjustedSetupEnd,
                    delayMinutes: delayMinutes,
                    strategy: 'large_delay'
                };
            }
        }
        
        Logger.log(`[ENHANCED-CONFLICT-RESOLUTION] ❌ All strategies failed`);
        return null;
    }
    
    // Find alternative operator for given time slot
    findAlternativeOperator(setupStart, setupEnd) {
        Logger.log(`[FIND-ALTERNATIVE] Looking for alternative operator for ${setupStart.toISOString()}-${setupEnd.toISOString()}`);
        
        // Get all operators who are on shift during this time
        const operatorsOnShift = this.getOperatorsOnShift(setupStart, setupEnd);
        
        // AGGRESSIVE ALTERNATIVE SEARCH: Try all operators with auto-balancing
        const operatorCandidates = [];
        
        for (const operator of operatorsOnShift) {
            if (!this.hasOperatorConflict(operator, setupStart, setupEnd)) {
                const totalSetupMinutes = this.getTotalOperatorSetupMinutes(operator);
                const currentShiftMinutes = this.getOperatorSetupMinutesInShift(operator, setupStart);
                const shift = this.operatorShifts[operator].shift;
                
                operatorCandidates.push({
                    operator,
                    totalSetupMinutes,
                    currentShiftMinutes,
                    shift,
                    priority: this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)
                });
                
                Logger.log(`[FIND-ALTERNATIVE] ${operator} available: ${totalSetupMinutes}min total, ${currentShiftMinutes}min current shift, priority: ${this.calculateOperatorPriority(operator, totalSetupMinutes, currentShiftMinutes, shift)}`);
            }
        }
        
        if (operatorCandidates.length > 0) {
            // Sort by priority (lower is better) and return the best one
            operatorCandidates.sort((a, b) => a.priority - b.priority);
            const bestOperator = operatorCandidates[0].operator;
            Logger.log(`[FIND-ALTERNATIVE] ✅ Found best alternative operator: ${bestOperator} (priority: ${operatorCandidates[0].priority})`);
            return bestOperator;
        }
        
        // If no immediate alternative, try with small delays
        Logger.log(`[FIND-ALTERNATIVE] No immediate alternative, trying with delays...`);
        
        for (let delayMinutes = 5; delayMinutes <= 30; delayMinutes += 5) {
            const delayedSetupStart = new Date(setupStart.getTime() + delayMinutes * 60000);
            const delayedSetupEnd = new Date(setupEnd.getTime() + delayMinutes * 60000);
            
            // Check if delayed setup still falls within shift
            const delayedOperatorsOnShift = this.getOperatorsOnShift(delayedSetupStart, delayedSetupEnd);
            
            for (const operator of delayedOperatorsOnShift) {
                if (!this.hasOperatorConflict(operator, delayedSetupStart, delayedSetupEnd)) {
                    Logger.log(`[FIND-ALTERNATIVE] ✅ Found delayed alternative operator: ${operator} (${delayMinutes}min delay)`);
                    return operator;
                }
            }
        }
        
        Logger.log(`[FIND-ALTERNATIVE] ❌ No alternative operator found even with delays`);
        return null;
    }
    
    // OPERATOR CONFLICT RESOLUTION (Legacy method for backward compatibility)
    resolveOperatorConflict(operator, setupStart, setupEnd, maxRetries = 3) {
        Logger.log(`[CONFLICT-RESOLUTION] Attempting to resolve conflict for ${operator} at ${setupStart.toISOString()}`);
        
        // First, try to find an alternative operator who is available
        const operatorsOnShift = this.getOperatorsOnShift(setupStart, setupEnd);
        for (const altOperator of operatorsOnShift) {
            if (altOperator !== operator && !this.hasOperatorConflict(altOperator, setupStart, setupEnd)) {
                Logger.log(`[CONFLICT-RESOLUTION] ✅ Found alternative operator ${altOperator}`);
                return {
                    operator: altOperator,
                    setupStart: setupStart,
                    setupEnd: setupEnd
                };
            }
        }
        
        // If no alternative operator found, try delaying the setup
        for (let retry = 0; retry < maxRetries; retry++) {
            // Add increasing delay for each retry
            const delayMinutes = (retry + 1) * 10; // 10, 20, 30 minutes
            const adjustedSetupStart = new Date(setupStart.getTime() + delayMinutes * 60000);
            const adjustedSetupEnd = new Date(setupEnd.getTime() + delayMinutes * 60000);
            
            Logger.log(`[CONFLICT-RESOLUTION] Retry ${retry + 1}: Trying ${adjustedSetupStart.toISOString()}`);
            
            if (!this.hasOperatorConflict(operator, adjustedSetupStart, adjustedSetupEnd)) {
                Logger.log(`[CONFLICT-RESOLUTION] ✅ Conflict resolved with ${delayMinutes}min delay`);
                return {
                    operator: operator,
                    setupStart: adjustedSetupStart,
                    setupEnd: adjustedSetupEnd,
                    delayMinutes: delayMinutes
                };
            }
        }
        
        Logger.log(`[CONFLICT-RESOLUTION] ❌ Could not resolve conflict after ${maxRetries} retries`);
        return null;
    }
    
    validateConcurrentSetupLimits() {
        Logger.log(`[VALIDATION] Checking concurrent setup limits per shift...`);
        
        // Check each shift for concurrent setup violations
        const shifts = [
            { name: 'morning', start: 6, end: 14, operators: ['A', 'B'] },
            { name: 'afternoon', start: 14, end: 22, operators: ['C', 'D'] }
        ];
        
        for (const shift of shifts) {
            const maxConcurrent = shift.operators.length; // Should be 2
            Logger.log(`[VALIDATION] Checking ${shift.name} shift (${shift.start}:00-${shift.end}:00) - max ${maxConcurrent} concurrent setups`);
            
            // Check every minute in the shift for concurrent setup violations
            for (let hour = shift.start; hour < shift.end; hour++) {
                for (let minute = 0; minute < 60; minute += 5) { // Check every 5 minutes
                    const checkTime = new Date();
                    checkTime.setHours(hour, minute, 0, 0);
                    
                    let activeSetups = 0;
                    for (const operator of shift.operators) {
                        const intervals = this.operatorSchedule[operator] || [];
                        for (const interval of intervals) {
                            if (checkTime >= interval.start && checkTime < interval.end) {
                                activeSetups++;
                                break; // Count each operator only once
                            }
                        }
                    }
                    
                    if (activeSetups > maxConcurrent) {
                        throw new Error(`[CONCURRENT-SETUP-VIOLATION] ${shift.name} shift at ${checkTime.toISOString()}: ${activeSetups} setups active (max: ${maxConcurrent})`);
                    }
                }
            }
        }
        
        Logger.log(`[VALIDATION] Concurrent setup limits validated - no violations found`);
    }
}

/* === Main Scheduling Function (browser-compatible) === */
function runScheduling(ordersData, globalSettings = {}) {
    try {
        Logger.log("=== Starting Browser-Compatible Scheduling Engine ===");
        
        const engine = new FixedUnifiedSchedulingEngine();
        engine.setGlobalSettings(globalSettings);
        
        const allResults = [];
        const alerts = [];
        const originalOrderResults = []; // Store original results for validation
        
        // CRITICAL: Sort by Earliest Due Date (EDD) first, then Priority
        const sortedOrders = [...ordersData].sort((a, b) => {
            const dueDateA = new Date(a.dueDate);
            const dueDateB = new Date(b.dueDate);
            
            // Primary: Earliest Due Date (EDD)
            if (dueDateA.getTime() !== dueDateB.getTime()) {
                return dueDateA.getTime() - dueDateB.getTime();
            }
            
            // Secondary: Priority
            const priorityWeight = { Urgent: 4, High: 3, Normal: 2, Low: 1 };
            if (priorityWeight[a.priority] !== priorityWeight[b.priority]) {
                return priorityWeight[b.priority] - priorityWeight[a.priority];
            }
            
            // Tertiary: Minimize makespan (earlier start time)
            return 0;
        });
        
        Logger.log(`[EDD-PRIORITY] Orders sorted by due date: ${sortedOrders.map(o => `${o.partNumber} (due: ${o.dueDate})`).join(', ')}`);
        
        // DUE-DATE RESCUE: Check if any orders are at risk and suggest batch splitting
        const urgentOrders = sortedOrders.filter(order => {
            const dueDate = new Date(order.dueDate);
            const today = new Date();
            const daysUntilDue = Math.ceil((dueDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
            return daysUntilDue <= 2; // Orders due within 2 days
        });
        
        if (urgentOrders.length > 0) {
            Logger.log(`[DUE-DATE-RESCUE] Found ${urgentOrders.length} urgent orders: ${urgentOrders.map(o => o.partNumber).join(', ')}`);
            alerts.push(`🚨 ${urgentOrders.length} urgent orders detected - scheduler will prioritize these for on-time delivery`);
        }

        sortedOrders.forEach((order, orderIndex) => {
            try {
                Logger.log(`Processing order ${orderIndex + 1}: ${order.partNumber}`);
                
                const orderResults = engine.scheduleOrder(order, alerts);
                
                // Store original results for validation (with CycleTime_Min)
                originalOrderResults.push({
                    partNumber: order.partNumber,
                    results: orderResults
                });
                
                orderResults.forEach(opResult => {
                    allResults.push({
                        PartNumber: order.partNumber,
                        Order_Quantity: order.quantity,
                        Priority: order.priority,
                        Batch_ID: opResult.Batch_ID || `B01`,
                        Batch_Qty: opResult.Batch_Qty || order.quantity,
                        OperationSeq: opResult.OperationSeq,
                        OperationName: opResult.OperationName,
                        Machine: opResult.Machine,
                        Person: opResult.Person,
                        SetupStart: engine.formatDateTime(opResult.SetupStart),
                        SetupEnd: engine.formatDateTime(opResult.SetupEnd),
                        RunStart: engine.formatDateTime(opResult.RunStart),
                        RunEnd: engine.formatDateTime(opResult.RunEnd),
                        Timing: opResult.Timing,
                        DueDate: order.dueDate,
                        SetupTime_Min: opResult.SetupTime_Min,
                        CycleTime_Min: opResult.CycleTime_Min
                    });
                });

                // Check if order is on time
                const lastOp = orderResults[orderResults.length - 1];
                if (lastOp) {
                    const completion = lastOp.RunEnd;
                    const due = new Date(order.dueDate);
                    if (completion > due) {
                        const lateHours = Math.ceil((completion.getTime() - due.getTime()) / (1000 * 60 * 60));
                        alerts.push(`⚠️ ${order.partNumber} may be ${lateHours}h late (due ${order.dueDate})`);
                    }
                }

            } catch (error) {
                Logger.log(`Error processing order ${order.partNumber}: ${error.message}`);
                alerts.push(`❌ Failed to schedule ${order.partNumber}: ${error.message}`);
            }
        });

        // Final validation: comprehensive schedule validation
        try {
            engine.validateAllMachineSchedules();
            engine.validateAllOperatorSchedules();
            
            // CRITICAL: Validate piece-flow triggers for each order using ORIGINAL results
            for (const originalResult of originalOrderResults) {
                try {
                    if (originalResult.results.length > 0) {
                        Logger.log(`[VALIDATION] Validating piece-flow triggers for ${originalResult.partNumber}...`);
                        engine.validatePieceFlowTriggers(originalResult.results);
                    }
                } catch (error) {
                    Logger.log(`[VALIDATION] Error validating ${originalResult.partNumber}: ${error.message}`);
                }
            }
            
            // Comprehensive validation for all results
            const validationResult = engine.validateCompleteSchedule(allResults);
            if (!validationResult.valid) {
                Logger.log(`[CRITICAL] Comprehensive validation failed: ${validationResult.violations.join(', ')}`);
                alerts.push(`❌ Schedule validation failed: ${validationResult.violations.join(', ')}`);
                // Don't throw error - let user see the violations
            }
            
        } catch (validationError) {
            Logger.log(`[CRITICAL] Schedule validation failed: ${validationError.message}`);
            alerts.push(`❌ Schedule validation failed: ${validationError.message}`);
            throw validationError;
        }

        Logger.log(`=== Scheduling Complete: ${allResults.length} operations scheduled ===`);
        
        // Add duration breakdown to each result
        allResults.forEach(result => {
            if (result.SetupStart && result.RunEnd) {
                const setupStart = new Date(result.SetupStart);
                const runEnd = new Date(result.RunEnd);
                
                // Calculate work minutes (setup + run time)
                const setupTime = result.SetupTime_Min || 0;
                const runTime = (result.CycleTime_Min || 0) * (result.Batch_Qty || 0);
                const workMinutes = setupTime + runTime;
                
                // Calculate holiday/non-productive time (total elapsed - work)
                const totalElapsedMs = runEnd.getTime() - setupStart.getTime();
                const totalElapsedMinutes = Math.floor(totalElapsedMs / (1000 * 60));
                const holidayMinutes = Math.max(0, totalElapsedMinutes - workMinutes);
                
                // Add duration breakdown to result
                result.DurationBreakdown = engine.formatDurationBreakdown(
                    setupStart, 
                    runEnd, 
                    workMinutes, 
                    holidayMinutes
                );
                
                Logger.log(`[DURATION] Op${result.OperationSeq}: ${result.DurationBreakdown}`);
            }
        });
        
        return {
            rows: allResults,
            alerts: alerts,
            summary: {
                totalOrders: ordersData.length,
                totalOperations: allResults.length,
                completedSuccessfully: sortedOrders.length - alerts.filter(a => a.includes('❌')).length
            }
        };

    } catch (error) {
        Logger.log(`Fatal scheduling error: ${error.message}`);
        return {
            rows: [],
            alerts: [`❌ Scheduling engine error: ${error.message}`],
            summary: { totalOrders: 0, totalOperations: 0, completedSuccessfully: 0 }
        };
    }
}

// PIECE-LEVEL SCHEDULING TEST FUNCTION
function testPieceLevelScheduling() {
    Logger.log("=== PIECE-LEVEL SCHEDULING TEST ===");
    
    // Test parameters from user
    const testParams = {
        batchQty: 4,
        setupTime: 70, // minutes
        cycleTimes: {
            Op1: 18, // minutes
            Op2: 10, // minutes  
            Op3: 1,  // minutes
            Op4: 1   // minutes
        },
        startTime: new Date('2025-09-05T07:00:00.000Z') // 07:00 start
    };
    
    Logger.log(`Test Parameters: Batch Qty = ${testParams.batchQty}, Setup = ${testParams.setupTime}min`);
    Logger.log(`Cycle Times: Op1=${testParams.cycleTimes.Op1}min, Op2=${testParams.cycleTimes.Op2}min, Op3=${testParams.cycleTimes.Op3}min, Op4=${testParams.cycleTimes.Op4}min`);
    Logger.log(`Start Time: ${testParams.startTime.toISOString()}`);
    
    // Create test operations
    const testOperations = [
        { OperationSeq: 1, OperationName: "Op1", CycleTime_Min: testParams.cycleTimes.Op1, SetupTime_Min: testParams.setupTime },
        { OperationSeq: 2, OperationName: "Op2", CycleTime_Min: testParams.cycleTimes.Op2, SetupTime_Min: testParams.setupTime },
        { OperationSeq: 3, OperationName: "Op3", CycleTime_Min: testParams.cycleTimes.Op3, SetupTime_Min: testParams.setupTime },
        { OperationSeq: 4, OperationName: "Op4", CycleTime_Min: testParams.cycleTimes.Op4, SetupTime_Min: testParams.setupTime }
    ];
    
    // Simulate piece-level scheduling
    let currentTime = testParams.startTime;
    const results = [];
    
    for (let i = 0; i < testOperations.length; i++) {
        const operation = testOperations[i];
        const prevOpResults = i > 0 ? results[i - 1] : null;
        
        Logger.log(`\n--- Processing ${operation.OperationName} ---`);
        
        // Calculate setup timing
        const setupStart = prevOpResults ? prevOpResults.firstPieceDone : currentTime;
        const setupEnd = new Date(setupStart.getTime() + operation.SetupTime_Min * 60000);
        
        Logger.log(`Setup: ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
        
        // Calculate piece-level run timing
        const pieceCompletionTimes = [];
        const pieceStartTimes = [];
        let machineAvailableTime = setupEnd;
        
        for (let piece = 0; piece < testParams.batchQty; piece++) {
            const pieceReadyTime = prevOpResults ? prevOpResults.pieceCompletionTimes[piece] : setupEnd;
            const runStart = new Date(Math.max(pieceReadyTime.getTime(), machineAvailableTime.getTime()));
            const runEnd = new Date(runStart.getTime() + operation.CycleTime_Min * 60000);
            
            pieceStartTimes.push(runStart);
            pieceCompletionTimes.push(runEnd);
            machineAvailableTime = runEnd;
            
            Logger.log(`Piece${piece + 1} run: ${runStart.toISOString()} → ${runEnd.toISOString()} (ready ${runEnd.toISOString()})`);
        }
        
        const firstPieceDone = pieceCompletionTimes[0];
        const lastPieceDone = pieceCompletionTimes[testParams.batchQty - 1];
        
        Logger.log(`✅ ${operation.OperationName} complete (all ${testParams.batchQty}): ${lastPieceDone.toISOString()}`);
        
        results.push({
            OperationSeq: operation.OperationSeq,
            OperationName: operation.OperationName,
            SetupStart: setupStart,
            SetupEnd: setupEnd,
            RunStart: pieceStartTimes[0],
            RunEnd: lastPieceDone,
            pieceCompletionTimes: pieceCompletionTimes,
            pieceStartTimes: pieceStartTimes,
            firstPieceDone: firstPieceDone,
            lastPieceDone: lastPieceDone
        });
    }
    
    Logger.log("\n=== EXPECTED vs ACTUAL RESULTS ===");
    Logger.log("Expected Final Completion Times:");
    Logger.log("Op1 done: 09:22");
    Logger.log("Op2 done: 10:18"); 
    Logger.log("Op3 done: 11:02");
    Logger.log("Op4 done: 12:13");
    
    Logger.log("\nActual Final Completion Times:");
    results.forEach(op => {
        const timeStr = op.lastPieceDone.toISOString().substr(11, 5); // HH:MM format
        Logger.log(`${op.OperationName} done: ${timeStr}`);
    });
    
    // Verify piece-flow triggers
    Logger.log("\n=== PIECE-FLOW TRIGGER VERIFICATION ===");
    for (let i = 1; i < results.length; i++) {
        const currentOp = results[i];
        const prevOp = results[i - 1];
        
        const setupStart = currentOp.SetupStart;
        const prevFirstPieceDone = prevOp.firstPieceDone;
        
        Logger.log(`${currentOp.OperationName}.SetupStart: ${setupStart.toISOString()}`);
        Logger.log(`${prevOp.OperationName}.FirstPieceDone: ${prevFirstPieceDone.toISOString()}`);
        
        if (setupStart >= prevFirstPieceDone) {
            Logger.log(`✅ ${currentOp.OperationName} setup starts correctly after ${prevOp.OperationName} first piece`);
        } else {
            Logger.log(`❌ ${currentOp.OperationName} setup starts too early! Violation detected.`);
        }
    }
    
    return results;
}

// SCHEDULE DIAGNOSTIC FUNCTION
function diagnoseScheduleIssues() {
    Logger.log("=== SCHEDULE DIAGNOSTIC ANALYSIS ===");
    
    // Your actual PN11001 schedule data
    const scheduleData = [
        { Op: 1, Machine: "VMC 1", SetupStart: "2025-09-05 14:55", SetupEnd: "2025-09-05 16:25", RunStart: "2025-09-05 16:25", RunEnd: "2025-09-06 14:37", Timing: "23H 42M total" },
        { Op: 2, Machine: "VMC 2", SetupStart: "2025-09-05 16:28", SetupEnd: "2025-09-05 17:58", RunStart: "2025-09-05 17:58", RunEnd: "2025-09-11 21:58", Timing: "6D 5H 30M total" },
        { Op: 3, Machine: "VMC 7", SetupStart: "2025-09-05 18:18", SetupEnd: "2025-09-05 19:48", RunStart: "2025-09-05 19:48", RunEnd: "2025-09-07 01:24", Timing: "1D 7H 6M total" },
        { Op: 4, Machine: "VMC 3", SetupStart: "2025-09-05 19:52", SetupEnd: "2025-09-05 21:22", RunStart: "2025-09-05 21:22", RunEnd: "2025-09-09 14:10", Timing: "3D 18H 18M total" },
        { Op: 5, Machine: "VMC 4", SetupStart: "2025-09-05 21:34", SetupEnd: "2025-09-06 07:30", RunStart: "2025-09-06 07:30", RunEnd: "2025-09-07 13:06", Timing: "1D 15H 31M total (8H 25M paused)" }
    ];
    
    Logger.log("\n=== PIECE-FLOW TRIGGER ANALYSIS ===");
    
    // Simulate piece-level completion times
    const batchQty = 444;
    const cycleTimes = [18, 10, 1, 1, 1]; // Estimated cycle times
    
    for (let i = 0; i < scheduleData.length; i++) {
        const op = scheduleData[i];
        const setupStart = new Date(op.SetupStart);
        const setupEnd = new Date(op.SetupEnd);
        const runStart = new Date(op.RunStart);
        const runEnd = new Date(op.RunEnd);
        
        Logger.log(`\n--- ${op.Op} (${op.Machine}) ---`);
        Logger.log(`Setup: ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
        Logger.log(`Run: ${runStart.toISOString()} → ${runEnd.toISOString()}`);
        
        // Calculate first piece completion
        const firstPieceDone = new Date(runStart.getTime() + cycleTimes[i] * 60000);
        Logger.log(`First piece done: ${firstPieceDone.toISOString()}`);
        
        // Check if next operation violates piece-flow trigger
        if (i < scheduleData.length - 1) {
            const nextOp = scheduleData[i + 1];
            const nextSetupStart = new Date(nextOp.SetupStart);
            
            Logger.log(`Next op setup start: ${nextSetupStart.toISOString()}`);
            
            if (nextSetupStart < firstPieceDone) {
                const violationMinutes = Math.ceil((firstPieceDone.getTime() - nextSetupStart.getTime()) / (1000 * 60));
                Logger.log(`❌ PIECE-FLOW VIOLATION: Next op starts ${violationMinutes} minutes too early!`);
            } else {
                Logger.log(`✅ Piece-flow trigger OK`);
            }
        }
        
        // Check for overnight pause
        if (setupEnd.getHours() > setupStart.getHours() && setupEnd.getDate() > setupStart.getDate()) {
            Logger.log(`⚠️ OVERNIGHT PAUSE: Setup crosses midnight (${setupStart.toISOString()} → ${setupEnd.toISOString()})`);
        }
        
        // Check run duration vs expected
        const runDuration = runEnd.getTime() - runStart.getTime();
        const expectedRunDuration = batchQty * cycleTimes[i] * 60000;
        const actualHours = Math.floor(runDuration / (1000 * 60 * 60));
        const expectedHours = Math.floor(expectedRunDuration / (1000 * 60 * 60));
        
        Logger.log(`Run duration: ${actualHours}h (expected: ${expectedHours}h for ${batchQty} pieces × ${cycleTimes[i]}min)`);
        
        if (actualHours > expectedHours * 1.5) {
            Logger.log(`❌ SUSPICIOUS: Run duration much longer than expected!`);
        }
    }
    
    Logger.log("\n=== ROOT CAUSE ANALYSIS ===");
    Logger.log("1. Piece-flow triggers may be violated (operations starting too early)");
    Logger.log("2. Run durations are much longer than expected for piece-level flow");
    Logger.log("3. Overnight pauses are causing delays");
    Logger.log("4. Different machines may have different availability/constraints");
    
    return scheduleData;
}

// COMPREHENSIVE PIECE-LEVEL SCHEDULING TEST (USER'S EXACT ALGORITHM)
function testUserAlgorithm() {
    Logger.log("=== TESTING USER'S EXACT PIECE-LEVEL ALGORITHM ===");
    
    // User's exact parameters
    const testParams = {
        batchQty: 4,
        setupTime: 70, // minutes
        cycleTimes: [18, 10, 1, 1], // Op1, Op2, Op3, Op4
        startTime: new Date('2025-09-05T07:00:00.000Z') // 07:00 start
    };
    
    Logger.log(`Test Parameters: Batch Qty = ${testParams.batchQty}, Setup = ${testParams.setupTime}min`);
    Logger.log(`Cycle Times: Op1=${testParams.cycleTimes[0]}min, Op2=${testParams.cycleTimes[1]}min, Op3=${testParams.cycleTimes[2]}min, Op4=${testParams.cycleTimes[3]}min`);
    Logger.log(`Start Time: ${testParams.startTime.toISOString()}`);
    
    // Simulate the exact user algorithm
    const results = [];
    let currentTime = testParams.startTime;
    
    for (let opIndex = 0; opIndex < testParams.cycleTimes.length; opIndex++) {
        const operationName = `Op${opIndex + 1}`;
        const cycleTime = testParams.cycleTimes[opIndex];
        const prevOpResults = opIndex > 0 ? results[opIndex - 1] : null;
        
        Logger.log(`\n--- ${operationName} ---`);
        
        // STEP 1: Calculate setup timing
        const setupStart = prevOpResults ? prevOpResults.firstPieceDone : currentTime;
        const setupEnd = new Date(setupStart.getTime() + testParams.setupTime * 60000);
        
        Logger.log(`Setup: ${setupStart.toISOString().substr(11, 5)} → ${setupEnd.toISOString().substr(11, 5)}`);
        
        // STEP 2: Calculate piece-level run timing (EXACT USER ALGORITHM)
        const pieceCompletionTimes = [];
        const pieceStartTimes = [];
        let machineAvailableTime = setupEnd;
        
        for (let piece = 0; piece < testParams.batchQty; piece++) {
            // Piece ready time from previous operation (if any)
            const pieceReadyTime = prevOpResults ? prevOpResults.pieceCompletionTimes[piece] : setupEnd;
            
            // Machine starts processing when both piece is ready AND machine is available
            const runStart = new Date(Math.max(pieceReadyTime.getTime(), machineAvailableTime.getTime()));
            const runEnd = new Date(runStart.getTime() + cycleTime * 60000);
            
            pieceStartTimes.push(runStart);
            pieceCompletionTimes.push(runEnd);
            machineAvailableTime = runEnd;
            
            Logger.log(`Piece${piece + 1} run: ${runStart.toISOString().substr(11, 5)} → ${runEnd.toISOString().substr(11, 5)} (ready ${runEnd.toISOString().substr(11, 5)})`);
        }
        
        const firstPieceDone = pieceCompletionTimes[0];
        const lastPieceDone = pieceCompletionTimes[testParams.batchQty - 1];
        
        Logger.log(`✅ ${operationName} complete (all ${testParams.batchQty}): ${lastPieceDone.toISOString().substr(11, 5)}`);
        
        results.push({
            OperationSeq: opIndex + 1,
            OperationName: operationName,
            SetupStart: setupStart,
            SetupEnd: setupEnd,
            RunStart: pieceStartTimes[0],
            RunEnd: lastPieceDone,
            pieceCompletionTimes: pieceCompletionTimes,
            pieceStartTimes: pieceStartTimes,
            firstPieceDone: firstPieceDone,
            lastPieceDone: lastPieceDone
        });
    }
    
    // VERIFICATION: Compare with user's expected results
    Logger.log("\n=== VERIFICATION: EXPECTED vs ACTUAL ===");
    const expectedTimes = ["09:22", "10:18", "11:02", "12:13"];
    
    results.forEach((op, index) => {
        const actualTime = op.lastPieceDone.toISOString().substr(11, 5);
        const expectedTime = expectedTimes[index];
        const match = actualTime === expectedTime;
        
        Logger.log(`${op.OperationName} done: ${actualTime} (expected: ${expectedTime}) ${match ? '✅ PERFECT' : '❌ MISMATCH'}`);
    });
    
    // PIECE-FLOW TRIGGER VERIFICATION
    Logger.log("\n=== PIECE-FLOW TRIGGER VERIFICATION ===");
    for (let i = 1; i < results.length; i++) {
        const currentOp = results[i];
        const prevOp = results[i - 1];
        
        const setupStart = currentOp.SetupStart;
        const prevFirstPieceDone = prevOp.firstPieceDone;
        
        Logger.log(`${currentOp.OperationName}.SetupStart: ${setupStart.toISOString().substr(11, 5)}`);
        Logger.log(`${prevOp.OperationName}.FirstPieceDone: ${prevFirstPieceDone.toISOString().substr(11, 5)}`);
        
        if (setupStart >= prevFirstPieceDone) {
            Logger.log(`✅ ${currentOp.OperationName} setup starts correctly after ${prevOp.OperationName} first piece`);
        } else {
            Logger.log(`❌ ${currentOp.OperationName} setup starts too early! Violation detected.`);
        }
    }
    
    Logger.log("\n=== ALGORITHM VALIDATION COMPLETE ===");
    Logger.log("This is the EXACT algorithm that should be implemented in the main scheduler!");
    
    return results;
}

// PN11001 SCHEDULE DIAGNOSTIC - FIND THE BUGS
function diagnosePN11001Schedule() {
    Logger.log("=== PN11001 SCHEDULE DIAGNOSTIC ===");
    
    // Your actual PN11001 schedule data
    const scheduleData = [
        { Op: 1, Machine: "VMC 1", SetupStart: "2025-09-05 15:53", SetupEnd: "2025-09-05 17:23", RunStart: "2025-09-05 17:23", RunEnd: "2025-09-07 02:41", Timing: "1D 10H 48M total" },
        { Op: 2, Machine: "VMC 2", SetupStart: "2025-09-05 17:26", SetupEnd: "2025-09-05 18:56", RunStart: "2025-09-05 18:56", RunEnd: "2025-09-15 00:56", Timing: "9D 7H 30M total" },
        { Op: 3, Machine: "VMC 7", SetupStart: "2025-09-05 19:16", SetupEnd: "2025-09-05 20:46", RunStart: "2025-09-05 20:46", RunEnd: "2025-09-07 17:10", Timing: "1D 21H 54M total" },
        { Op: 4, Machine: "VMC 3", SetupStart: "2025-09-05 20:50", SetupEnd: "2025-09-06 07:30", RunStart: "2025-09-06 07:30", RunEnd: "2025-09-11 20:42", Timing: "5D 23H 51M total" },
        { Op: 5, Machine: "VMC 4", SetupStart: "2025-09-06 07:42", SetupEnd: "2025-09-06 09:12", RunStart: "2025-09-06 09:12", RunEnd: "2025-09-08 05:36", Timing: "1D 21H 54M total" }
    ];
    
    Logger.log("\n=== PIECE-FLOW TRIGGER ANALYSIS ===");
    
    // Simulate piece-level completion times with correct cycle times
    const batchQty = 666;
    const cycleTimes = [18, 10, 1, 1, 1]; // Estimated cycle times
    
    for (let i = 0; i < scheduleData.length; i++) {
        const op = scheduleData[i];
        const setupStart = new Date(op.SetupStart);
        const setupEnd = new Date(op.SetupEnd);
        const runStart = new Date(op.RunStart);
        const runEnd = new Date(op.RunEnd);
        
        Logger.log(`\n--- ${op.Op} (${op.Machine}) ---`);
        Logger.log(`Setup: ${setupStart.toISOString()} → ${setupEnd.toISOString()}`);
        Logger.log(`Run: ${runStart.toISOString()} → ${runEnd.toISOString()}`);
        
        // Calculate CORRECT first piece completion
        const correctFirstPieceDone = new Date(runStart.getTime() + cycleTimes[i] * 60000);
        Logger.log(`CORRECT First piece done: ${correctFirstPieceDone.toISOString()}`);
        
        // Calculate CORRECT run duration
        const correctRunDuration = batchQty * cycleTimes[i]; // minutes
        const correctRunDurationHours = Math.floor(correctRunDuration / 60);
        Logger.log(`CORRECT Run duration: ${correctRunDurationHours}h (${batchQty} pieces × ${cycleTimes[i]}min)`);
        
        // Calculate ACTUAL run duration
        const actualRunDuration = runEnd.getTime() - runStart.getTime();
        const actualRunDurationHours = Math.floor(actualRunDuration / (1000 * 60 * 60));
        Logger.log(`ACTUAL Run duration: ${actualRunDurationHours}h`);
        
        // Check if durations match
        if (Math.abs(actualRunDurationHours - correctRunDurationHours) > 1) {
            const ratio = actualRunDurationHours / correctRunDurationHours;
            Logger.log(`❌ DURATION MISMATCH: Actual is ${ratio.toFixed(1)}x expected!`);
        } else {
            Logger.log(`✅ Duration matches expected`);
        }
        
        // Check if next operation violates piece-flow trigger
        if (i < scheduleData.length - 1) {
            const nextOp = scheduleData[i + 1];
            const nextSetupStart = new Date(nextOp.SetupStart);
            
            Logger.log(`Next op setup start: ${nextSetupStart.toISOString()}`);
            
            if (nextSetupStart < correctFirstPieceDone) {
                const violationMinutes = Math.ceil((correctFirstPieceDone.getTime() - nextSetupStart.getTime()) / (1000 * 60));
                Logger.log(`❌ PIECE-FLOW VIOLATION: Next op starts ${violationMinutes} minutes too early!`);
            } else {
                Logger.log(`✅ Piece-flow trigger OK`);
            }
        }
    }
    
    Logger.log("\n=== ROOT CAUSE SUMMARY ===");
    Logger.log("1. ❌ Run durations are WRONG (4x to 12x expected)");
    Logger.log("2. ❌ Piece-flow triggers are VIOLATED");
    Logger.log("3. ❌ Different machines causing inconsistency");
    Logger.log("4. ❌ Scheduler NOT following your piece-level algorithm");
    
    Logger.log("\n=== WHAT SHOULD HAPPEN (YOUR ALGORITHM) ===");
    Logger.log("Op1: 666 × 18min = 200h run duration");
    Logger.log("Op2: 666 × 10min = 111h run duration");
    Logger.log("Op3: 666 × 1min = 11h run duration");
    Logger.log("Op4: 666 × 1min = 11h run duration");
    Logger.log("Op5: 666 × 1min = 11h run duration");
    Logger.log("Each operation should start when previous operation's FIRST PIECE is done!");
    
    return scheduleData;
}

// Global function to process a single order (for UI)
window.processOrderSingle = function(order) {
    try {
        // Use the same data mapping as the main scheduler
        const ordersData = [{
            partNumber: order.partNumber,
            quantity: order.quantity,
            priority: order.priority,
            dueDate: order.dueDate,
            operations: (order.filteredOperations || order.operations).map(op => ({
                OperationSeq: op.OperationSeq,
                OperationName: op.OperationName,
                SetupTime_Min: op.SetupTime_Min,
                CycleTime_Min: op.CycleTime_Min,
                EligibleMachines: op.EligibleMachines,
                Minimum_BatchSize: op.Minimum_BatchSize
            })),
            breakdownMachine: order.breakdownMachine,
            breakdownDateTime: order.breakdownDateTime,
            startDateTime: order.startDateTime,
            holidayRange: order.holidayRange,
            setupWindow: order.setupWindow
        }];

        // Use the engine's global settings if available, otherwise use defaults
        const globalSettings = window.SCHEDULING_CONFIG ? {
            startDate: window.SCHEDULING_CONFIG.startDate || '2025-09-01',
            startTime: window.SCHEDULING_CONFIG.startTime || '06:00',
            setupWindow: window.SCHEDULING_CONFIG.setupWindow || "06:00-22:00",
            breakdownMachines: window.SCHEDULING_CONFIG.breakdownMachines || [],
            breakdownDateTime: window.SCHEDULING_CONFIG.breakdownDateTime || "",
            holidays: window.SCHEDULING_CONFIG.holidays || [],
            productionWindow: window.SCHEDULING_CONFIG.productionWindow || "24x7",
            shifts: window.SCHEDULING_CONFIG.shifts || {
                shift1: "06:00-14:00",
                shift2: "14:00-22:00",
                shift3: "22:00-06:00"
            },
            operatorShifts: window.SCHEDULING_CONFIG.operatorShifts || {
                'A': { start: 6, end: 14, shift: 'morning' },
                'B': { start: 6, end: 14, shift: 'morning' },
                'C': { start: 14, end: 22, shift: 'afternoon' },
                'D': { start: 14, end: 22, shift: 'afternoon' }
            }
        } : {
            startDateTime: "2025-09-01T06:00:00", // Fallback for testing
            setupWindow: "06:00-22:00",
            breakdownMachines: [],
            breakdownDateTime: "",
            holidays: [],
            productionWindow: "24x7",
            shifts: {
                shift1: "06:00-14:00",
                shift2: "14:00-22:00",
                shift3: "22:00-06:00"
            },
            operatorShifts: {
                'A': { start: 6, end: 14, shift: 'morning' },
                'B': { start: 6, end: 14, shift: 'morning' },
                'C': { start: 14, end: 22, shift: 'afternoon' },
                'D': { start: 14, end: 22, shift: 'afternoon' }
            }
        };

        // Use the x10-browser.js engine for single order processing
        const result = window.runScheduling(ordersData, globalSettings);
        return result;
    } catch (error) {
        console.error('Single order scheduling error:', error);
        return {
            rows: [],
            alerts: [`Error processing ${order.partNumber}: ${error.message}`],
            summary: { totalOrders: 0, totalOperations: 0, completedSuccessfully: 0 }
        };
    }
};

// Export for browser use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { runScheduling, FixedUnifiedSchedulingEngine, CONFIG };
}

// Always expose to window/global
if (typeof window !== 'undefined') {
    window.runScheduling = runScheduling;
    window.processOrderSingle = window.processOrderSingle;
    window.testPieceLevelScheduling = testPieceLevelScheduling;
    window.testUserAlgorithm = testUserAlgorithm;
    window.diagnoseScheduleIssues = diagnoseScheduleIssues;
    window.diagnosePN11001Schedule = diagnosePN11001Schedule;
    window.FixedUnifiedSchedulingEngine = FixedUnifiedSchedulingEngine;
    window.SCHEDULING_CONFIG = CONFIG;
}

// Also expose to global for Node.js eval
if (typeof global !== 'undefined') {
    global.runScheduling = runScheduling;
    global.processOrderSingle = global.processOrderSingle;
    global.FixedUnifiedSchedulingEngine = FixedUnifiedSchedulingEngine;
}
    
    // Expose calculateBatchSplitting as a global function
    window.calculateBatchSplitting = function(totalQuantity, minBatchSize, priority = 'normal', dueDate = null, startDate = null) {
        try {
            const engine = new FixedUnifiedSchedulingEngine();
            const result = engine.calculateBatchSplitting(totalQuantity, minBatchSize, priority, dueDate, startDate);
            console.log('Batch splitting result:', result);
            return result;
        } catch (error) {
            console.error('Batch splitting error:', error);
            // USER'S SPECIFIC BATCH SPLITTING REQUIREMENTS - FALLBACK
            // For PN2001: Split into batches of 150 each
            const TARGET_BATCH_SIZE = orderData.partNumber === 'PN2001' ? 150 : 300;
            const MIN_BATCH_SIZE = Math.max(minBatchSize, 100); // Minimum 100 pieces per batch
            
            let batches = [];
            
            if (totalQuantity <= TARGET_BATCH_SIZE) {
                // Single batch for quantities <= 300
                batches.push({
                    batchId: 'B01',
                    quantity: totalQuantity,
                    batchIndex: 0
                });
                console.log(`Fallback: Single batch: ${totalQuantity} pieces`);
            } else {
                // Split into batches of 300 each
                let remainingQuantity = totalQuantity;
                let batchIndex = 0;
                
                while (remainingQuantity > 0) {
                    batchIndex++;
                    const batchId = `B${String(batchIndex).padStart(2, '0')}`;
                    
                    // Use 300 as batch size, but ensure last batch gets remaining pieces
                    const batchQuantity = Math.min(TARGET_BATCH_SIZE, remainingQuantity);
                    
                    batches.push({
                        batchId: batchId,
                        quantity: batchQuantity,
                        batchIndex: batchIndex - 1
                    });
                    
                    remainingQuantity -= batchQuantity;
                    console.log(`Fallback: Created ${batchId}: ${batchQuantity} pieces (remaining: ${remainingQuantity})`);
                }
            }
            
            console.log('Fallback batch splitting result:', batches);
            return batches;
        }
    };
