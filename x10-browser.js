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
    calculateBatchSplitting(totalQuantity, minBatchSize, priority = 'normal', dueDate = null, startDate = null) {
        Logger.log(`[BATCH-CALC] Calculating batch splitting: ${totalQuantity} pieces, min batch size: ${minBatchSize}, priority: ${priority}`);
        
        // SMART BATCH SPLITTING ALGORITHM
        // Use 200-300 as default batch size for better efficiency
        const DEFAULT_BATCH_SIZE = 250; // Optimal batch size
        const MIN_BATCH_SIZE = Math.max(minBatchSize, 100); // Minimum 100 pieces per batch
        
        // PRIORITY-BASED BATCH SPLITTING LOGIC
        let maxBatches;
        let batchSizeMultiplier = 1.0;
        
        // Calculate deadline urgency if due date is provided
        let deadlineUrgency = 1.0;
        if (dueDate && startDate) {
            const dueDateObj = new Date(dueDate);
            const startDateObj = new Date(startDate);
            const daysToDeadline = Math.ceil((dueDateObj.getTime() - startDateObj.getTime()) / (1000 * 60 * 60 * 24));
            
            if (daysToDeadline <= 1) {
                deadlineUrgency = 0.5; // Very urgent - allow more batches
            } else if (daysToDeadline <= 3) {
                deadlineUrgency = 0.7; // Urgent - allow more batches
            } else if (daysToDeadline <= 7) {
                deadlineUrgency = 0.8; // Somewhat urgent
            } else {
                deadlineUrgency = 1.0; // Normal timeline
            }
            
            Logger.log(`[BATCH-CALC] Deadline urgency: ${daysToDeadline} days = ${deadlineUrgency} urgency factor`);
        }
        
        // PRIORITY-BASED BATCH LIMITS
        if (priority.toLowerCase() === 'urgent' || priority.toLowerCase() === 'high') {
            // High priority: Allow more batches for faster completion
            if (totalQuantity <= 500) {
                maxBatches = 4; // High priority: max 4 batches
            } else if (totalQuantity <= 1000) {
                maxBatches = 5; // High priority: max 5 batches
            } else {
                maxBatches = 6; // High priority: max 6 batches
            }
            batchSizeMultiplier = 0.8; // Smaller batches for faster completion
            Logger.log(`[BATCH-CALC] High priority detected - allowing up to ${maxBatches} batches`);
        } else if (priority.toLowerCase() === 'critical' || priority.toLowerCase() === 'emergency') {
            // Critical priority: Maximum batches for fastest completion
            if (totalQuantity <= 500) {
                maxBatches = 5; // Critical: max 5 batches
            } else if (totalQuantity <= 1000) {
                maxBatches = 6; // Critical: max 6 batches
            } else {
                maxBatches = 8; // Critical: max 8 batches
            }
            batchSizeMultiplier = 0.6; // Much smaller batches for fastest completion
            Logger.log(`[BATCH-CALC] Critical priority detected - allowing up to ${maxBatches} batches`);
        } else {
            // Normal priority: Standard batch limits
            if (totalQuantity <= 500) {
                maxBatches = 3; // Normal: max 3 batches
            } else if (totalQuantity <= 1000) {
                maxBatches = 4; // Normal: max 4 batches
            } else {
                maxBatches = 5; // Normal: max 5 batches
            }
            batchSizeMultiplier = 1.0; // Standard batch sizes
        }
        
        // Apply deadline urgency to batch limits
        if (deadlineUrgency < 1.0) {
            maxBatches = Math.ceil(maxBatches / deadlineUrgency); // Allow more batches for urgent deadlines
            batchSizeMultiplier *= deadlineUrgency; // Smaller batches for urgent deadlines
            Logger.log(`[BATCH-CALC] Deadline urgency applied - adjusted to ${maxBatches} max batches`);
        }
        
        // Calculate optimal batch size based on total quantity and priority
        let optimalBatchSize;
        if (totalQuantity <= 300) {
            // Small quantities: use smaller batches
            optimalBatchSize = Math.max(MIN_BATCH_SIZE, Math.ceil(totalQuantity / 2));
        } else if (totalQuantity <= 600) {
            // Medium quantities: use default batch size
            optimalBatchSize = DEFAULT_BATCH_SIZE;
        } else if (totalQuantity <= 1000) {
            // Large quantities: use larger batches
            optimalBatchSize = Math.min(DEFAULT_BATCH_SIZE + 50, Math.ceil(totalQuantity / 3));
        } else {
            // Very large quantities: use maximum efficient batch size
            optimalBatchSize = Math.min(300, Math.ceil(totalQuantity / 4));
        }
        
        // Apply priority-based batch size adjustment
        optimalBatchSize = Math.ceil(optimalBatchSize * batchSizeMultiplier);
        
        // Calculate number of batches
        let numBatches = Math.ceil(totalQuantity / optimalBatchSize);
        
        // Ensure we don't exceed maximum batches
        if (numBatches > maxBatches) {
            numBatches = maxBatches;
            optimalBatchSize = Math.ceil(totalQuantity / numBatches);
            Logger.log(`[BATCH-CALC] Limited to ${maxBatches} batches, adjusted batch size to ${optimalBatchSize}`);
        }
        
        // Ensure we don't create batches smaller than minimum
        if (optimalBatchSize < MIN_BATCH_SIZE) {
            optimalBatchSize = MIN_BATCH_SIZE;
            numBatches = Math.ceil(totalQuantity / optimalBatchSize);
            Logger.log(`[BATCH-CALC] Adjusted to respect minimum batch size ${MIN_BATCH_SIZE}`);
        }
        
        // Calculate final batch size (distribute evenly)
        const batchSize = Math.ceil(totalQuantity / numBatches);
        
        const batches = [];
        let remainingQuantity = totalQuantity;
        
        for (let i = 0; i < numBatches; i++) {
            const batchId = `B${String(i + 1).padStart(2, '0')}`;
            const batchQuantity = Math.min(batchSize, remainingQuantity);
            
            batches.push({
                batchId: batchId,
                quantity: batchQuantity,
                batchIndex: i
            });
            
            remainingQuantity -= batchQuantity;
            
            Logger.log(`[BATCH-CALC] Created ${batchId}: ${batchQuantity} pieces (remaining: ${remainingQuantity})`);
        }
        
        Logger.log(`[BATCH-CALC] Final result: ${batches.length} batches created (max allowed: ${maxBatches})`);
        return batches;
    }

    setGlobalSettings(settings) {
        this.globalSettings = settings || {};
        
        // Parse global start date time
        if (settings.startDateTime) {
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
            const batches = this.calculateBatchSplitting(totalQuantity, minBatchSize, orderData.priority, orderData.dueDate, orderData.startDateTime);
            
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

            Logger.log(`=== ORDER ${orderData.partNumber} SCHEDULING COMPLETE ===\n`);
            return orderResults;
        } catch (error) {
            Logger.log(`Error scheduling order: ${error.message}`);
            throw error;
        }
    }

    scheduleOperation(operation, orderData, batchQty, previousSequenceFirstPieceDone, sequenceIndex, previousOpRunEnd = null) {
        // RULE 1: Check machine eligibility for this part
        const eligibleMachines = operation.EligibleMachines || this.allMachines;
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
            Logger.log(`Sequence ${operation.OperationSeq} can start after previous sequence's first piece: ${previousSequenceFirstPieceDone.toISOString()}`);
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
        const selectedPerson = this.selectOptimalPerson(orderData, preliminaryTiming.setupStart, preliminaryTiming.setupEnd);
        
        // RULE 6.1: Handle setup spillover across shift boundaries
        const setupDuration = operation.SetupTime_Min || 0;
        const spilloverResult = this.handleSetupSpillover(selectedPerson, preliminaryTiming.setupStart, preliminaryTiming.setupEnd, setupDuration);
        
        // Use actual setup times after spillover handling
        let actualSetupStart = spilloverResult.actualSetupStart;
        let actualSetupEnd = spilloverResult.actualSetupEnd;
        let actualOperator = spilloverResult.operator;
        
        // CRITICAL FIX: Enhanced operator conflict resolution for multiple orders
        // Try multiple operators and timing adjustments to avoid conflicts
        let operatorFound = false;
        let maxAttempts = 15; // Increased attempts for multiple orders
        let attempt = 0;
        
        while (!operatorFound && attempt < maxAttempts) {
            attempt++;
            Logger.log(`[OPERATOR-SELECTION] Attempt ${attempt}: Trying ${actualOperator} at ${actualSetupStart.toISOString()}`);
            
            if (!this.hasOperatorConflict(actualOperator, actualSetupStart, actualSetupEnd)) {
                operatorFound = true;
                Logger.log(`[OPERATOR-SELECTION] ✅ Successfully selected ${actualOperator} at ${actualSetupStart.toISOString()}`);
            } else {
                Logger.log(`[OPERATOR-SELECTION] ❌ Conflict detected for ${actualOperator} at ${actualSetupStart.toISOString()}`);
                
                // Try alternative operators first
                const operatorsOnShift = this.getOperatorsOnShift(actualSetupStart, actualSetupEnd);
                let alternativeFound = false;
                
                for (const altOperator of operatorsOnShift) {
                    if (altOperator !== actualOperator && !this.hasOperatorConflict(altOperator, actualSetupStart, actualSetupEnd)) {
                        actualOperator = altOperator;
                        alternativeFound = true;
                        Logger.log(`[OPERATOR-SELECTION] ✅ Found alternative operator ${actualOperator}`);
                        break;
                    }
                }
                
                if (!alternativeFound) {
                    // If no alternative operator, try delaying the setup with smarter delays
                    let delayMinutes;
                    if (attempt <= 5) {
                        delayMinutes = attempt * 30; // 30, 60, 90, 120, 150 minutes
                    } else if (attempt <= 10) {
                        delayMinutes = 150 + (attempt - 5) * 60; // 210, 270, 330, 390, 450 minutes
                    } else {
                        delayMinutes = 450 + (attempt - 10) * 120; // 570, 690, 810, 930, 1050 minutes
                    }
                    
                    actualSetupStart = new Date(actualSetupStart.getTime() + delayMinutes * 60000);
                    actualSetupEnd = new Date(actualSetupEnd.getTime() + delayMinutes * 60000);
                    Logger.log(`[OPERATOR-SELECTION] ⚠️ Delaying setup by ${delayMinutes} minutes to ${actualSetupStart.toISOString()}`);
                    
                    // Re-select operator for the new time
                    actualOperator = this.selectOptimalPerson(orderData, actualSetupStart, actualSetupEnd);
                    
                    // If still no operator available, try next shift
                    if (attempt > 10) {
                        const nextShiftStart = this.getNextShiftStart(actualSetupStart);
                        actualSetupStart = nextShiftStart;
                        actualSetupEnd = new Date(actualSetupStart.getTime() + (operation.SetupTime_Min || 0) * 60000);
                        Logger.log(`[OPERATOR-SELECTION] 🔄 Moving to next shift: ${actualSetupStart.toISOString()}`);
                        actualOperator = this.selectOptimalPerson(orderData, actualSetupStart, actualSetupEnd);
                    }
                }
            }
        }
        
        if (!operatorFound) {
            // ENHANCED ERROR: Provide more detailed information about operator conflicts
            const operatorStatus = [];
            for (const [operator, intervals] of Object.entries(this.operatorSchedule)) {
                const activeSetups = intervals.filter(interval => 
                    interval.start <= actualSetupEnd && interval.end >= actualSetupStart
                );
                operatorStatus.push(`${operator}: ${activeSetups.length} active setups`);
            }
            
            throw new Error(`[OPERATOR-CONFLICT] Unable to find available operator after ${maxAttempts} attempts. All operators are overbooked. Operator status: ${operatorStatus.join(', ')}. Consider reducing batch count or increasing operator capacity.`);
        }

        // RULE 5: Select machine with NO CONFLICTS for the required time window
        const selectedMachine = this.selectOptimalMachine(
            operation, 
            orderData, 
            actualSetupStart, 
            preliminaryTiming.runEnd
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

    selectOptimalMachine(operation, orderData, setupStart, runEnd) {
        const eligibleMachines = operation.EligibleMachines || this.allMachines;
        
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

        const candidateWindow = { start: setupStart, end: runEnd };
        const dueDate = new Date(orderData.dueDate);
        
        Logger.log(`[MACHINE-SELECTION] Looking for machine for window: ${candidateWindow.start.toISOString()} → ${candidateWindow.end.toISOString()}`);
        Logger.log(`[DUE-DATE-CHECK] Order ${orderData.partNumber} due: ${dueDate.toISOString()}`);

        // DUE-DATE RESCUE LOGIC: Try to find machine that meets due date
        const candidates = [];
        
        for (const machine of availableMachines) {
            const intervals = this.machineSchedule[machine] || [];
            Logger.log(`[MACHINE-CHECK] ${machine} has ${intervals.length} existing bookings`);
            
            if (!this.hasConflict(machine, candidateWindow)) {
                // Machine available for original window
                candidates.push({
                    machine,
                    runEnd: runEnd,
                    setupStart: setupStart,
                    reason: 'no_conflict',
                    meetsDueDate: runEnd <= dueDate
                });
                Logger.log(`[CANDIDATE-FOUND] ${machine} available for original window, meets due date: ${runEnd <= dueDate}`);
            } else {
                // Check if machine becomes available later but still meets due date
                const earliestFree = this.getEarliestFreeTime(machine);
                const adjustedSetupStart = new Date(Math.max(setupStart.getTime(), earliestFree.getTime()));
                const adjustedRunEnd = new Date(adjustedSetupStart.getTime() + (runEnd.getTime() - setupStart.getTime()));
                
                if (adjustedRunEnd <= dueDate) {
                    candidates.push({
                        machine,
                        runEnd: adjustedRunEnd,
                        setupStart: adjustedSetupStart,
                        reason: 'delayed_but_on_time',
                        meetsDueDate: true
                    });
                    Logger.log(`[CANDIDATE-FOUND] ${machine} available later but still meets due date: ${adjustedRunEnd.toISOString()}`);
                } else {
                    Logger.log(`[CANDIDATE-REJECTED] ${machine} would miss due date: ${adjustedRunEnd.toISOString()} > ${dueDate.toISOString()}`);
                }
            }
        }

        // Select best candidate: prefer on-time, then earliest completion
        if (candidates.length > 0) {
            const onTimeCandidates = candidates.filter(c => c.meetsDueDate);
            if (onTimeCandidates.length > 0) {
                // Choose earliest completion among on-time candidates
                const best = onTimeCandidates.reduce((best, current) => 
                    current.runEnd < best.runEnd ? current : best
                );
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (earliest on-time completion: ${best.runEnd.toISOString()})`);
                return best.machine;
            } else {
                // No on-time options, choose earliest possible
                const best = candidates.reduce((best, current) => 
                    current.runEnd < best.runEnd ? current : best
                );
                Logger.log(`[MACHINE-SELECTED] ${best.machine} (earliest possible: ${best.runEnd.toISOString()}, will be late)`);
                return best.machine;
            }
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
        
        return {
            operator: operator,
            actualSetupStart: setupStart,
            actualSetupEnd: setupEnd,
            spillover: false
        };
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
        
        // STEP 1: Get operators who are on shift during the setup interval
        const operatorsOnShift = this.getOperatorsOnShift(setupStart, setupEnd);
        
        if (operatorsOnShift.length === 0) {
            // STEP 1.1: Check if setup can be handled with spillover
            Logger.log(`[OPERATOR-SPILLOVER-CHECK] No operators available for full setup, checking spillover options`);
            
            // Find operators who can start the setup (even if it spills over)
            const operatorsWhoCanStart = [];
            for (const [operator, shift] of Object.entries(this.operatorShifts)) {
                const shiftStart = new Date(setupStart);
                shiftStart.setHours(shift.start, 0, 0, 0);
                const shiftEnd = new Date(setupStart);
                shiftEnd.setHours(shift.end, 0, 0, 0);
                
                // Check if setup starts within this operator's shift
                if (setupStart >= shiftStart && setupStart < shiftEnd) {
                    operatorsWhoCanStart.push(operator);
                    Logger.log(`[OPERATOR-SPILLOVER-CHECK] ${operator} can start setup at ${setupStart.toISOString()} (shift: ${shift.start}:00-${shift.end}:00)`);
                }
            }
            
            if (operatorsWhoCanStart.length > 0) {
                // Return the first available operator - spillover will be handled later
                const selectedOperator = operatorsWhoCanStart[0];
                Logger.log(`[OPERATOR-SPILLOVER-CHECK] Selected ${selectedOperator} for spillover handling`);
                return selectedOperator;
            }
            
            Logger.log(`[OPERATOR-ERROR] No operators on shift for ${setupStart.toISOString()}-${setupEnd.toISOString()}`);
            throw new Error(`No operators available during setup window ${setupStart.toISOString()}-${setupEnd.toISOString()}`);
        }
        
        // STEP 2: Find operators with no conflicts (with buffer)
        const availableOperators = [];
        const BUFFER_MINUTES = 1; // 1 minute buffer between setups
        
        for (const operator of operatorsOnShift) {
            // Add buffer to setup times to prevent microsecond conflicts
            const bufferedSetupStart = new Date(setupStart.getTime() - BUFFER_MINUTES * 60000);
            const bufferedSetupEnd = new Date(setupEnd.getTime() + BUFFER_MINUTES * 60000);
            
            if (!this.hasOperatorConflict(operator, bufferedSetupStart, bufferedSetupEnd)) {
                const freeTime = this.getEarliestOperatorFreeTime(operator, setupStart);
                availableOperators.push({
                    operator,
                    freeTime,
                    reason: 'no_conflict'
                });
                Logger.log(`[OPERATOR-AVAILABLE] ${operator} available for setup, free at: ${freeTime.toISOString()}`);
            } else {
                Logger.log(`[OPERATOR-CONFLICT] ${operator} has conflicting setup`);
            }
        }
        
        // STEP 3: Select best operator using detailed tie-breaker rules
        if (availableOperators.length > 0) {
            // Tie-breaker 1: Earliest free time
            const earliestFreeOperators = availableOperators.filter(op => 
                op.freeTime.getTime() === Math.min(...availableOperators.map(o => o.freeTime.getTime()))
            );
            
            if (earliestFreeOperators.length === 1) {
                const selected = earliestFreeOperators[0];
                Logger.log(`[SETUP-ASSIGN] Operator ${selected.operator} chosen — earliest-free (${selected.freeTime.toISOString()})`);
                return selected.operator;
            }
            
            // Tie-breaker 2: Least-loaded (smallest sum of setup minutes in current shift)
            const leastLoadedOperator = earliestFreeOperators.reduce((best, current) => {
                const bestMinutes = this.getOperatorSetupMinutesInShift(best.operator, setupStart);
                const currentMinutes = this.getOperatorSetupMinutesInShift(current.operator, setupStart);
                return currentMinutes < bestMinutes ? current : best;
            });
            
            Logger.log(`[SETUP-ASSIGN] Operator ${leastLoadedOperator.operator} chosen — least-loaded (${this.getOperatorSetupMinutesInShift(leastLoadedOperator.operator, setupStart)} min)`);
            return leastLoadedOperator.operator;
        }
        
        // STEP 4: If no operator available immediately, find earliest possible time
        const earliestOptions = [];
        
        for (const operator of operatorsOnShift) {
            const earliestFree = this.getEarliestOperatorFreeTime(operator, setupStart);
            const adjustedSetupStart = new Date(Math.max(setupStart.getTime(), earliestFree.getTime()));
            const adjustedSetupEnd = new Date(adjustedSetupStart.getTime() + (setupEnd.getTime() - setupStart.getTime()));
            
            // Check if adjusted setup still falls within operator's shift
            if (this.isOperatorOnShift(operator, adjustedSetupStart, adjustedSetupEnd)) {
                earliestOptions.push({
                    operator,
                    adjustedSetupStart,
                    adjustedSetupEnd,
                    delay: adjustedSetupStart.getTime() - setupStart.getTime()
                });
                Logger.log(`[OPERATOR-DELAYED] ${operator} available at ${adjustedSetupStart.toISOString()} (${Math.round(adjustedSetupStart.getTime() - setupStart.getTime()) / (1000 * 60)} min delay)`);
            }
        }
        
        if (earliestOptions.length > 0) {
            // Choose operator with minimum delay
            const bestDelayed = earliestOptions.reduce((best, current) => 
                current.delay < best.delay ? current : best
            );
            
            Logger.log(`[OPERATOR-SELECTED-DELAYED] ${bestDelayed.operator} (min delay: ${Math.round(bestDelayed.delay / (1000 * 60))} min)`);
            return bestDelayed.operator;
        }
        
        // STEP 5: Fallback - return first operator on shift (will need manual adjustment)
        Logger.log(`[OPERATOR-FALLBACK] Using ${operatorsOnShift[0]} (requires manual schedule adjustment)`);
        return operatorsOnShift[0];
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

        // Calculate production timing
        const cycleTime = operation.CycleTime_Min || 0;
        const runStart = new Date(setupEndTime);
        const runEnd = new Date(runStart.getTime() + (cycleTime * batchQty * 60000));

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
        
        // CRITICAL FIX: Enforce piece-flow trigger rule
        // Setup can ONLY start when first piece from previous operation is ready
        let setupStartTime = earliestStartTime;
        
        // If this is not the first operation, enforce piece-flow trigger
        if (previousOpPieceCompletionTimes && previousOpPieceCompletionTimes.length > 0) {
            const firstPieceReadyTime = previousOpPieceCompletionTimes[0];
            
            // VALIDATE firstPieceReadyTime
            if (!firstPieceReadyTime || isNaN(firstPieceReadyTime.getTime())) {
                Logger.log(`[ERROR] Invalid firstPieceReadyTime: ${firstPieceReadyTime}`);
                throw new Error(`Invalid firstPieceReadyTime: ${firstPieceReadyTime}`);
            }
            
            setupStartTime = new Date(Math.max(setupStartTime.getTime(), firstPieceReadyTime.getTime()));
            Logger.log(`[PIECE-FLOW-TRIGGER] Previous op first piece ready at: ${firstPieceReadyTime.toISOString()}`);
            Logger.log(`[PIECE-FLOW-TRIGGER] Setup start enforced to: ${setupStartTime.toISOString()}`);
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
        
        Logger.log(`[USER-ALGORITHM] Applying exact piece-level algorithm: ${batchQty} pieces × ${cycleTime}min = ${batchQty * cycleTime}min total`);

        // CALCULATE RUN DURATION: BatchQty × CycleTime (USER'S FORMULA)
        const totalRunDuration = batchQty * cycleTime; // minutes
        const totalRunDurationMs = totalRunDuration * 60000; // milliseconds

        // CALCULATE FIRST PIECE DONE TIME
        let firstPieceDoneTime;
        if (previousOpPieceCompletionTimes && previousOpPieceCompletionTimes.length > 0) {
            // PIECE-LEVEL HANDOFF: Current operation's first piece is ready when previous operation's first piece is ready
            // This triggers the setup for the current operation
            firstPieceDoneTime = new Date(previousOpPieceCompletionTimes[0]);
            Logger.log(`[PIECE-LEVEL-HANDOFF] Previous op first piece ready at: ${firstPieceDoneTime.toISOString()}`);
            Logger.log(`[PIECE-LEVEL-HANDOFF] This triggers setup for Op${operation.OperationSeq}`);
        } else {
            // First operation - first piece ready at setup end
            firstPieceDoneTime = new Date(setupEndTime);
            Logger.log(`[USER-ALGORITHM] First piece ready at setup end: ${firstPieceDoneTime.toISOString()}`);
        }

        // VALIDATE firstPieceDoneTime
        if (!firstPieceDoneTime || isNaN(firstPieceDoneTime.getTime())) {
            Logger.log(`[ERROR] Invalid firstPieceDoneTime: ${firstPieceDoneTime}`);
            throw new Error(`Invalid firstPieceDoneTime: ${firstPieceDoneTime}`);
        }

        // CALCULATE RUN START AND END TIMES
        // PIECE-LEVEL LOGIC: Run starts after setup is complete, regardless of when first piece was ready
        const runStartTime = new Date(setupEndTime);
        let runEndTime = new Date(runStartTime.getTime() + totalRunDurationMs);

        // CRITICAL FIX: Implement CORRECT piece-level handoff logic
        // In piece-level flow, operations can overlap - Op2 processes pieces while Op1 is still running
        // Op2 RunEnd = Op2 RunStart + Op2 Processing Time (independent of Op1's total completion)
        // Only constraint: Op2 cannot finish before Op1's first piece triggers Op2 to start
        if (previousOpRunEnd) {
            Logger.log(`[PIECE-LEVEL-HANDOFF] Op${operation.OperationSeq} natural RunEnd: ${runEndTime.toISOString()}`);
            Logger.log(`[PIECE-LEVEL-HANDOFF] Previous Op RunEnd: ${previousOpRunEnd.toISOString()}`);
            
            // CORRECT PIECE-LEVEL LOGIC: Each operation runs independently once triggered
            // No need to wait for previous operation to complete entirely
            const naturalRunEnd = new Date(runStartTime.getTime() + totalRunDurationMs);
            
            Logger.log(`[PIECE-LEVEL-HANDOFF] Op${operation.OperationSeq} will finish naturally at: ${naturalRunEnd.toISOString()}`);
            Logger.log(`[PIECE-LEVEL-HANDOFF] Previous operation finishes at: ${previousOpRunEnd.toISOString()}`);
            
            // Use natural completion time - piece-level flow allows overlap
            runEndTime = naturalRunEnd;
            
            Logger.log(`[PIECE-LEVEL-HANDOFF] ✅ Op${operation.OperationSeq} RunEnd set to natural completion: ${runEndTime.toISOString()}`);
        }

        // CALCULATE FIRST PIECE COMPLETION TIME
        const firstPieceCompletionTime = new Date(runStartTime.getTime() + cycleTime * 60000);

        Logger.log(`[USER-ALGORITHM] Run duration: ${totalRunDuration}min (${batchQty} × ${cycleTime}min)`);
        Logger.log(`[USER-ALGORITHM] Run start: ${runStartTime.toISOString()}`);
        Logger.log(`[USER-ALGORITHM] Run end: ${runEndTime.toISOString()}`);
        Logger.log(`[USER-ALGORITHM] First piece done: ${firstPieceCompletionTime.toISOString()}`);

        // CREATE PIECE COMPLETION TIMES ARRAY (for next operation)
        const pieceCompletionTimes = [];
        const pieceStartTimes = [];
        
        for (let piece = 0; piece < batchQty; piece++) {
            const pieceStartTime = new Date(runStartTime.getTime() + piece * cycleTime * 60000);
            const pieceEndTime = new Date(pieceStartTime.getTime() + cycleTime * 60000);
            
            pieceStartTimes.push(pieceStartTime);
            pieceCompletionTimes.push(pieceEndTime);
        }

        // RETURN RESULTS WITH USER'S ALGORITHM
        return {
            setupStartTime,
            setupEndTime,
            runStartTime,
            runEndTime,
            pieceCompletionTimes,
            pieceStartTimes,
            firstPieceDone: firstPieceCompletionTime,
            totalWorkTime: totalRunDuration,
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
        
        // More flexible setup window enforcement
        if (hour < setupWindow.start) {
            // Before window - move to start of window (same day)
            const newTime = new Date(time);
            newTime.setHours(setupWindow.start, 0, 0, 0);
            Logger.log(`Setup moved to window start: ${newTime.toISOString()}`);
            return newTime;
        } else if (hour >= setupWindow.end) {
            // After window - allow immediate start if setup is short, otherwise move to next day
            const setupDuration = 60; // Assume 1 hour setup for decision
            const setupEndHour = hour + Math.ceil(setupDuration / 60);
            
            if (setupEndHour <= 24) {
                // Setup can complete today, allow immediate start
                Logger.log(`Setup allowed outside window (can complete today): ${time.toISOString()}`);
                return new Date(time);
            } else {
                // Setup would go past midnight, move to next day
                const newTime = new Date(time);
                newTime.setDate(newTime.getDate() + 1);
                newTime.setHours(setupWindow.start, 0, 0, 0);
                Logger.log(`Setup moved to next day window start: ${newTime.toISOString()}`);
                return newTime;
            }
        }
        
        // Within window, return as-is
        Logger.log(`Setup within window: ${time.toISOString()}`);
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
        
        const [start, end] = windowString.split('-');
        if (!start || !end) return { start: 6, end: 22 };
        
        const startHour = parseInt(start.split(':')[0]);
        const endHour = parseInt(end.split(':')[0]);
        
        return {
            start: isNaN(startHour) ? 6 : startHour,
            end: isNaN(endHour) ? 22 : endHour
        };
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
        
        // Format total duration
        let totalDuration = '';
        if (days > 0) totalDuration += `${days}D `;
        if (hours > 0) totalDuration += `${hours}H `;
        if (minutes > 0) totalDuration += `${minutes}M`;
        totalDuration = totalDuration.trim();
        
        // Format work/holiday breakdown - only show non-zero values
        const workHours = Math.floor(workMinutes / 60);
        const workMins = workMinutes % 60;
        const holidayHours = Math.floor(holidayMinutes / 60);
        const holidayMins = holidayMinutes % 60;
        
        let workStr = '';
        if (workHours > 0) {
            const workDays = Math.floor(workHours / 24);
            const workRemainingHours = workHours % 24;
            if (workDays > 0) workStr += `${workDays}D `;
            if (workRemainingHours > 0) workStr += `${workRemainingHours}H `;
            if (workMins > 0) workStr += `${workMins}M`;
        } else if (workMins > 0) {
            workStr += `${workMins}M`;
        }
        workStr = workStr.trim();
        
        let holidayStr = '';
        if (holidayHours > 0) {
            const holidayDays = Math.floor(holidayHours / 24);
            const holidayRemainingHours = holidayHours % 24;
            if (holidayDays > 0) holidayStr += `${holidayDays}D `;
            if (holidayRemainingHours > 0) holidayStr += `${holidayRemainingHours}H `;
            if (holidayMins > 0) holidayStr += `${holidayMins}M`;
        } else if (holidayMins > 0) {
            holidayStr += `${holidayMins}M`;
        }
        holidayStr = holidayStr.trim();
        
        // Build result - only include non-zero values
        let result = totalDuration;
        
        if (workStr && holidayStr) {
            result += ` (${workStr} Work, ${holidayStr} Holiday)`;
        } else if (workStr) {
            result += ` (${workStr} Work)`;
        } else if (holidayStr) {
            result += ` (${holidayStr} Holiday)`;
        }
        
        return result;
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
        
        // Check if entire setup interval falls within operator's shift
        const isOnShift = startHour >= shift.start && endHour < shift.end;
        
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
        
        // Check for conflicts before adding
        const candidateInterval = { start: setupStart, end: setupEnd };
        for (const existing of this.operatorSchedule[operator]) {
            if (this.hasConflict(operator, candidateInterval)) {
                Logger.log(`[OPERATOR-CONFLICT] ${operator} has conflicting setup: ${existing.start.toISOString()}-${existing.end.toISOString()}`);
                
                // Try one more time to resolve the conflict automatically
                Logger.log(`[OPERATOR-CONFLICT] Attempting automatic conflict resolution...`);
                const resolution = this.resolveOperatorConflict(operator, setupStart, setupEnd);
                
                if (resolution) {
                    Logger.log(`[OPERATOR-CONFLICT] ✅ Auto-resolved: Using ${resolution.operator} at ${resolution.setupStart.toISOString()}`);
                    // Reserve the resolved operator instead
                    this.reserveOperator(resolution.operator, resolution.setupStart, resolution.setupEnd);
                    return;
                } else {
                    throw new Error(`[SETUP-OVERBOOKING] Operator ${operator} has overlapping setup intervals: ${existing.start.toISOString()}-${existing.end.toISOString()} overlaps with ${setupStart.toISOString()}-${setupEnd.toISOString()}. Automatic resolution failed.`);
                }
            }
        }
        
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
    
    // OPERATOR CONFLICT RESOLUTION
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

        const globalSettings = {
            startDateTime: null, // Will use current time
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
} else {
    window.runScheduling = runScheduling;
    window.processOrderSingle = window.processOrderSingle;
    window.testPieceLevelScheduling = testPieceLevelScheduling;
    window.testUserAlgorithm = testUserAlgorithm;
    window.diagnoseScheduleIssues = diagnoseScheduleIssues;
    window.diagnosePN11001Schedule = diagnosePN11001Schedule;
    window.FixedUnifiedSchedulingEngine = FixedUnifiedSchedulingEngine;
    window.SCHEDULING_CONFIG = CONFIG;
    
    // Expose calculateBatchSplitting as a global function
    window.calculateBatchSplitting = function(totalQuantity, minBatchSize, priority = 'normal', dueDate = null, startDate = null) {
        try {
            const engine = new FixedUnifiedSchedulingEngine();
            const result = engine.calculateBatchSplitting(totalQuantity, minBatchSize, priority, dueDate, startDate);
            console.log('Batch splitting result:', result);
            return result;
        } catch (error) {
            console.error('Batch splitting error:', error);
            // Improved fallback implementation with priority support
            const DEFAULT_BATCH_SIZE = 250; // Optimal batch size
            const MIN_BATCH_SIZE = Math.max(minBatchSize, 100); // Minimum 100 pieces per batch
            
            // Priority-based batch limits
            let maxBatches;
            let batchSizeMultiplier = 1.0;
            
            if (priority.toLowerCase() === 'urgent' || priority.toLowerCase() === 'high') {
                if (totalQuantity <= 500) maxBatches = 4;
                else if (totalQuantity <= 1000) maxBatches = 5;
                else maxBatches = 6;
                batchSizeMultiplier = 0.8;
            } else if (priority.toLowerCase() === 'critical' || priority.toLowerCase() === 'emergency') {
                if (totalQuantity <= 500) maxBatches = 5;
                else if (totalQuantity <= 1000) maxBatches = 6;
                else maxBatches = 8;
                batchSizeMultiplier = 0.6;
            } else {
                if (totalQuantity <= 500) maxBatches = 3;
                else if (totalQuantity <= 1000) maxBatches = 4;
                else maxBatches = 5;
                batchSizeMultiplier = 1.0;
            }
            
            // Calculate optimal batch size
            let optimalBatchSize;
            if (totalQuantity <= 300) {
                optimalBatchSize = Math.max(MIN_BATCH_SIZE, Math.ceil(totalQuantity / 2));
            } else if (totalQuantity <= 600) {
                optimalBatchSize = DEFAULT_BATCH_SIZE;
            } else if (totalQuantity <= 1000) {
                optimalBatchSize = Math.min(DEFAULT_BATCH_SIZE + 50, Math.ceil(totalQuantity / 3));
            } else {
                optimalBatchSize = Math.min(300, Math.ceil(totalQuantity / 4));
            }
            
            optimalBatchSize = Math.ceil(optimalBatchSize * batchSizeMultiplier);
            let numBatches = Math.ceil(totalQuantity / optimalBatchSize);
            
            if (numBatches > maxBatches) {
                numBatches = maxBatches;
                optimalBatchSize = Math.ceil(totalQuantity / numBatches);
            }
            
            if (optimalBatchSize < MIN_BATCH_SIZE) {
                optimalBatchSize = MIN_BATCH_SIZE;
                numBatches = Math.ceil(totalQuantity / optimalBatchSize);
            }
            
            const batchSize = Math.ceil(totalQuantity / numBatches);
            const batches = [];
            let remainingQuantity = totalQuantity;
            
            for (let i = 0; i < numBatches; i++) {
                const batchId = `B${String(i + 1).padStart(2, '0')}`;
                const batchQuantity = Math.min(batchSize, remainingQuantity);
                
                batches.push({
                    batchId: batchId,
                    quantity: batchQuantity,
                    batchIndex: i
                });
                
                remainingQuantity -= batchQuantity;
            }
            
            console.log('Fallback batch splitting result:', batches);
            return batches;
        }
    };
}
