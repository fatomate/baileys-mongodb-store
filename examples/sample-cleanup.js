/**
 * Global Unified Cleanup Module
 * 
 * Consolidates all cleanup operations into a single Bull queue that runs at 3 AM daily.
 * Runs only on the designated master instance (IS_CLEANUP_MASTER=true).
 * 
 * Includes:
 * - Media cleanup for all WhatsApp instances
 * - Message queue log cleanup for all apps
 * - Follow-up evaluation log cleanup
 * - Orphaned subscriber cleanup
 * 
 * This is a global cleanup that handles all apps' data from a single instance.
 */

const Queue = require('bull');
const Redis = require('ioredis');
const config = require('../config.js');
const { promisePool } = require('./db.js');
const fs = require('fs');
const path = require('path');
const { createGzip } = require('zlib');
const { pipeline } = require('stream');
const { promisify } = require('util');
const pipelineAsync = promisify(pipeline);

class UnifiedCleanupManager {
    constructor(appVersion, waziper) {
        this.appVersion = appVersion;
        this.waziper = waziper;
        this.queueName = 'cleanup-global';  // Single global queue
        this.cleanupQueue = null;
        this.redis = new Redis(config.redis);
        
        // Configuration for retention periods (in days)
        this.retentionDays = {
            media: 7,           // Keep media for 7 days
            messageQueue: 7,    // Keep message queue logs for 7 days
            followUpLogs: 7,    // Keep follow-up evaluation logs for 7 days
            orphanedSubscribers: 0,  // Delete orphaned subscribers immediately
            reports: 30,        // Keep reports for 30 days
            backups: 7          // Keep backups for 7 days
        };
        
        // Report directory path
        this.reportBasePath = path.join(__dirname, '../../cleanup-report');
    }

    /**
     * Initialize the cleanup queue with cron scheduling
     */
    async initialize() {
        console.log(`Initializing global cleanup manager (running on app ${this.appVersion})`);
        
        try {
            // Create cleanup queue with robust settings
            this.cleanupQueue = new Queue(this.queueName, config.redis_keepalive, {
                defaultJobOptions: {
                    removeOnComplete: true,
                    removeOnFail: true,  // Keep failed jobs for debugging
                    attempts: 3,
                    backoff: {
                        type: 'exponential',
                        delay: 5000
                    }
                },
                settings: {
                    stalledInterval: 300000,     // Check every 5 minutes
                    maxStalledCount: 1,          // Fail after 1 stall (cleanup shouldn't take that long)
                    lockDuration: 600000,        // 10 minute lock for cleanup operations
                    lockRenewTime: 300000        // Renew at 5 minutes
                },
                prefix: "cleanup"
            });

            // Clean existing repeatable jobs before initializing
            await this.cleanExistingJobs();

            // Process cleanup jobs
            this.cleanupQueue.process('unified_cleanup', async (job) => {
                console.log(`Starting global unified cleanup at ${new Date().toISOString()} (running on app ${this.appVersion})`);
                
                try {
                    const results = await this.performAllCleanups();
                    console.log(`Global cleanup completed:`, JSON.stringify(results, null, 2));
                    return results;
                } catch (error) {
                    console.error(`Error during global unified cleanup:`, error);
                    throw error;
                }
            });

            // Schedule daily cleanup at 3 AM
            const job = await this.cleanupQueue.add(
                'unified_cleanup',
                { 
                    timestamp: Date.now(), 
                    appVersion: this.appVersion 
                },
                {
                    repeat: {
                        cron: '0 1 * * *',  // 1:00 AM every day
                        tz: 'Asia/Kuala_Lumpur'  // Malaysian timezone (adjust as needed)
                    },
                    removeOnComplete: true,
                    removeOnFail: true
                }
            );

            console.log(`Global cleanup scheduled at 3 AM daily (running on app ${this.appVersion}, job ID: ${job.id})`);

            // Add error handling
            this.cleanupQueue
                .on('error', (error) => {
                    console.error(`Cleanup queue ${this.queueName} error:`, error);
                })
                .on('failed', (job, error) => {
                    console.error(`Cleanup job ${job.id} in ${this.queueName} failed:`, error);
                })
                .on('stalled', (jobId) => {
                    console.warn(`Cleanup job ${jobId} in ${this.queueName} has stalled`);
                });

            // Add health check
            this.startHealthCheck();

            return true;
        } catch (error) {
            console.error(`Error initializing global cleanup manager:`, error);
            return false;
        }
    }

    /**
     * Clean existing repeatable jobs
     */
    async cleanExistingJobs() {
        try {
            const existingJobs = await this.cleanupQueue.getRepeatableJobs();
            await Promise.all(
                existingJobs.map(job => this.cleanupQueue.removeRepeatableByKey(job.key))
            );
            console.log(`Cleaned ${existingJobs.length} existing repeatable cleanup jobs for ${this.queueName}`);
        } catch (error) {
            console.error(`Error cleaning existing jobs for ${this.queueName}:`, error);
        }
    }

    /**
     * Ensure report directory structure exists
     */
    async ensureReportDirectory(date) {
        const dateDir = path.join(this.reportBasePath, date);
        const backupsDir = path.join(dateDir, 'backups');
        const reportsDir = path.join(dateDir, 'reports');
        
        // Create directories if they don't exist
        await fs.promises.mkdir(backupsDir, { recursive: true });
        await fs.promises.mkdir(reportsDir, { recursive: true });
        
        return { dateDir, backupsDir, reportsDir };
    }

    /**
     * Stream orphaned subscribers to CSV with compression
     */
    async streamOrphanedSubscribersToCSV(outputPath) {
        const startTime = Date.now();
        let recordCount = 0;
        const batchSize = 1000;
        
        return new Promise(async (resolve, reject) => {
            let connection;
            try {
                // Create write stream with gzip compression
                const writeStream = fs.createWriteStream(outputPath);
                const gzipStream = createGzip({ level: 9 });
                
                // Setup pipeline
                gzipStream.pipe(writeStream);
                
                // Write CSV header
                gzipStream.write('id,instance_id,phone,name,created_at,updated_at,tags,custom_fields,team_id,status\n');
                
                // Get connection
                connection = await promisePool.getConnection();
                
                // Process in batches for efficient memory usage
                let offset = 0;
                let hasMore = true;
                
                while (hasMore) {
                    const query = `
                        SELECT s.* 
                        FROM sp_whatsapp_subscriber s 
                        LEFT JOIN sp_accounts a ON s.instance_id = a.token 
                        WHERE a.token IS NULL
                        ORDER BY s.id
                        LIMIT ${batchSize} OFFSET ${offset}
                    `;
                    
                    const [rows] = await connection.query(query);
                    
                    if (rows.length === 0) {
                        hasMore = false;
                    } else {
                        for (const row of rows) {
                            recordCount++;
                            // Format row as CSV - escape special characters properly
                            const csvRow = [
                                row.id,
                                row.instance_id || '',
                                row.phone || '',
                                (row.name || '').replace(/"/g, '""').replace(/\n/g, ' '),
                                row.created_at ? new Date(row.created_at).toISOString() : '',
                                row.updated_at ? new Date(row.updated_at).toISOString() : '',
                                (row.tags || '').replace(/"/g, '""'),
                                (row.custom_fields || '').replace(/"/g, '""'),
                                row.team_id || '',
                                row.status || ''
                            ].map(field => {
                                // Wrap in quotes if contains comma, newline, or quotes
                                const fieldStr = String(field);
                                if (fieldStr.includes(',') || fieldStr.includes('\n') || fieldStr.includes('"')) {
                                    return `"${fieldStr}"`;
                                }
                                return fieldStr;
                            }).join(',') + '\n';
                            
                            gzipStream.write(csvRow);
                        }
                        
                        offset += batchSize;
                        hasMore = rows.length === batchSize;
                    }
                }
                
                // Close the gzip stream
                gzipStream.end();
                
                // Release connection
                connection.release();
                
                // Wait for write to finish
                writeStream.on('finish', () => {
                    const stats = fs.statSync(outputPath);
                    resolve({
                        success: true,
                        recordCount,
                        filePath: outputPath,
                        fileSize: stats.size,
                        duration: Date.now() - startTime
                    });
                });
                
                writeStream.on('error', reject);
                gzipStream.on('error', reject);
                
            } catch (error) {
                if (connection) {
                    connection.release();
                }
                reject(error);
            }
        });
    }

    /**
     * Backup orphaned subscribers before deletion
     */
    async backupOrphanedSubscribers(date) {
        try {
            const dirs = await this.ensureReportDirectory(date);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const backupPath = path.join(dirs.backupsDir, `orphaned-subscribers-${timestamp}.csv.gz`);
            
            console.log(`Creating backup of orphaned subscribers at ${backupPath}`);
            
            const result = await this.streamOrphanedSubscribersToCSV(backupPath);
            
            console.log(`Backup completed: ${result.recordCount} records, ${(result.fileSize / 1024).toFixed(2)} KB, took ${result.duration}ms`);
            
            return result;
        } catch (error) {
            console.error(`Error creating orphaned subscribers backup:`, error);
            throw error;
        }
    }

    /**
     * Generate JSON summary report
     */
    async generateJSONReport(results, outputPath) {
        try {
            const report = {
                timestamp: new Date().toISOString(),
                appVersion: this.appVersion,
                duration: results.duration,
                durationFormatted: this.formatDuration(results.duration),
                status: results.cleanups ? 'success' : 'failed',
                summary: {
                    media: {
                        instances: results.cleanups?.media?.instances || 0,
                        filesDeleted: results.cleanups?.media?.totalDeleted || 0,
                        errors: results.cleanups?.media?.errors?.length || 0
                    },
                    messageQueue: {
                        recordsDeleted: results.cleanups?.messageQueue?.deleted || 0,
                        batches: results.cleanups?.messageQueue?.batches || 0,
                        errors: results.cleanups?.messageQueue?.errors?.length || 0
                    },
                    mongodb: {
                        messagesDeleted: results.cleanups?.mongodbMessages?.totalDeleted || 0,
                        instances: results.cleanups?.mongodbMessages?.instances || 0,
                        errors: results.cleanups?.mongodbMessages?.errors?.length || 0
                    },
                    orphanedSubscribers: {
                        found: results.cleanups?.orphanedSubscribers?.orphanedCount || 0,
                        deleted: results.cleanups?.orphanedSubscribers?.deleted || 0,
                        backupFile: results.orphanedBackup?.filePath || null,
                        backupRecords: results.orphanedBackup?.recordCount || 0
                    },
                    followUpLogs: {
                        deleted: results.cleanups?.followUpLogs?.deleted || 0
                    }
                },
                details: results.cleanups,
                errors: this.collectAllErrors(results)
            };
            
            await fs.promises.writeFile(outputPath, JSON.stringify(report, null, 2));
            return report;
        } catch (error) {
            console.error(`Error generating JSON report:`, error);
            throw error;
        }
    }

    /**
     * Generate HTML dashboard
     */
    async generateHTMLDashboard(results, outputPath) {
        try {
            const report = await this.generateJSONReport(results, outputPath.replace('.html', '.json'));
            
            const html = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cleanup Report - ${new Date().toLocaleDateString()}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }
        h1 { font-size: 2.5rem; margin-bottom: 10px; }
        .status { display: inline-block; padding: 5px 15px; border-radius: 20px; font-weight: bold; }
        .status.success { background: #10b981; }
        .status.failed { background: #ef4444; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .card h3 { color: #667eea; margin-bottom: 15px; font-size: 1.2rem; }
        .metric { display: flex; justify-content: space-between; margin-bottom: 10px; padding: 10px; background: #f9fafb; border-radius: 5px; }
        .metric-label { color: #6b7280; }
        .metric-value { font-weight: bold; color: #111827; }
        .error-section { background: #fef2f2; border: 1px solid #fecaca; border-radius: 10px; padding: 20px; margin-bottom: 30px; }
        .error-title { color: #dc2626; margin-bottom: 15px; }
        .error-list { list-style: none; }
        .error-item { background: white; padding: 10px; margin-bottom: 10px; border-radius: 5px; border-left: 4px solid #dc2626; }
        .table-container { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; }
        th { background: #f3f4f6; padding: 12px; text-align: left; font-weight: 600; color: #374151; }
        td { padding: 12px; border-bottom: 1px solid #e5e7eb; }
        tr:hover { background: #f9fafb; }
        .footer { text-align: center; margin-top: 30px; color: #6b7280; }
        .backup-link { color: #667eea; text-decoration: none; font-weight: bold; }
        .backup-link:hover { text-decoration: underline; }
        .chart-container { height: 300px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Cleanup Report</h1>
            <p>Generated on ${new Date().toLocaleString()}</p>
            <p>Duration: ${report.durationFormatted}</p>
            <span class="status ${report.status}">${report.status.toUpperCase()}</span>
        </div>
        
        <div class="grid">
            <div class="card">
                <h3>📁 Media Cleanup</h3>
                <div class="metric">
                    <span class="metric-label">Instances Processed</span>
                    <span class="metric-value">${report.summary.media.instances}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Files Deleted</span>
                    <span class="metric-value">${report.summary.media.filesDeleted.toLocaleString()}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Errors</span>
                    <span class="metric-value">${report.summary.media.errors}</span>
                </div>
            </div>
            
            <div class="card">
                <h3>📨 Message Queue</h3>
                <div class="metric">
                    <span class="metric-label">Records Deleted</span>
                    <span class="metric-value">${report.summary.messageQueue.recordsDeleted.toLocaleString()}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Batches Processed</span>
                    <span class="metric-value">${report.summary.messageQueue.batches}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Errors</span>
                    <span class="metric-value">${report.summary.messageQueue.errors}</span>
                </div>
            </div>
            
            <div class="card">
                <h3>🗄️ MongoDB Messages</h3>
                <div class="metric">
                    <span class="metric-label">Messages Deleted</span>
                    <span class="metric-value">${report.summary.mongodb.messagesDeleted.toLocaleString()}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Instances Processed</span>
                    <span class="metric-value">${report.summary.mongodb.instances}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Errors</span>
                    <span class="metric-value">${report.summary.mongodb.errors}</span>
                </div>
            </div>
            
            <div class="card">
                <h3>👥 Orphaned Subscribers</h3>
                <div class="metric">
                    <span class="metric-label">Found</span>
                    <span class="metric-value">${report.summary.orphanedSubscribers.found.toLocaleString()}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Deleted</span>
                    <span class="metric-value">${report.summary.orphanedSubscribers.deleted.toLocaleString()}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Backed Up</span>
                    <span class="metric-value">${report.summary.orphanedSubscribers.backupRecords.toLocaleString()}</span>
                </div>
                ${report.summary.orphanedSubscribers.backupFile ? `
                <div class="metric">
                    <span class="metric-label">Backup File</span>
                    <span class="metric-value"><a href="#" class="backup-link">${path.basename(report.summary.orphanedSubscribers.backupFile)}</a></span>
                </div>
                ` : ''}
            </div>
            
            <div class="card">
                <h3>📋 Follow-up Logs</h3>
                <div class="metric">
                    <span class="metric-label">Records Deleted</span>
                    <span class="metric-value">${report.summary.followUpLogs.deleted.toLocaleString()}</span>
                </div>
            </div>
        </div>
        
        ${report.errors && report.errors.length > 0 ? `
        <div class="error-section">
            <h2 class="error-title">⚠️ Errors Encountered</h2>
            <ul class="error-list">
                ${report.errors.map(error => `
                    <li class="error-item">${error}</li>
                `).join('')}
            </ul>
        </div>
        ` : ''}
        
        <div class="footer">
            <p>Generated by Unified Cleanup Manager v${this.appVersion}</p>
        </div>
    </div>
</body>
</html>`;
            
            await fs.promises.writeFile(outputPath, html);
            return true;
        } catch (error) {
            console.error(`Error generating HTML dashboard:`, error);
            throw error;
        }
    }

    /**
     * Append results to CSV log for trend analysis
     */
    async appendToCSVLog(results, logPath) {
        try {
            const fileExists = fs.existsSync(logPath);
            
            // Create header if file doesn't exist
            if (!fileExists) {
                const header = 'date,time,duration_ms,media_deleted,queue_deleted,mongo_deleted,orphaned_found,orphaned_deleted,followup_deleted,total_errors\n';
                await fs.promises.writeFile(logPath, header);
            }
            
            // Prepare CSV row
            const row = [
                new Date().toISOString().split('T')[0],
                new Date().toTimeString().split(' ')[0],
                results.duration || 0,
                results.cleanups?.media?.totalDeleted || 0,
                results.cleanups?.messageQueue?.deleted || 0,
                results.cleanups?.mongodbMessages?.totalDeleted || 0,
                results.cleanups?.orphanedSubscribers?.orphanedCount || 0,
                results.cleanups?.orphanedSubscribers?.deleted || 0,
                results.cleanups?.followUpLogs?.deleted || 0,
                this.collectAllErrors(results).length
            ].join(',') + '\n';
            
            await fs.promises.appendFile(logPath, row);
            return true;
        } catch (error) {
            console.error(`Error appending to CSV log:`, error);
            throw error;
        }
    }

    /**
     * Generate all cleanup reports
     */
    async generateCleanupReport(results, date) {
        try {
            const dirs = await this.ensureReportDirectory(date);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            
            // Generate JSON report
            const jsonPath = path.join(dirs.reportsDir, `cleanup-summary-${timestamp}.json`);
            await this.generateJSONReport(results, jsonPath);
            console.log(`JSON report saved to ${jsonPath}`);
            
            // Generate HTML dashboard
            const htmlPath = path.join(dirs.reportsDir, `cleanup-dashboard-${timestamp}.html`);
            await this.generateHTMLDashboard(results, htmlPath);
            console.log(`HTML dashboard saved to ${htmlPath}`);
            
            // Append to CSV log
            const csvLogPath = path.join(this.reportBasePath, 'cleanup-log.csv');
            await this.appendToCSVLog(results, csvLogPath);
            console.log(`Results appended to CSV log at ${csvLogPath}`);
            
            // Create latest symlinks for easy access
            const latestJsonLink = path.join(this.reportBasePath, 'latest-report.json');
            const latestHtmlLink = path.join(this.reportBasePath, 'latest-dashboard.html');
            
            // Remove old symlinks if they exist
            try {
                await fs.promises.unlink(latestJsonLink);
            } catch (e) { /* ignore */ }
            try {
                await fs.promises.unlink(latestHtmlLink);
            } catch (e) { /* ignore */ }
            
            // Create new symlinks
            await fs.promises.symlink(jsonPath, latestJsonLink);
            await fs.promises.symlink(htmlPath, latestHtmlLink);
            
            return {
                success: true,
                jsonPath,
                htmlPath,
                csvLogPath
            };
        } catch (error) {
            console.error(`Error generating cleanup reports:`, error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Clean up old reports and backups
     */
    async cleanupOldReports(retentionDays = 30) {
        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
            
            const entries = await fs.promises.readdir(this.reportBasePath, { withFileTypes: true });
            let deletedCount = 0;
            
            for (const entry of entries) {
                if (entry.isDirectory()) {
                    // Check if directory name is a date
                    const dateMatch = entry.name.match(/^\d{4}-\d{2}-\d{2}$/);
                    if (dateMatch) {
                        const dirDate = new Date(entry.name);
                        if (dirDate < cutoffDate) {
                            const dirPath = path.join(this.reportBasePath, entry.name);
                            await fs.promises.rm(dirPath, { recursive: true, force: true });
                            deletedCount++;
                            console.log(`Deleted old report directory: ${entry.name}`);
                        }
                    }
                }
            }
            
            if (deletedCount > 0) {
                console.log(`Cleaned up ${deletedCount} old report directories`);
            }
            
            return { success: true, deletedCount };
        } catch (error) {
            console.error(`Error cleaning up old reports:`, error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Collect all errors from results
     */
    collectAllErrors(results) {
        const errors = [];
        
        if (results.cleanups?.media?.errors) {
            results.cleanups.media.errors.forEach(e => {
                errors.push(`Media cleanup - Instance ${e.instance_id}: ${e.error}`);
            });
        }
        
        if (results.cleanups?.messageQueue?.errors) {
            results.cleanups.messageQueue.errors.forEach(e => {
                errors.push(`Message queue cleanup: ${e}`);
            });
        }
        
        if (results.cleanups?.mongodbMessages?.errors) {
            results.cleanups.mongodbMessages.errors.forEach(e => {
                errors.push(`MongoDB cleanup - Instance ${e.instance_id}: ${e.error}`);
            });
        }
        
        if (results.cleanups?.orphanedSubscribers?.errors) {
            results.cleanups.orphanedSubscribers.errors.forEach(e => {
                errors.push(`Orphaned subscribers cleanup: ${e}`);
            });
        }
        
        return errors;
    }

    /**
     * Format duration in human-readable format
     */
    formatDuration(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        
        if (hours > 0) {
            return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
        } else if (minutes > 0) {
            return `${minutes}m ${seconds % 60}s`;
        } else {
            return `${seconds}s`;
        }
    }

    /**
     * Perform all cleanup operations
     */
    async performAllCleanups() {
        const startTime = Date.now();
        const date = new Date().toISOString().split('T')[0];
        const results = {
            appVersion: this.appVersion,
            type: 'global',
            startTime: new Date(startTime).toISOString(),
            cleanups: {}
        };

        try {
            // Ensure report directory exists
            await this.ensureReportDirectory(date);
            
            // Create backup of orphaned subscribers before any cleanup
            console.log(`[GLOBAL] Creating backup of orphaned subscribers...`);
            try {
                results.orphanedBackup = await this.backupOrphanedSubscribers(date);
            } catch (error) {
                console.error(`Error creating orphaned subscribers backup:`, error);
                // Continue with cleanup even if backup fails, but log the error
                results.orphanedBackup = { success: false, error: error.message };
            }

            // Run cleanups sequentially to avoid overwhelming the system
            console.log(`[GLOBAL] Starting media cleanup for all instances...`);
            results.cleanups.media = await this.cleanupMedia(this.retentionDays.media);
            
            console.log(`[GLOBAL] Starting message queue cleanup for all apps...`);
            results.cleanups.messageQueue = await this.cleanupMessageQueue(this.retentionDays.messageQueue);
            
            console.log(`[GLOBAL] Starting follow-up logs cleanup...`);
            results.cleanups.followUpLogs = await this.cleanupFollowUpLogs(this.retentionDays.followUpLogs);
            
            console.log(`[GLOBAL] Starting orphaned subscriber cleanup...`);
            // Only proceed with deletion if backup was successful
            if (results.orphanedBackup && results.orphanedBackup.success) {
                results.cleanups.orphanedSubscribers = await this.cleanupOrphanedSubscribers();
            } else {
                console.warn(`Skipping orphaned subscriber cleanup due to backup failure`);
                results.cleanups.orphanedSubscribers = {
                    success: false,
                    message: 'Skipped due to backup failure',
                    deleted: 0,
                    orphanedCount: 0
                };
            }
            
            console.log(`[GLOBAL] Starting MongoDB message cleanup based on instance retention settings...`);
            results.cleanups.mongodbMessages = await this.cleanupMongoDBMessages();

            results.endTime = new Date().toISOString();
            results.duration = Date.now() - startTime;

            // Generate comprehensive reports
            console.log(`[GLOBAL] Generating cleanup reports...`);
            const reportResult = await this.generateCleanupReport(results, date);
            results.reportGeneration = reportResult;
            
            // Clean old reports based on retention policy
            console.log(`[GLOBAL] Cleaning old reports (retention: ${this.retentionDays.reports} days)...`);
            const cleanupResult = await this.cleanupOldReports(this.retentionDays.reports);
            results.oldReportsCleanup = cleanupResult;
            
            console.log(`[GLOBAL] Cleanup completed successfully. Reports saved to ${this.reportBasePath}/${date}/`);
            
        } catch (error) {
            console.error(`[GLOBAL] Error during cleanup operations:`, error);
            results.error = error.message;
        }

        return results;
    }

    /**
     * Clean up old media files for all active instances
     */
    async cleanupMedia(daysToKeep) {
        const results = {
            instances: 0,
            totalDeleted: 0,
            errors: [],
            details: []
        };

        try {
            // Get all active WhatsApp instances (global - all apps)
            const [instances] = await promisePool.query(
                `SELECT token as instance_id, team_id, name, app 
                 FROM sp_accounts 
                 WHERE social_network = 'whatsapp' 
                 AND login_type = '2' 
                 AND status = 1`
            );

            results.instances = instances.length;
            console.log(`Found ${instances.length} active instances for global media cleanup`);

            // Process each instance
            for (const instance of instances) {
                try {
                    console.log(`Cleaning media for instance ${instance.instance_id} (${instance.name})`);
                    
                    // Use the WAZIPER cleanupInstanceMedia function
                    const cleanupResult = await this.waziper.cleanupInstanceMedia(instance.instance_id, daysToKeep);
                    
                    if (cleanupResult.success) {
                        results.totalDeleted += cleanupResult.deleted || 0;
                        results.details.push({
                            instance_id: instance.instance_id,
                            name: instance.name,
                            deleted: cleanupResult.deleted || 0,
                            message: cleanupResult.message || 'Success'
                        });
                        console.log(`Media cleanup completed for ${instance.instance_id}: ${cleanupResult.deleted || 0} files deleted`);
                    } else {
                        results.errors.push({
                            instance_id: instance.instance_id,
                            error: cleanupResult.error || 'Unknown error'
                        });
                        console.error(`Media cleanup failed for ${instance.instance_id} (app: ${instance.app}):`, cleanupResult.error);
                    }
                } catch (error) {
                    results.errors.push({
                        instance_id: instance.instance_id,
                        error: error.message
                    });
                    console.error(`Error cleaning media for instance ${instance.instance_id}:`, error);
                }

                // Small delay between instances to avoid overwhelming the system
                await new Promise(resolve => setTimeout(resolve, 500));
            }

            results.success = true;
            results.message = `Cleaned ${results.totalDeleted} media files from ${results.instances} instances`;
        } catch (error) {
            console.error(`Error during global media cleanup:`, error);
            results.success = false;
            results.error = error.message;
        }

        return results;
    }

    /**
     * Clean up old message queue records
     */
    async cleanupMessageQueue(daysToKeep, batchSize = 1000) {
        const results = {
            deleted: 0,
            batches: 0,
            errors: []
        };

        console.log(`Starting global message queue cleanup - removing records older than ${daysToKeep} days`);
        
        let hasMore = true;
        let retryCount = 0;
        const maxRetries = 3;
        
        while (hasMore && retryCount < maxRetries) {
            try {
                // Global cleanup with batch processing and ordered deletion
                const [result] = await promisePool.query(
                    `DELETE FROM sp_whatsapp_message_queue 
                     WHERE status IN ('completed', 'failed') 
                     AND created_at < DATE_SUB(NOW(), INTERVAL ? DAY) 
                     ORDER BY id 
                     LIMIT ?`,
                    [daysToKeep, batchSize]
                );
                
                results.deleted += result.affectedRows;
                results.batches++;
                hasMore = result.affectedRows === batchSize;
                
                if (result.affectedRows > 0) {
                    console.log(`Message queue cleanup batch ${results.batches}: deleted ${result.affectedRows} records`);
                }
                
                // Small delay between batches to reduce lock contention
                if (hasMore) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
                
                // Reset retry count on successful batch
                retryCount = 0;
                
            } catch (error) {
                if (error.code === 'ER_LOCK_DEADLOCK') {
                    retryCount++;
                    const backoffDelay = Math.pow(2, retryCount) * 1000 + Math.random() * 1000;
                    console.log(`Deadlock detected during global message queue cleanup, retry ${retryCount}/${maxRetries} after ${backoffDelay}ms`);
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    continue;
                }
                
                console.error(`Error during global message queue cleanup:`, error);
                results.errors.push(error.message);
                break;
            }
        }
        
        if (retryCount >= maxRetries) {
            const errorMsg = `Global message queue cleanup failed after ${maxRetries} retries due to persistent deadlocks`;
            console.error(errorMsg);
            results.errors.push(errorMsg);
        }
        
        results.success = results.errors.length === 0;
        results.message = `Cleaned up ${results.deleted} old message queue records globally in ${results.batches} batches`;
        
        return results;
    }

    /**
     * Clean up old follow-up evaluation logs
     */
    async cleanupFollowUpLogs(daysToKeep) {
        const results = {
            deleted: 0,
            success: false,
            message: ''
        };

        try {
            const [result] = await promisePool.query(
                'DELETE FROM sp_whatsapp_follow_up_evaluation_log WHERE created_at < DATE_SUB(NOW(), INTERVAL ? DAY)',
                [daysToKeep]
            );
            
            results.deleted = result.affectedRows;
            results.success = true;
            results.message = `Cleaned up ${result.affectedRows} old follow-up evaluation log records`;
            
            console.log(`Global follow-up logs cleanup: ${results.message}`);
        } catch (error) {
            console.error(`Error cleaning up follow-up logs globally:`, error);
            results.success = false;
            results.message = `Failed to clean up follow-up logs: ${error.message}`;
        }

        return results;
    }

    /**
     * Clean up MongoDB messages based on per-instance retention settings
     */
    async cleanupMongoDBMessages(batchSize = 100) {
        const results = {
            instances: 0,
            totalDeleted: 0,
            errors: [],
            details: []
        };

        try {
            // Get MongoDB connection
            const { MongoClient } = require('mongodb');
            const mongoClient = new MongoClient(config.mongo_uri);
            await mongoClient.connect();
            const db = mongoClient.db('wabotv3');

            // Resolve an appropriate hint for deletion. If the compound index does not exist,
            // attempt to create it. Fallback gracefully to unhinted delete.
            const messagesCollection = db.collection('wabot_messages');
            let deletionHint = undefined;
            try {
                const indexes = await messagesCollection.listIndexes().toArray();
                const hasCompound = indexes.find(idx => {
                    const key = idx.key || {};
                    return key.instanceId === 1 && key.updatedAt === 1;
                });
                if (hasCompound) {
                    deletionHint = { instanceId: 1, updatedAt: 1 };
                } else {
                    // Try to create the index if missing
                    await messagesCollection.createIndex(
                        { instanceId: 1, updatedAt: 1 },
                        { name: 'messages_instance_updatedAt' }
                    );
                    deletionHint = { instanceId: 1, updatedAt: 1 };
                }
            } catch (e) {
                console.warn('[Cleanup] Could not resolve/create deletion index; proceeding without hint:', e?.message || e);
                deletionHint = undefined;
            }
            
            // Get all active WhatsApp instances with their retention settings
            const [instances] = await promisePool.query(
                `SELECT 
                    a.token as instance_id,
                    a.team_id,
                    a.name,
                    a.message_retention_days,
                    a.status_live_chat,
                    t.permissions
                 FROM sp_accounts a
                 LEFT JOIN sp_team t ON a.team_id = t.id
                 WHERE a.social_network = 'whatsapp' 
                 AND a.status = 1`
            );

            results.instances = instances.length;
            console.log(`Starting MongoDB message cleanup for ${instances.length} instances`);

            for (const instance of instances) {
                try {
                    // Calculate retention days for this instance
                    let retentionDays = 30; // Default
                    
                    // Try Redis cache first
                    const cachedRetention = await this.redis.get(`retention:${instance.instance_id}`);
                    if (cachedRetention) {
                        retentionDays = parseInt(cachedRetention);
                    } else {
                        // Calculate based on account settings
                        if (instance.message_retention_days > 0) {
                            retentionDays = instance.message_retention_days;
                        } else if (instance.status_live_chat === 0) {
                            retentionDays = 7;
                        } else if (instance.status_live_chat === 1 && instance.message_retention_days === 0) {
                            // Parse permissions to get live_chat_message_retention_period
                            const permissions = instance.permissions ? JSON.parse(instance.permissions) : {};
                            retentionDays = permissions.live_chat_message_retention_period || 30;
                        }
                        
                        // Cache the calculated retention
                        await this.redis.set(`retention:${instance.instance_id}`, retentionDays, 'EX', 86400);
                    }

                    // Calculate cutoff date
                    const cutoffDate = new Date(Date.now() - (retentionDays * 24 * 60 * 60 * 1000));
                    
                    // Delete messages older than retention period (messages only)
                    const deleteOptions = deletionHint ? { hint: deletionHint } : undefined;
                    const deleteResult = await messagesCollection.deleteMany(
                        {
                            instanceId: instance.instance_id,
                            updatedAt: { $lt: cutoffDate }
                        },
                        deleteOptions
                    );

                    if (deleteResult.deletedCount > 0) {
                        console.log(`Deleted ${deleteResult.deletedCount} messages for instance ${instance.instance_id} (retention: ${retentionDays} days)`);
                    }

                    results.totalDeleted += deleteResult.deletedCount;
                    results.details.push({
                        instance_id: instance.instance_id,
                        name: instance.name,
                        retention_days: retentionDays,
                        deleted: deleteResult.deletedCount
                    });

                    // Do not delete from chats or contacts here; cleanup is restricted to messages only

                } catch (error) {
                    results.errors.push({
                        instance_id: instance.instance_id,
                        error: error.message
                    });
                    console.error(`Error cleaning MongoDB data for instance ${instance.instance_id}:`, error);
                }

                // Small delay between instances
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            await mongoClient.close();
            
            results.success = true;
            results.message = `Cleaned ${results.totalDeleted} MongoDB messages from ${results.instances} instances`;
            
        } catch (error) {
            console.error(`Error during MongoDB message cleanup:`, error);
            results.success = false;
            results.error = error.message;
        }

        return results;
    }

    /**
     * Clean up orphaned subscribers (where instance_id doesn't exist in sp_accounts.token)
     * Adapted from cleanup-cron.js
     */
    async cleanupOrphanedSubscribers(batchSize = 1000) {
        const results = {
            deleted: 0,
            batches: 0,
            orphanedCount: 0,
            success: false,
            message: '',
            errors: []
        };
        
        try {
            console.log(`Starting global orphaned subscriber cleanup...`);
            
            // First, get count of orphaned records for reporting
            const countQuery = `
                SELECT COUNT(*) as orphaned_count 
                FROM sp_whatsapp_subscriber s 
                LEFT JOIN sp_accounts a ON s.instance_id = a.token 
                WHERE a.token IS NULL
            `;
            
            const [countResult] = await promisePool.query(countQuery);
            const orphanedCount = countResult[0].orphaned_count;
            results.orphanedCount = orphanedCount;
            
            console.log(`Found ${orphanedCount} orphaned subscriber records`);
            
            if (orphanedCount === 0) {
                results.success = true;
                results.message = `No orphaned subscribers found`;
                return results;
            }
            
            // Process in batches to avoid timeout
            let totalDeleted = 0;
            
            if (orphanedCount > batchSize) {
                console.log(`Processing ${orphanedCount} records in batches of ${batchSize}...`);
                
                let batchCount = 0;
                let deletedInBatch = 0;
                
                do {
                    batchCount++;
                    console.log(`Processing orphaned cleanup batch ${batchCount}...`);
                    
                    // First, get IDs of orphaned records in batch
                    const getIdsQuery = `
                        SELECT s.id 
                        FROM sp_whatsapp_subscriber s
                        LEFT JOIN sp_accounts a ON s.instance_id = a.token 
                        WHERE a.token IS NULL 
                        LIMIT ?
                    `;
                    
                    const [idsResult] = await promisePool.query(getIdsQuery, [batchSize]);
                    
                    if (idsResult.length === 0) {
                        deletedInBatch = 0;
                    } else {
                        // Extract the IDs
                        const idsToDelete = idsResult.map(row => row.id);
                        
                        // Delete by IDs
                        const batchQuery = `
                            DELETE FROM sp_whatsapp_subscriber 
                            WHERE id IN (${idsToDelete.map(() => '?').join(',')})
                        `;
                        
                        const [batchResult] = await promisePool.query(batchQuery, idsToDelete);
                        deletedInBatch = batchResult.affectedRows;
                        totalDeleted += deletedInBatch;
                        
                        console.log(`Orphaned cleanup batch ${batchCount}: deleted ${deletedInBatch} records (total: ${totalDeleted})`);
                    }
                    
                    // Small delay between batches to avoid overwhelming the database
                    if (deletedInBatch > 0) {
                        await new Promise(resolve => setTimeout(resolve, 2000)); // 2 second delay
                    }
                    
                } while (deletedInBatch > 0);
                
                results.batches = batchCount;
                
            } else {
                // If count is manageable, delete all at once
                const deleteQuery = `
                    DELETE s FROM sp_whatsapp_subscriber s
                    LEFT JOIN sp_accounts a ON s.instance_id = a.token 
                    WHERE a.token IS NULL
                `;
                
                console.log(`Deleting all orphaned records in single operation...`);
                const [result] = await promisePool.query(deleteQuery);
                totalDeleted = result.affectedRows;
                results.batches = 1;
            }
            
            results.deleted = totalDeleted;
            results.success = true;
            results.message = `Cleaned up ${totalDeleted} orphaned subscriber records`;
            
            console.log(`Orphaned subscriber cleanup completed: ${results.message}`);
            
        } catch (error) {
            console.error(`Error during orphaned subscriber cleanup:`, error);
            results.success = false;
            results.message = `Failed to clean up orphaned subscribers: ${error.message}`;
            results.errors.push(error.message);
        }
        
        return results;
    }

    /**
     * Start health check for the cleanup queue
     */
    startHealthCheck() {
        setInterval(async () => {
            try {
                const repeatableJobs = await this.cleanupQueue.getRepeatableJobs();
                if (repeatableJobs.length === 0) {
                    console.log(`No repeatable cleanup jobs found for ${this.queueName}, reinitializing...`);
                    await this.initialize();
                }
            } catch (error) {
                console.error(`Error in cleanup health check for ${this.queueName}:`, error);
            }
        }, 60 * 60 * 1000); // Check every hour
    }

    /**
     * Manual trigger for cleanup (useful for testing or on-demand cleanup)
     */
    async triggerManualCleanup() {
        console.log(`Manually triggering cleanup for app ${this.appVersion}`);
        
        try {
            const job = await this.cleanupQueue.add(
                'unified_cleanup',
                { 
                    timestamp: Date.now(), 
                    appVersion: this.appVersion,
                    manual: true
                },
                {
                    removeOnComplete: true,
                    removeOnFail: true
                }
            );
            
            console.log(`Manual cleanup job created with ID: ${job.id}`);
            return { success: true, jobId: job.id };
        } catch (error) {
            console.error(`Error triggering manual cleanup for app ${this.appVersion}:`, error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Manual trigger for orphaned subscriber backup only (without deletion)
     */
    async triggerManualBackup() {
        console.log(`Manually triggering orphaned subscriber backup for app ${this.appVersion}`);
        
        try {
            const date = new Date().toISOString().split('T')[0];
            const result = await this.backupOrphanedSubscribers(date);
            
            console.log(`Manual backup completed: ${result.recordCount} records backed up to ${result.filePath}`);
            return result;
        } catch (error) {
            console.error(`Error triggering manual backup for app ${this.appVersion}:`, error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get cleanup status and statistics
     */
    async getStatus() {
        try {
            const [waiting, active, completed, failed, delayed] = await Promise.all([
                this.cleanupQueue.getWaitingCount(),
                this.cleanupQueue.getActiveCount(),
                this.cleanupQueue.getCompletedCount(),
                this.cleanupQueue.getFailedCount(),
                this.cleanupQueue.getDelayedCount()
            ]);

            const repeatableJobs = await this.cleanupQueue.getRepeatableJobs();
            
            // Get latest report info
            let latestReport = null;
            try {
                const latestReportPath = path.join(this.reportBasePath, 'latest-report.json');
                if (fs.existsSync(latestReportPath)) {
                    const reportContent = await fs.promises.readFile(latestReportPath, 'utf8');
                    const report = JSON.parse(reportContent);
                    latestReport = {
                        timestamp: report.timestamp,
                        duration: report.durationFormatted,
                        summary: report.summary
                    };
                }
            } catch (e) {
                // Ignore if latest report doesn't exist
            }

            return {
                appVersion: this.appVersion,
                queue: this.queueName,
                reportPath: this.reportBasePath,
                status: {
                    waiting,
                    active,
                    completed,
                    failed,
                    delayed
                },
                repeatableJobs: repeatableJobs.map(job => ({
                    key: job.key,
                    name: job.name,
                    cron: job.cron,
                    tz: job.tz,
                    next: job.next
                })),
                retentionDays: this.retentionDays,
                latestReport
            };
        } catch (error) {
            console.error(`Error getting cleanup status for app ${this.appVersion}:`, error);
            return { error: error.message };
        }
    }

    /**
     * Update retention configuration
     */
    updateRetentionDays(type, days) {
        if (this.retentionDays.hasOwnProperty(type)) {
            this.retentionDays[type] = days;
            console.log(`Updated ${type} retention to ${days} days for app ${this.appVersion}`);
            return true;
        }
        return false;
    }

    /**
     * Graceful shutdown
     */
    async shutdown() {
        try {
            console.log(`Shutting down cleanup queue for app ${this.appVersion}`);
            
            // Pause the queue
            await this.cleanupQueue.pause(true);
            
            // Clean existing jobs
            await this.cleanExistingJobs();
            
            // Close the queue
            await this.cleanupQueue.close();
            
            // Close Redis connection
            await this.redis.quit();
            
            console.log(`Cleanup queue shutdown completed for app ${this.appVersion}`);
        } catch (error) {
            console.error(`Error during cleanup queue shutdown for app ${this.appVersion}:`, error);
        }
    }
}

// Export the class
module.exports = UnifiedCleanupManager;