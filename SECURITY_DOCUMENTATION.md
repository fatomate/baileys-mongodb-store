# Security Documentation - Baileys MongoDB Store

## Table of Contents
1. [Security Audit Summary](#security-audit-summary)
2. [Implemented Security Features](#implemented-security-features)
3. [Memory Management](#memory-management)
4. [TTL Monitoring](#ttl-monitoring)
5. [Security Concepts](#security-concepts)
6. [Configuration Guide](#configuration-guide)

## Security Audit Summary

A comprehensive security audit was performed on the Baileys MongoDB store, identifying and addressing the following critical issues:

### Critical Findings (Resolved)
1. **Input Validation**: All user inputs are now validated to prevent NoSQL injection
2. **Access Control**: Multi-tenancy support with instance isolation implemented
3. **Error Handling**: Secure error messages prevent information disclosure

### Medium Priority Findings (Resolved)
1. **Memory Management**: Batch processing with backpressure control
2. **TTL Verification**: Automatic monitoring and alerts for data retention

## Implemented Security Features

### 1. Input Validation (`src/utils/security.ts`)
- **JID Validation**: WhatsApp-specific format validation
  - Individual users: `[0-9]+@s.whatsapp.net`
  - Groups: `[0-9]+-[0-9]+@g.us`
  - Linked devices: `[0-9]+@lid`
  - Contacts: `[0-9]+@c.us`
- **Message ID Validation**: Alphanumeric uppercase, 16-32 characters
- **Instance ID Validation**: Alphanumeric with hyphens/underscores, 3-50 characters
- **MongoDB Query Validation**: Blocks dangerous operators ($where, $expr, $function)

### 2. Authentication & Authorization (`src/utils/auth.ts`)
- **API Key Authentication**: Secure API key generation and validation
- **Access Tokens**: HMAC-signed tokens with expiration
- **Permission System**: Role-based access control
  - `read:own`, `write:own`, `delete:own` - Instance-specific permissions
  - `read:all`, `write:all`, `delete:all` - Admin permissions
- **Instance Isolation**: Automatic data filtering by instance
- **Session Management**: Secure session creation and validation

### 3. Error Handling
- **Safe Error Messages**: No sensitive information in error responses
- **Error Categories**: ValidationError, AuthorizationError, SecurityError
- **Logging**: Sanitized logging with data hashing for correlation

## Memory Management

### Features (`src/utils/memory.ts`)
- **Memory Monitor**: Real-time heap and RSS tracking
- **Backpressure Controller**: Automatic flow control
  - Pauses processing at 80% memory usage
  - Resumes at 60% memory usage
- **Batch Optimization**: Dynamic batch sizing based on memory pressure
- **Metrics Collection**: Performance tracking and reporting

### Configuration
```typescript
{
  memory: {
    maxMemoryMB: 512,        // Maximum memory threshold
    maxBatchSize: 1000,      // Maximum items per batch
    batchTimeWindowMs: 100,  // Batch accumulation window
    enableMonitoring: true   // Enable memory monitoring
  }
}
```

## TTL Monitoring

### Features (`src/utils/ttl.ts`)
- **Index Verification**: Validates TTL indexes after creation
- **Expired Document Detection**: Monitors for TTL failures
- **Alert System**: Configurable alerts for data retention issues
- **Manual Cleanup**: Fallback cleanup capabilities
- **Status Reporting**: Comprehensive TTL health reports

### Configuration
```typescript
{
  ttlDays: 30,
  ttlMonitoring: {
    enableMonitoring: true,
    checkIntervalMinutes: 60,
    alertThresholdDays: 1
  }
}
```

## Security Concepts

### Unit Testing
Unit tests verify individual components work correctly in isolation. Our security implementation includes 102 tests covering:
- Input validation functions
- Authentication mechanisms
- Authorization logic
- Memory management
- TTL monitoring

### Rate Limiting (Not Implemented)
Rate limiting prevents abuse by limiting requests per time period. While explained, it was not implemented per user request to avoid potential data loss in high-volume scenarios.

## Configuration Guide

### Basic Security Setup
```typescript
const store = await makeMongoDBStore({
  uri: 'mongodb://localhost:27017',
  database: 'baileys_db',
  instanceId: 'my-instance',
  auth: {
    enableApiKey: true,
    apiKeys: new Map([
      [generateApiKey(), 'instance-1']
    ])
  }
})
```

### Full Security Configuration
```typescript
const store = await makeMongoDBStore({
  uri: process.env.MONGODB_URI,
  database: 'baileys_secure',
  instanceId: 'production-instance',
  ttlDays: 30,
  auth: {
    enableApiKey: true,
    secretKey: process.env.JWT_SECRET,
    strictIsolation: true,
    allowedInstances: ['production-instance'],
    apiKeys: new Map([
      [process.env.API_KEY_1, 'instance-1'],
      [process.env.API_KEY_2, 'instance-2']
    ])
  },
  memory: {
    maxMemoryMB: 512,
    maxBatchSize: 1000,
    enableMonitoring: true
  },
  ttlMonitoring: {
    enableMonitoring: true,
    checkIntervalMinutes: 60,
    alertThresholdDays: 1
  }
})
```

### Environment Variables
```bash
# Required for production
MONGODB_URI=mongodb://localhost:27017
JWT_SECRET=your-secret-key-here
API_KEY_1=bmdb_generated_key_here
API_KEY_2=bmdb_another_key_here
```

### Generating API Keys
```typescript
import { generateApiKey } from '@baileys/mongodb-store/utils/auth'

const apiKey = generateApiKey()
console.log(apiKey) // bmdb_64_char_hex_string
```

## Best Practices

1. **Always use environment variables** for sensitive configuration
2. **Enable strict isolation** in multi-tenant environments
3. **Monitor memory usage** in production
4. **Set up TTL monitoring alerts** for compliance
5. **Rotate API keys** regularly
6. **Use HTTPS/TLS** for MongoDB connections
7. **Implement proper logging** without exposing sensitive data

## Troubleshooting

### TTL Not Working
- Check TTL monitor status: `await store.getTTLStatus()`
- Verify indexes: `await store.getIndexStatus()`
- Manual cleanup: Use TTL monitor's cleanup function

### Memory Issues
- Check stats: `store.getPerformanceStats().memoryStats`
- Reduce batch size configuration
- Increase memory limits if needed

### Authentication Failures
- Verify API key format: `bmdb_` prefix + 64 hex characters
- Check instance whitelist configuration
- Ensure secret key is consistent