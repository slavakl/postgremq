# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of PostgreMQ seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please Do Not

- **Do not** open a public GitHub issue for security vulnerabilities
- **Do not** disclose the vulnerability publicly until it has been addressed

### How to Report

**Please report security vulnerabilities by emailing:** security@[your-domain].com

*(Note: Update this email address before publishing)*

Alternatively, you can use GitHub's private vulnerability reporting feature:

1. Go to the [Security tab](https://github.com/slavakl/postgremq/security)
2. Click "Report a vulnerability"
3. Fill out the form with details

### What to Include

Please include the following information in your report:

- **Type of vulnerability** (e.g., SQL injection, authentication bypass, etc.)
- **Full paths of source files** related to the vulnerability
- **Location of the affected code** (tag/branch/commit or direct URL)
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact** of the vulnerability - what an attacker could do
- **Suggested fix** (if you have one)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Assessment**: We will investigate and determine the severity and impact within 5 business days
- **Updates**: We will keep you informed of our progress
- **Fix**: We will work on a fix and coordinate the release timeline with you
- **Credit**: We will credit you in the security advisory (unless you prefer to remain anonymous)

### Disclosure Policy

- We aim to patch critical vulnerabilities within 30 days
- We will coordinate disclosure timing with you
- We will publish a security advisory on GitHub after the patch is released
- We appreciate your patience and coordination during this process

## Security Considerations for PostgreMQ

When using PostgreMQ, please be aware of the following security considerations:

### Database Security

PostgreMQ operates directly on your PostgreSQL database. Ensure:

- **Connection Security**: Use SSL/TLS for database connections in production
- **Authentication**: Use strong passwords and consider certificate-based authentication
- **Least Privilege**: Grant only necessary permissions to PostgreMQ database users
- **Network Security**: Restrict database access to authorized networks only

### SQL Injection

PostgreMQ uses parameterized queries throughout to prevent SQL injection. However:

- **Queue/Topic Names**: Do not allow untrusted user input directly as queue or topic names
- **Custom Queries**: If extending PostgreMQ with custom SQL, always use parameterized queries

### Message Payload Security

- **Data Encryption**: PostgreMQ does not encrypt message payloads - use application-level encryption for sensitive data
- **Payload Validation**: Always validate and sanitize message payloads before processing
- **Message Size**: Be aware of message size limits to prevent resource exhaustion

### Access Control

- **Database Permissions**: Control who can create topics, queues, and publish messages via PostgreSQL roles and permissions
- **Consumer Tokens**: PostgreMQ uses random tokens to track message ownership - ensure sufficient entropy in your PostgreSQL installation

### Denial of Service

Protect against DoS attacks:

- **Rate Limiting**: Implement rate limiting at the application level
- **Message Size Limits**: Enforce reasonable message size limits
- **Queue Depth Monitoring**: Monitor queue depths and set up alerts
- **Connection Pooling**: Configure appropriate connection pool limits

### Transaction Isolation

- PostgreMQ operations may involve multiple database operations
- Be aware of transaction isolation levels and potential race conditions
- Test your application under concurrent load

### Visibility Timeout

- **Timeout Values**: Set appropriate visibility timeouts - too short may cause duplicate processing, too long delays retries
- **Token Security**: Consumer tokens are used to extend visibility timeouts - these are random but not cryptographically secure identifiers

## Known Security Limitations

### Not Cryptographically Secure

- Consumer tokens are random but not cryptographically secure
- Message IDs are sequential integers (SERIAL)
- Do not rely on PostgreMQ for cryptographic security

### Database-Level Security

PostgreMQ inherits PostgreSQL's security model:

- Any user with database access can potentially read all messages
- Use PostgreSQL's row-level security if needed for message isolation
- Consider running PostgreMQ in a dedicated database

## Security Updates

Security updates will be announced via:

- GitHub Security Advisories
- Release notes in CHANGELOG.md
- GitHub releases

## Best Practices

### Production Deployment

1. **Use SSL/TLS** for all database connections
2. **Restrict database access** to authorized hosts only
3. **Monitor logs** for suspicious activity
4. **Keep dependencies updated** (PostgreSQL, client libraries)
5. **Implement rate limiting** at the application level
6. **Use secrets management** for database credentials
7. **Enable PostgreSQL audit logging** if required for compliance
8. **Backup regularly** and test restore procedures
9. **Test disaster recovery** procedures
10. **Monitor resource usage** (connections, disk space, queue depths)

### Development and Testing

1. **Never use production credentials** in development
2. **Use separate databases** for testing
3. **Do not commit secrets** to version control
4. **Use environment variables** for configuration
5. **Review code changes** for security implications

## Responsible Disclosure

We believe in responsible disclosure and will:

- Work with security researchers to understand and fix vulnerabilities
- Provide credit to researchers (unless they prefer anonymity)
- Coordinate disclosure timing
- Publish security advisories after fixes are released

Thank you for helping keep PostgreMQ and its users safe!
