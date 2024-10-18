/**
 * Test Setup
 * Runs before all tests to configure the test environment
 */

import { jest, beforeAll, afterAll } from '@jest/globals';

// Increase test timeout for integration tests with Docker
jest.setTimeout(120000);

// Suppress console output during tests (optional)
// Uncomment to reduce noise in test output
// global.console = {
//   ...console,
//   log: jest.fn(),
//   debug: jest.fn(),
//   info: jest.fn(),
//   warn: jest.fn(),
// };

// Global test setup
beforeAll(async () => {
  // Any global setup logic here
});

// Global test teardown
afterAll(async () => {
  // Any global cleanup logic here
  // Give time for async operations to complete
  await new Promise(resolve => setTimeout(resolve, 500));
});
