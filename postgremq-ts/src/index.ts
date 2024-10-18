/**
 * PostgreMQ TypeScript Client
 * Main entry point for the PostgreMQ client
 */

// Export main interfaces and types
export { IConnection as Connection } from './types';
export { Message } from './message';
export { Consumer } from './consumer';

// Export public types
export {
  ConnectionOptions,
  PublishOptions,
  QueueOptions,
  ConsumerOptions,
  MessageOptions,
  QueueInfo,
  QueueStatistics,
  DLQMessage,
  QueueMessage,
  PublishedMessage,
  Transaction
} from './types';

// Import implementation classes
import { Connection as ConnectionImpl } from './connection';
import { ConnectionOptions } from './types';

/**
 * Create and connect a new PostgreMQ client
 * @param options - Connection options
 * @returns A connected client
 */
export async function connect(options: ConnectionOptions = {}): Promise<ConnectionImpl> {
  const client = new ConnectionImpl(options);
  await client.connect();
  return client;
}

// Create default exports
const PostgreMQ = {
  connect
};

export default PostgreMQ; 