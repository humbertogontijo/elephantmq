/**
 * Connection-related events shared by Queue, Worker, FlowProducer, QueueEvents.
 */
export interface EmqConnectionListener {
  /**
   * Emitted when the underlying pg pool / listener client is closing.
   */
  'connection:close': () => void;
}
