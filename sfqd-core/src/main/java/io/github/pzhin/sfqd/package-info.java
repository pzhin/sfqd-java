/**
 * Generic, thread-safe SFQ(D) scheduling API.
 *
 * <p>The scheduler orders weighted, costed jobs and limits the number of jobs
 * dispatched but not yet completed. Execution remains the caller's
 * responsibility.
 */
package io.github.pzhin.sfqd;
