# Bounded resource-pool lifecycle example

`BoundedResourcePoolIntegration` is a compiled and tested lifecycle example,
not a reusable production adapter. It demonstrates only the lifecycle boundary
between an application, SFQ(D), and a bounded external resource pool. The
`sfqd-examples` module is not part of `sfqd-core` and is skipped during Maven
deployment.

```text
application computes cost
         |
         v
       SFQ(D)
         |
      dispatch
         |
         v
external resource pool
         |
     completion
         |
         v
scheduler.complete(handle)
```

The example deliberately uses a local `ResourcePool<T>` interface instead of
depending on a database, connection pool, executor, or other resource
implementation.

## Run the walkthrough

Run the deterministic lifecycle walkthrough without the test suite:

```shell
./mvnw --batch-mode --no-transfer-progress \
  -pl sfqd-examples -am -DskipTests verify
```

It simulates two bounded resources, a synchronous submission failure, terminal
success and task-failure callbacks, automatic redispatch after completion, and
one unused resource. The final line is:

```text
BOUNDED_RESOURCE_POOL_EXAMPLE PASS
```

The `verify` phase runs this walkthrough automatically, so the documented
entry point and lifecycle assertions are part of the normal build gate.

## Lifecycle contract

1. Configure scheduler issue depth `D` to equal the pool's maximum number of
   concurrently issued resources.
2. Call `onResourcesAvailable(available)` only with capacity observed to be
   free now. Never report the same free resource twice.
3. Every `Dispatch` returned by the scheduler is already running and cannot be
   rolled back.
4. If submission throws, the example integration calls `complete()` for that
   dispatch before propagating the failure.
5. For an accepted task, the pool invokes its terminal callback exactly once
   after success, failure, or cancellation. It must invoke the callback only
   after `ResourcePool.execute()` returns. The callback calls `complete()`.
6. The terminal callback offers the released resource to the scheduler again.
7. `onResourcesAvailable` returns the number of reported resources that SFQ(D)
   did not use. Those resources remain free; the example never acquires them.

The example pulls one dispatch at a time with `dispatchUpTo(1)`. A whole
batch would become running atomically before the first pool submission. If
that submission failed, later batch entries could otherwise be left running
without ever reaching the pool.

The example does not define coordination between concurrent capacity signals;
that policy belongs to the application and its resource pool.

The application still computes cost and owns admission:

```java
long cost = costModel.estimate(task);
scheduler.enqueue(flowHandle, jobId, task, cost);

// Called when the pool reports resources that are actually free.
integration.onResourcesAvailable(available);
```

See
[`BoundedResourcePoolIntegration.java`](src/main/java/io/github/pzhin/sfqd/examples/BoundedResourcePoolIntegration.java)
for the example integration and
[`BoundedResourcePoolExample.java`](src/main/java/io/github/pzhin/sfqd/examples/BoundedResourcePoolExample.java)
for the runnable walkthrough.
