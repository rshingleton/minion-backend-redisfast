[![Actions Status](https://github.com/rshingleton/minion-backend-redisfast/actions/workflows/test.yml/badge.svg)](https://github.com/rshingleton/minion-backend-redisfast/actions)
# NAME

Minion::Backend::RedisFast - High-performance Redis backend for Minion job queue

# SYNOPSIS

    use Minion::Backend::RedisFast;

    # Pass connection parameters (recommended for most users)
    my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

    # Use Sentinels, possibly with password
    my $backend = Minion::Backend::RedisFast->new((
      sentinels => [ '10.0.0.1:16379', '10.0.0.2:16379', ... ],
      service   => 'my_main',
      sentinels_password => 'TheB1gS3CR3T', # optional
      reconnect_on_error => sub {
          my ($error) = @_;
          return 1 if $error =~ /READONLY|CONNECTION/;
          return -1;
      }
    );

    # Or pass an existing Redis::Fast object
    use Redis::Fast;
    my $redis = Redis::Fast->new(server => '127.0.0.1:6379');
    my $backend2 = Minion::Backend::RedisFast->new(redis => $redis);

    # Use with Minion
    use Minion;
    my $minion = Minion->new(backend => $backend);

# DESCRIPTION

[Minion::Backend::RedisFast](https://metacpan.org/pod/Minion%3A%3ABackend%3A%3ARedisFast) is a high-performance, fully-featured Redis backend for [Minion](https://metacpan.org/pod/Minion),
using [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast) for maximum speed and reliability. It supports all Minion features, including:

- Multiple named queues
- Job priorities and delayed jobs
- Efficient stats and job listing using secondary indexes
- Job history with event-driven hourly buckets
- Shared and exclusive locking with atomic Lua
- Job dependencies, retries, and notes
- Scalable to millions or billions of jobs

This backend is suitable for production workloads and large-scale Minion deployments.

# CONSTRUCTOR

## new

    my $backend = Minion::Backend::RedisFast->new(%options);

Construct a new backend. You may either:

- Pass a [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast) object:

        my $backend = Minion::Backend::RedisFast->new(redis => $redis);

- Or pass connection parameters for [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast):

        my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

All other options are forwarded to [Minion::Backend](https://metacpan.org/pod/Minion%3A%3ABackend).

# ATTRIBUTES

## redis

    my $redis = $backend->redis;
    $backend = $backend->redis(Redis::Fast->new);

[Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast) client used for all Redis operations.

## prefix

    my $prefix = $backend->prefix;
    $backend = $backend->prefix('minion');

Namespace prefix for all Redis keys.

## json

    my $json = $backend->json;

[JSON::PP](https://metacpan.org/pod/JSON%3A%3APP) encoder/decoder.

# METHODS

## broadcast

    $backend->broadcast($command, \@args, \@ids);

Broadcast a message to all workers.

## enqueue

    my $job_id = $backend->enqueue($task, \@args, \%options);

Add a new job to the queue. Supports `queue`, `priority`, `delay`, `attempts`, `parents`, and `notes`.

## dequeue

    my $job_info = $backend->dequeue($worker_id, $timeout, \%options);

Dequeue a job. Supports `id`, `queues`, and `min_priority` options.

## fail\_job

    $backend->fail_job($job_id, $retries, $error);

Mark a job as failed.

## finish\_job

    $backend->finish_job($job_id, $retries, $result);

Mark a job as finished.

## retry\_job

    $backend->retry_job($job_id, \%options);

Retry a job with new options (`attempts`, `priority`, `queue`, `delay`).

## remove\_job

    $backend->remove_job($job_id);

Remove a job from all queues and indexes.

## list\_jobs

    $backend->list_jobs($offset, $limit, \%options);

List jobs with optional filters (`state`, `queues`, `task`) and pagination.

## stats

    my $stats = $backend->stats;

Efficiently return job and worker stats using secondary indexes.

## history

    my $history = $backend->history;

Return job history for the last 24 hours (hourly buckets, event-driven).

## lock

    my $lock = $backend->lock($name, $expire, \%options);

Acquire a shared or exclusive lock using a ZSET and Lua for atomicity. Supports `limit` (default 1 for exclusive).

## unlock

    $backend->unlock($resource, $value);

Release a lock.

## note

    $backend->note($job_id, \%notes);

Add, update, or remove notes for a job. Setting a note value to `undef` removes it.

## purge

    $backend->purge;

Remove finished jobs older than `remove_after` with no dependents.

## receive

    my $messages = $backend->receive($worker_id);

Receive messages for a worker.

## register\_worker

    my $worker_id = $backend->register_worker($worker_id, \%options);

Register a worker.

## unregister\_worker

    $backend->unregister_worker($worker_id);

Unregister a worker.

## worker\_info

    my $info = $backend->worker_info($worker_id);

Get worker info.

## job\_info

    my $info = $backend->job_info($job_id);

Get job info.

## repair

    $backend->repair;

Clean up stale workers and stuck jobs.

## reset

    $backend->reset;

Delete all keys in the namespace.

# CONSTRUCTOR

## new

    my $backend = Minion::Backend::RedisFast->new(%options);

Construct a new backend. You may either:

- Pass a [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast) object:

        my $backend = Minion::Backend::RedisFast->new(redis => $redis);

- Or pass connection parameters for [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast):

        my $backend = Minion::Backend::RedisFast->new(
          server    => 'redis.example.com:6379',
          password  => 'secret',
          ssl       => 1,
          # ... other Redis::Fast parameters
        );

Supported Redis::Fast parameters include:

- Basic: `server`, `sock`, `name`, `password`, `username`
- Sentinel: `sentinels`, `service`, `sentinels_username`, `sentinels_password`
- TLS/SSL: `ssl`, `SSL_verify_mode`, `SSL_ca_file`, `SSL_cert_file`, `SSL_key_file`
- Timeouts: `write_timeout`, `read_timeout`
- Reconnection: `reconnect`, `every`, `reconnect_on_error`
- Advanced: `encoding`, `lazy`

For the full list and descriptions, see ["CONNECTION ARGUMENTS" in Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast#CONNECTION-ARGUMENTS).

All other options are forwarded to [Minion::Backend](https://metacpan.org/pod/Minion%3A%3ABackend).

# DEFAULT RECONNECT BEHAVIOR

By default, the backend will:

- Attempt reconnection for 60 seconds total
- Retry every 500 milliseconds (0.5 seconds)
- Automatically reconnect on READONLY/CONNECTION errors in Sentinel setups

Override these defaults:

    my $backend = Minion::Backend::RedisFast->new(
        server    => 'redis.example.com',
        reconnect => 120,    # 2 minute retry window
        every     => 1_000_000, # 1 second between attempts
    );

# PERFORMANCE

This backend uses secondary indexes (Redis sets) for job states and queues, providing O(1) stats and fast, scalable job listing and filtering. All locking is atomic and safe for distributed use. Event-driven job history is efficient and suitable for production analytics.

# SEE ALSO

[Minion](https://metacpan.org/pod/Minion), [Minion::Backend](https://metacpan.org/pod/Minion%3A%3ABackend), [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast)

# AUTHOR

Your Name <your@email.com>

# COPYRIGHT AND LICENSE

Copyright (C) 2025 Your Name

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# METHODS

## new

    my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

Construct a new backend. Accepts all [Redis::Fast](https://metacpan.org/pod/Redis%3A%3AFast) connection parameters.

## broadcast

    $backend->broadcast($command, \@args, \@ids);

Broadcast a message to all workers.

## enqueue

    my $job_id = $backend->enqueue($task, \@args, \%options);

Add a new job to the queue.

## dequeue

    my $job_info = $backend->dequeue($worker_id, $timeout, \%options);

Dequeue a job, supporting options `id`, `queues`, and `min_priority`.

## fail\_job

    $backend->fail_job($job_id, $retries, $error);

Mark a job as failed.

## finish\_job

    $backend->finish_job($job_id, $retries, $result);

Mark a job as finished.

## retry\_job

    $backend->retry_job($job_id, \%options);

Retry a job with new options.

## remove\_job

    $backend->remove_job($job_id);

Remove a job from all queues and indexes.

## list\_jobs

    $backend->list_jobs($offset, $limit, \%options);

List jobs with optional filters and pagination.

## stats

    my $stats = $backend->stats;

Efficiently return job and worker stats using secondary indexes.

## history

    my $history = $backend->history;

Return job history for the last 24 hours.

## lock

    my $lock = $backend->lock($name, $expire, \%options);

Acquire a shared or exclusive lock using a ZSET and Lua for atomicity.

## unlock

    $backend->unlock($resource, $value);

Release a lock.

## note

    $backend->note($job_id, \%notes);

Add, update, or remove notes for a job.

## purge

    $backend->purge;

Remove finished jobs older than `remove_after` with no dependents.

## receive

    my $messages = $backend->receive($worker_id);

Receive messages for a worker.

## register\_worker

    my $worker_id = $backend->register_worker($worker_id, \%options);

Register a worker.

## unregister\_worker

    $backend->unregister_worker($worker_id);

Unregister a worker.

## worker\_info

    my $info = $backend->worker_info($worker_id);

Get worker info.

## job\_info

    my $info = $backend->job_info($job_id);

Get job info.

## repair

    $backend->repair;

Clean up stale workers and stuck jobs.

## reset

    $backend->reset;

Delete all keys in the namespace.
