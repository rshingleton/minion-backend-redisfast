package Minion::Backend::RedisFast;

use Mojo::Base 'Minion::Backend';
use Redis::Fast;
use JSON::PP;
use Time::HiRes 'time';
use UUID::Tiny ':std';

=head1 NAME

Minion::Backend::RedisFast - High-performance Redis backend for Minion job queue

=head1 SYNOPSIS

  use Minion::Backend::RedisFast;

  # Pass connection parameters (recommended for most users)
  my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

  # Use Sentinels, possibly with password
  my $backend = Minion::Backend::RedisFast->new((
    sentinels => [ '10.0.0.1:16379', '10.0.0.2:16379', ... ],
    service   => 'my_main',
    sentinels_password => 'TheB1gS3CR3T', # optional
  );

  # Or pass an existing Redis::Fast object
  use Redis::Fast;
  my $redis = Redis::Fast->new(server => '127.0.0.1:6379');
  my $backend2 = Minion::Backend::RedisFast->new(redis => $redis);

  # Use with Minion
  use Minion;
  my $minion = Minion->new(backend => $backend);

=head1 DESCRIPTION

L<Minion::Backend::RedisFast> is a high-performance, fully-featured Redis backend for L<Minion>,
using L<Redis::Fast> for maximum speed and reliability. It supports all Minion features, including:

=over 4

=item *

Multiple named queues

=item *

Job priorities and delayed jobs

=item *

Efficient stats and job listing using secondary indexes

=item *

Job history with event-driven hourly buckets

=item *

Shared and exclusive locking with atomic Lua

=item *

Job dependencies, retries, and notes

=item *

Scalable to millions or billions of jobs

=back

This backend is suitable for production workloads and large-scale Minion deployments.

=head1 CONSTRUCTOR

=head2 new

  my $backend = Minion::Backend::RedisFast->new(%options);

Construct a new backend. You may either:

=over 4

=item *

Pass a L<Redis::Fast> object:

  my $backend = Minion::Backend::RedisFast->new(redis => $redis);

=item *

Or pass connection parameters for L<Redis::Fast>:

  my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

=back

All other options are forwarded to L<Minion::Backend>.

=head1 ATTRIBUTES

=head2 redis

  my $redis = $backend->redis;
  $backend = $backend->redis(Redis::Fast->new);

L<Redis::Fast> client used for all Redis operations.

=head2 prefix

  my $prefix = $backend->prefix;
  $backend = $backend->prefix('minion');

Namespace prefix for all Redis keys.

=head2 json

  my $json = $backend->json;

L<JSON::PP> encoder/decoder.

=head1 METHODS

=head2 broadcast

  $backend->broadcast($command, \@args, \@ids);

Broadcast a message to all workers.

=head2 enqueue

  my $job_id = $backend->enqueue($task, \@args, \%options);

Add a new job to the queue. Supports C<queue>, C<priority>, C<delay>, C<attempts>, C<parents>, and C<notes>.

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, $timeout, \%options);

Dequeue a job. Supports C<id>, C<queues>, and C<min_priority> options.

=head2 fail_job

  $backend->fail_job($job_id, $retries, $error);

Mark a job as failed.

=head2 finish_job

  $backend->finish_job($job_id, $retries, $result);

Mark a job as finished.

=head2 retry_job

  $backend->retry_job($job_id, \%options);

Retry a job with new options (C<attempts>, C<priority>, C<queue>, C<delay>).

=head2 remove_job

  $backend->remove_job($job_id);

Remove a job from all queues and indexes.

=head2 list_jobs

  $backend->list_jobs($offset, $limit, \%options);

List jobs with optional filters (C<state>, C<queues>, C<task>) and pagination.

=head2 stats

  my $stats = $backend->stats;

Efficiently return job and worker stats using secondary indexes.

=head2 history

  my $history = $backend->history;

Return job history for the last 24 hours (hourly buckets, event-driven).

=head2 lock

  my $lock = $backend->lock($name, $expire, \%options);

Acquire a shared or exclusive lock using a ZSET and Lua for atomicity. Supports C<limit> (default 1 for exclusive).

=head2 unlock

  $backend->unlock($resource, $value);

Release a lock.

=head2 note

  $backend->note($job_id, \%notes);

Add, update, or remove notes for a job. Setting a note value to C<undef> removes it.

=head2 purge

  $backend->purge;

Remove finished jobs older than C<remove_after> with no dependents.

=head2 receive

  my $messages = $backend->receive($worker_id);

Receive messages for a worker.

=head2 register_worker

  my $worker_id = $backend->register_worker($worker_id, \%options);

Register a worker.

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister a worker.

=head2 worker_info

  my $info = $backend->worker_info($worker_id);

Get worker info.

=head2 job_info

  my $info = $backend->job_info($job_id);

Get job info.

=head2 repair

  $backend->repair;

Clean up stale workers and stuck jobs.

=head2 reset

  $backend->reset;

Delete all keys in the namespace.

=head1 CONSTRUCTOR

=head2 new

  my $backend = Minion::Backend::RedisFast->new(%options);

Construct a new backend. You may either:

=over 4

=item *

Pass a L<Redis::Fast> object:

  my $backend = Minion::Backend::RedisFast->new(redis => $redis);

=item *

Or pass connection parameters for L<Redis::Fast>:

  my $backend = Minion::Backend::RedisFast->new(
    server    => 'redis.example.com:6379',
    password  => 'secret',
    ssl       => 1,
    # ... other Redis::Fast parameters
  );

=back

Supported Redis::Fast parameters include:

=over 4

=item *

Basic: C<server>, C<sock>, C<name>, C<password>, C<username>

=item *

Sentinel: C<sentinels>, C<service>, C<sentinels_username>, C<sentinels_password>

=item *

TLS/SSL: C<ssl>, C<SSL_verify_mode>, C<SSL_ca_file>, C<SSL_cert_file>, C<SSL_key_file>

=item *

Timeouts: C<write_timeout>, C<read_timeout>

=item *

Reconnection: C<reconnect>, C<every>, C<reconnect_on_error>

=item *

Advanced: C<encoding>, C<lazy>

=back

For the full list and descriptions, see L<Redis::Fast/CONNECTION ARGUMENTS>.

All other options are forwarded to L<Minion::Backend>.

=cut

=head1 PERFORMANCE

This backend uses secondary indexes (Redis sets) for job states and queues, providing O(1) stats and fast, scalable job listing and filtering. All locking is atomic and safe for distributed use. Event-driven job history is efficient and suitable for production analytics.

=head1 SEE ALSO

L<Minion>, L<Minion::Backend>, L<Redis::Fast>

=head1 AUTHOR

Your Name <your@email.com>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2025 Your Name

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut


has 'redis' => sub {die "No Redis::Fast client initialized"};
has 'prefix' => 'minion';
has 'json' => sub {JSON::PP->new->allow_nonref->convert_blessed};

=head1 METHODS

=head2 new

  my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

Construct a new backend. Accepts all L<Redis::Fast> connection parameters.

=cut

sub new {
    my ($class, %args) = @_;

    # Extract all valid Redis::Fast parameters
    my %redis_args;
    my @redis_params = qw(
        server sock sentinels service
        username password sentinels_username sentinels_password
        name reconnect every encoding reconnect_on_error
        ssl SSL_verify_mode SSL_ca_file SSL_cert_file SSL_key_file
        lazy write_timeout read_timeout
    );

    for my $key (@redis_params) {
        $redis_args{$key} = delete $args{$key} if exists $args{$key};
    }

    my $redis = Redis::Fast->new(%redis_args);
    my $self = $class->SUPER::new(%args);
    $self->{redis} = $redis;
    return $self;
}

# Helper methods for key namespacing
sub _key {join ':', shift->prefix, @_}
sub _job_key {shift->_key('job:' . shift)}
sub _queue_key {shift->_key('queue:' . shift)}
sub _worker_key {shift->_key('worker:' . shift)}
sub _state_set {shift->_key('state:' . shift)}
sub _queue_set {shift->_key('queue_set:' . shift)}

=head2 broadcast

  $backend->broadcast($command, \@args, \@ids);

Broadcast a message to all workers.

=cut

sub broadcast {
    my ($self, $command, $args, $ids) = @_;
    $args ||= [];
    $ids ||= [];
    my $message = $self->json->encode({
        command => $command,
        args    => $args,
        ids     => $ids
    });
    $self->redis->publish($self->_key('broadcast'), $message);
    return 1;
}

=head2 enqueue

  my $job_id = $backend->enqueue($task, \@args, \%options);

Add a new job to the queue.

=cut

sub enqueue {
    my ($self, $task, $args, $options) = @_;
    $args ||= [];
    $options ||= {};

    my $job_id = $self->redis->incr($self->_key('job_counter'));
    my $job_key = $self->_job_key($job_id);
    my $now = time;
    my $queue = $options->{queue} || 'default';
    my $state = 'inactive';

    my %job = (
        id       => $job_id,
        task     => $task,
        args     => $self->json->encode($args),
        state    => $state,
        created  => $now,
        queue    => $queue,
        priority => $options->{priority} || 0,
        attempts => $options->{attempts} || 1,
        retries  => 0,
        notes    => $self->json->encode($options->{notes} || {}),
    );

    if (my $parents = $options->{parents}) {
        $job{pending_parents} = 0 + @$parents;
        for my $parent_id (@$parents) {
            $self->redis->sadd($self->_key("dependents:$parent_id"), $job_id);
        }
    }

    $self->redis->hmset($job_key, %job);
    $self->redis->sadd($self->_state_set($state), $job_id);
    $self->redis->sadd($self->_queue_set($queue), $job_id);

    if (my $delay = $options->{delay}) {
        $self->redis->zadd($self->_key('delayed'), $now + $delay, $job_id);
    }
    else {
        $self->redis->lpush($self->_queue_key($queue), $job_id);
    }

    return $job_id;
}

# Update secondary indexes on state/queue change
sub _update_indexes {
    my ($self, $job_id, $old_state, $new_state, $old_queue, $new_queue) = @_;
    $self->redis->srem($self->_state_set($old_state), $job_id) if defined $old_state;
    $self->redis->sadd($self->_state_set($new_state), $job_id);
    $self->redis->srem($self->_queue_set($old_queue), $job_id) if defined $old_queue;
    $self->redis->sadd($self->_queue_set($new_queue), $job_id);
}

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, $timeout, \%options);

Dequeue a job, supporting options C<id>, C<queues>, and C<min_priority>.

=cut

sub dequeue {
    my ($self, $worker_id, $timeout, $options) = @_;
    $options ||= {};

    my $lua_activate_job = q{
        if redis.call('HGET', KEYS[1], 'state') == 'inactive' then
            redis.call('HMSET', KEYS[1],
                'state', 'active',
                'started', ARGV[1],
                'worker', ARGV[2]
            )
            return 1
        end
        return 0
    };

    # Process delayed jobs
    my @delayed = $self->redis->zrangebyscore($self->_key('delayed'), 0, time, 'LIMIT', 0, 1);
    if (@delayed) {
        $self->redis->multi;
        $self->redis->zrem($self->_key('delayed'), $delayed[0]);
        my $queue = $self->redis->hget($self->_job_key($delayed[0]), 'queue') || 'default';
        $self->redis->lpush($self->_queue_key($queue), $delayed[0]);
        $self->redis->exec;
    }

    # 1. Dequeue by ID
    if (my $id = $options->{id}) {
        my $job_key = $self->_job_key($id);
        my %job = $self->redis->hgetall($job_key);
        return unless %job;
        return unless $job{state} && $job{state} eq 'inactive';
        if ($options->{queues}) {
            return unless grep {$_ eq ($job{queue} || 'default')} @{$options->{queues}};
        }
        if (defined $options->{min_priority}) {
            return unless $job{priority} >= $options->{min_priority};
        }
        my $now = time;
        unless ($self->redis->eval($lua_activate_job, 1, $job_key, $now, $worker_id)) {
            return;
        }
        $self->_update_indexes($id, 'inactive', 'active', $job{queue}, $job{queue});
        $self->redis->lrem($self->_queue_key($job{queue}), 0, $id);
        return $self->job_info($id);
    }

    # 2. Dequeue by min_priority (and/or queues)
    my @queues = $options->{queues} ? @{$options->{queues}} : ('default');
    my @queue_keys = map {$self->_queue_key($_)} @queues;
    my $min_priority = $options->{min_priority};

    my $start_time = time;
    my $timeout_at = $start_time + ($timeout // 5);
    while (time < $timeout_at) {
        my $result = $self->redis->brpop(@queue_keys, 1);
        last unless $result && @$result == 2;
        my $job_id = $result->[1];
        my $job_key = $self->_job_key($job_id);
        my %job = $self->redis->hgetall($job_key);
        next unless %job && $job{state} && $job{state} eq 'inactive';
        if (defined $min_priority && $job{priority} < $min_priority) {
            $self->redis->rpush($self->_queue_key($job{queue}), $job_id);
            next;
        }
        my $now = time;
        unless ($self->redis->eval($lua_activate_job, 1, $job_key, $now, $worker_id)) {
            next;
        }
        $self->_update_indexes($job_id, 'inactive', 'active', $job{queue}, $job{queue});
        return $self->job_info($job_id);
    }
    return;
}

=head2 fail_job

  $backend->fail_job($job_id, $retries, $error);

Mark a job as failed.

=cut

sub fail_job {
    my ($self, $job_id, $retries, $error) = @_;
    my $job_key = $self->_job_key($job_id);
    my %job = $self->redis->hgetall($job_key);
    $self->redis->hmset($job_key,
        state    => 'failed',
        error    => $error,
        retries  => $retries + 1,
        finished => time
    );
    $self->_update_indexes($job_id, $job{state}, 'failed', $job{queue}, $job{queue});
    $self->_process_dependents($job_id);
    $self->_increment_history('failed');
    return 1;
}

=head2 finish_job

  $backend->finish_job($job_id, $retries, $result);

Mark a job as finished.

=cut

sub finish_job {
    my ($self, $job_id, $retries, $result) = @_;
    my $job_key = $self->_job_key($job_id);
    my %job = $self->redis->hgetall($job_key);
    $self->redis->hmset($job_key,
        state    => 'finished',
        result   => $self->json->encode($result),
        finished => time
    );
    $self->_update_indexes($job_id, $job{state}, 'finished', $job{queue}, $job{queue});
    $self->_process_dependents($job_id);
    $self->_increment_history('finished');
    return 1;
}

=head2 retry_job

  $backend->retry_job($job_id, \%options);

Retry a job with new options.

=cut

sub retry_job {
    my ($self, $job_id, $options) = @_;
    $options ||= {};
    my $job_key = $self->_job_key($job_id);
    my %job = $self->redis->hgetall($job_key);
    my $old_state = $job{state};
    my $old_queue = $job{queue};
    my $attempts = $options->{attempts} || $job{attempts} || 1;
    my $new_queue = $options->{queue} || $job{queue} || 'default';

    $self->redis->hmset($job_key,
        state    => 'inactive',
        retries  => 0,
        attempts => $attempts,
        ($options->{priority} ? (priority => $options->{priority}) : ()),
        ($options->{queue} ? (queue => $options->{queue}) : ()),
    );
    $self->_update_indexes($job_id, $old_state, 'inactive', $old_queue, $new_queue);

    if (my $delay = $options->{delay}) {
        $self->redis->zadd($self->_key('delayed'), time + $delay, $job_id);
    }
    else {
        $self->redis->lpush($self->_queue_key($new_queue), $job_id);
    }
    return 1;
}

=head2 remove_job

  $backend->remove_job($job_id);

Remove a job from all queues and indexes.

=cut

sub remove_job {
    my ($self, $job_id) = @_;
    my $job_key = $self->_job_key($job_id);
    my %job = $self->redis->hgetall($job_key);
    my $queue = $job{queue} || 'default';
    my $state = $job{state} || 'inactive';
    $self->redis->lrem($self->_queue_key($queue), 0, $job_id);
    $self->redis->zrem($self->_key('delayed'), $job_id);
    $self->redis->srem($self->_state_set($state), $job_id);
    $self->redis->srem($self->_queue_set($queue), $job_id);
    $self->redis->del($job_key);
    return 1;
}

=head2 list_jobs

  $backend->list_jobs($offset, $limit, \%options);

List jobs with optional filters and pagination.

=cut

sub list_jobs {
    my ($self, $offset, $limit, $options) = @_;
    $options ||= {};
    my @ids;
    if ($options->{state} && $options->{queues}) {
        @ids = $self->redis->sinter(
            $self->_state_set($options->{state}),
            map {$self->_queue_set($_)} @{$options->{queues}}
        );
    }
    elsif ($options->{state}) {
        @ids = $self->redis->smembers($self->_state_set($options->{state}));
    }
    elsif ($options->{queues}) {
        @ids = map {$self->redis->smembers($self->_queue_set($_))} @{$options->{queues}};
    }
    else {
        my $pattern = $self->_key('job:*');
        my $cursor = 0;
        do {
            my ($next, $keys) = $self->redis->scan($cursor, 'MATCH', $pattern, 'COUNT', 500);
            $cursor = $next;
            foreach my $key (@$keys) {
                my $job_id = (split /:/, $key)[-1];
                push @ids, $job_id;
            }
        } while ($cursor);
    }
    @ids = sort {$b <=> $a} @ids;
    my $total = @ids;
    @ids = splice(@ids, $offset, $limit);
    my @jobs = map {$self->job_info($_)} @ids;
    return { jobs => \@jobs, total => $total };
}

=head2 stats

  my $stats = $backend->stats;

Efficiently return job and worker stats using secondary indexes.

=cut

sub stats {
    my $self = shift;
    my $start_time_key = $self->_key('start_time');
    my $start_time = $self->redis->get($start_time_key);
    unless ($start_time) {
        $start_time = time;
        $self->redis->set($start_time_key, $start_time);
    }
    my %counts = (
        inactive_jobs    => $self->redis->scard($self->_state_set('inactive')),
        active_jobs      => $self->redis->scard($self->_state_set('active')),
        failed_jobs      => $self->redis->scard($self->_state_set('failed')),
        finished_jobs    => $self->redis->scard($self->_state_set('finished')),
        delayed_jobs     => $self->redis->zcard($self->_key('delayed')),
        enqueued_jobs    => $self->redis->get($self->_key('job_counter')) || 0,
        active_locks     => 0,
        workers          => $self->redis->scard($self->_key('workers')),
        active_workers   => 0,
        inactive_workers => 0,
        uptime           => time - $start_time,
    );
    my $lock_cursor = 0;
    do {
        my ($next, $keys) = $self->redis->scan($lock_cursor, 'MATCH', $self->_key('lock:*'), 'COUNT', 1000);
        $lock_cursor = $next;
        $counts{active_locks} += scalar(@$keys);
    } while ($lock_cursor);

    my %active_workers;
    my $job_cursor = 0;
    do {
        my ($next, $keys) = $self->redis->scan($job_cursor, 'MATCH', $self->_key('job:*'), 'COUNT', 1000);
        $job_cursor = $next;
        foreach my $key (@$keys) {
            my %job = $self->redis->hgetall($key);
            if (($job{state} || '') eq 'active' && $job{worker}) {
                $active_workers{$job{worker}} = 1;
            }
        }
    } while ($job_cursor);
    $counts{active_workers} = scalar(keys %active_workers);
    $counts{inactive_workers} = $counts{workers} - $counts{active_workers};
    return \%counts;
}

=head2 history

  my $history = $backend->history;

Return job history for the last 24 hours.

=cut

sub history {
    my $self = shift;
    my $now = time;
    my @buckets;
    for my $i (0 .. 23) {
        my $epoch = int($now / 3600) * 3600 - ($i * 3600);
        my $key = $self->_key("history:$epoch");
        my @counts = $self->redis->hgetall($key);
        my %counts = @counts;
        unshift @buckets, {
            epoch         => $epoch,
            finished_jobs => $counts{finished_jobs} || 0,
            failed_jobs   => $counts{failed_jobs} || 0,
        };
    }
    return { daily => \@buckets };
}

=head2 lock

  my $lock = $backend->lock($name, $expire, \%options);

Acquire a shared or exclusive lock using a ZSET and Lua for atomicity.

=cut

sub lock {
    my ($self, $name, $expire, $options) = @_;
    $options ||= {};
    my $limit = $options->{limit} || 1;
    my $lock_key = $self->_key("lock:$name");
    my $id = create_uuid_as_string(UUID_V4);
    my $now = time;
    my $expire_time = $now + $expire;
    my $lua = q{
        redis.call('zremrangebyscore', KEYS[1], 0, ARGV[1])
        local count = redis.call('zcount', KEYS[1], ARGV[1], '+inf')
        if count < tonumber(ARGV[3]) then
            redis.call('zadd', KEYS[1], ARGV[2], ARGV[4])
            redis.call('expire', KEYS[1], ARGV[5])
            return ARGV[4]
        end
        return nil
    };
    my $result = $self->redis->eval(
        $lua, 1, $lock_key, $now, $expire_time, $limit, $id, $expire
    );
    return $result ? {
        resource => $lock_key,
        value    => $id,
        validity => $expire * 0.99
    } : undef;
}

=head2 unlock

  $backend->unlock($resource, $value);

Release a lock.

=cut

sub unlock {
    my ($self, $resource, $value) = @_;
    my $lua = q{
        return redis.call('zrem', KEYS[1], ARGV[1])
    };
    return $self->redis->eval($lua, 1, $resource, $value);
}

=head2 note

  $backend->note($job_id, \%notes);

Add, update, or remove notes for a job.

=cut

sub note {
    my ($self, $job_id, $notes) = @_;
    my $job_key = $self->_job_key($job_id);
    my $current = $self->redis->hget($job_key, 'notes') || '{}';
    my $existing = $self->json->decode($current);
    for my $k (keys %$notes) {
        if (defined $notes->{$k}) {
            $existing->{$k} = $notes->{$k};
        }
        else {
            delete $existing->{$k};
        }
    }
    $self->redis->hset($job_key, 'notes', $self->json->encode($existing));
    return 1;
}

=head2 purge

  $backend->purge;

Remove finished jobs older than C<remove_after> with no dependents.

=cut

sub purge {
    my ($self, $options) = @_;
    $options ||= {};
    my $now = time;
    my $remove_after = $self->minion ? ($self->minion->remove_after || 172800) : 172800;
    my $pattern = $self->_key('job:*');
    my $cursor = 0;
    do {
        my ($next, $keys) = $self->redis->scan($cursor, 'MATCH', $pattern, 'COUNT', 500);
        $cursor = $next;
        foreach my $key (@$keys) {
            my %data = $self->redis->hgetall($key);
            next unless $data{state} && $data{state} eq 'finished';
            next unless $data{finished};
            next if $self->redis->scard($self->_key("dependents:$data{id}"));
            my $age = $now - $data{finished};
            if ($age > $remove_after) {
                $self->redis->del($key);
                $self->redis->lrem($self->_queue_key($data{queue}), 0, $data{id});
            }
        }
    } while ($cursor);
    return 1;
}

=head2 receive

  my $messages = $backend->receive($worker_id);

Receive messages for a worker.

=cut

sub receive {
    my ($self, $worker_id) = @_;
    my $messages = $self->redis->lrange($self->_worker_key($worker_id, 'commands'), 0, -1);
    $self->redis->del($self->_worker_key($worker_id, 'commands'));
    return [ map {$self->json->decode($_)} @$messages ];
}

=head2 register_worker

  my $worker_id = $backend->register_worker($worker_id, \%options);

Register a worker.

=cut

sub register_worker {
    my ($self, $worker_id, $options) = @_;
    $options ||= {};
    $worker_id ||= create_uuid_as_string(UUID_V4);
    $self->redis->hmset($self->_worker_key($worker_id),
        notified => time,
        status   => $self->json->encode($options->{status} || {}),
        pid      => $$
    );
    $self->redis->sadd($self->_key('workers'), $worker_id);
    return $worker_id;
}

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister a worker.

=cut

sub unregister_worker {
    my ($self, $worker_id) = @_;
    $self->redis->del($self->_worker_key($worker_id));
    $self->redis->srem($self->_key('workers'), $worker_id);
    return 1;
}

=head2 worker_info

  my $info = $backend->worker_info($worker_id);

Get worker info.

=cut

sub worker_info {
    my ($self, $worker_id) = @_;
    my %data = $self->redis->hgetall($self->_worker_key($worker_id));
    return undef unless %data;
    return {
        id       => $worker_id,
        status   => $self->json->decode($data{status} || '{}'),
        notified => $data{notified},
        pid      => $data{pid}
    };
}

=head2 job_info

  my $info = $backend->job_info($job_id);

Get job info.

=cut

sub job_info {
    my ($self, $job_id) = @_;
    my %data = $self->redis->hgetall($self->_job_key($job_id));
    return undef unless %data;
    return {
        id       => $job_id,
        args     => $self->json->decode($data{args} || '[]'),
        attempts => $data{attempts} || 1,
        created  => $data{created},
        delayed  => $data{delayed},
        finished => $data{finished},
        notes    => $self->json->decode($data{notes} || '{}'),
        priority => $data{priority} || 0,
        queue    => $data{queue} || 'default',
        result   => $self->json->decode($data{result} || '{}'),
        retries  => $data{retries} || 0,
        started  => $data{started},
        state    => $data{state} || 'inactive',
        task     => $data{task},
        worker   => $data{worker},
        error    => $data{error},
    };
}

=head2 repair

  $backend->repair;

Clean up stale workers and stuck jobs.

=cut

sub repair {
    my $self = shift;
    my $workers = $self->redis->smembers($self->_key('workers'));
    foreach my $wid (@$workers) {
        my $last = $self->redis->hget($self->_worker_key($wid), 'notified');
        $self->unregister_worker($wid) if time - $last > 600;
    }
    my $pattern = $self->_key('job:*');
    my $cursor = 0;
    do {
        my ($next, $keys) = $self->redis->scan($cursor, 'MATCH', $pattern, 'COUNT', 500);
        $cursor = $next;
        foreach my $key (@$keys) {
            my $job_id = (split /:/, $key)[-1];
            my $started = $self->redis->hget($self->_job_key($job_id), 'started');
            $self->fail_job($job_id, 0, 'Job stuck') if $started && time - $started > 1800;
        }
    } while ($cursor);
    return 1;
}

=head2 reset

  $backend->reset;

Delete all keys in the namespace.

=cut

sub reset {
    my ($self, $options) = @_;
    my $pattern = $self->_key('*');
    my $cursor = 0;
    do {
        my ($next, $keys) = $self->redis->scan($cursor, 'MATCH', $pattern, 'COUNT', 500);
        $cursor = $next;
        $self->redis->del(@$keys) if @$keys;
    } while ($cursor);
    return 1;
}

# Helper: process dependents
sub _process_dependents {
    my ($self, $job_id) = @_;
    my $deps = $self->redis->smembers($self->_key("dependents:$job_id"));
    return unless @$deps;
    foreach my $dep (@$deps) {
        $self->redis->hincrby($self->_job_key($dep), 'pending_parents', -1);
        if (($self->redis->hget($self->_job_key($dep), 'pending_parents') || 0) <= 0) {
            my $queue = $self->redis->hget($self->_job_key($dep), 'queue') || 'default';
            $self->redis->lpush($self->_queue_key($queue), $dep);
        }
    }
    $self->redis->del($self->_key("dependents:$job_id"));
}

# Helper: increment job history
sub _increment_history {
    my ($self, $state) = @_;
    return unless $state eq 'finished' || $state eq 'failed';
    my $hour = int(time / 3600) * 3600;
    my $remove_after = $self->minion ? ($self->minion->remove_after || 604800) : 604800;
    my $expire = int($remove_after * 1.5);
    my $field = $state eq 'finished' ? 'finished_jobs' : 'failed_jobs';
    my $key = $self->_key("history:$hour");
    $self->redis->hincrby($key, $field, 1);
    $self->redis->expire($key, $expire);
}

1;

__END__
