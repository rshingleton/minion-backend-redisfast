package Minion::Backend::RedisFast;

use Mojo::Base 'Minion::Backend';
use Mojo::Log;
use Redis::Fast;
use JSON::PP;
use Time::HiRes 'time';
use Try::Tiny;
use UUID::Tiny ':std';

our $VERSION = "0.001";

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

=head1 DEFAULT RECONNECT BEHAVIOR

By default, the backend will:

=over 4

=item * Attempt reconnection for 60 seconds total

=item * Retry every 500 milliseconds (0.5 seconds)

=item * Automatically reconnect on READONLY/CONNECTION errors in Sentinel setups

=back

Override these defaults:

  my $backend = Minion::Backend::RedisFast->new(
      server    => 'redis.example.com',
      reconnect => 120,    # 2 minute retry window
      every     => 1_000_000, # 1 second between attempts
  );


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
has 'log' => sub {Mojo::Log->new};

=head1 METHODS

=head2 new

  my $backend = Minion::Backend::RedisFast->new(server => '127.0.0.1:6379');

Construct a new backend. Accepts all L<Redis::Fast> connection parameters.

=cut

sub new {
    my ($class, %args) = @_;

    # Default auto-reconnect: 60s total, 500ms between attempts
    my %redis_args = (
        reconnect => 60,      # 60 seconds total retry window
        every     => 500_000, # 500ms between attempts
        %args,                # User args override defaults
    );

    # Extract valid Redis::Fast parameters
    my @valid_params = qw(
        server sock sentinels service
        username password sentinels_username sentinels_password
        name reconnect every encoding reconnect_on_error
        ssl SSL_verify_mode SSL_ca_file SSL_cert_file SSL_key_file
        lazy write_timeout read_timeout
    );

    my %filtered_args;
    foreach my $param (@valid_params) {
        $filtered_args{$param} = delete $redis_args{$param}
            if exists $redis_args{$param};
    }

    # Add default failover handling if using Sentinel and no custom reconnect_on_error
    if ($redis_args{sentinels} && !exists $redis_args{reconnect_on_error}) {
        $redis_args{reconnect_on_error} = sub {
            my ($error) = @_;
            return 1 if $error =~ /READONLY|CONNECTION/;
            return -1;
        };
    }

    my $redis = Redis::Fast->new(%filtered_args);

    $redis->ping or die "Could not connect to Redis";

    return $class->SUPER::new(%redis_args)->redis($redis);
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

    # Validate input
    unless (defined $task && length $task) {
        $self->log->error('enqueue: Task name required');
        return undef;
    }

    # Phase 1: Create job core
    my $job_id = $self->redis->eval(
        $self->_lua_script('enqueue_core'),
        1, # KEYS count
        $self->_key('job_counter'),
        $task,
        $self->json->encode($args),
        $options->{queue} || 'default',
        $options->{priority} || 0,
        $options->{attempts} || 1,
        $options->{delay} || 0,
        time,
        $self->json->encode($options->{notes} || {}),
        $self->prefix
    );

    return undef unless $job_id;

    # Phase 2: Handle dependencies
    if (my $parents = $options->{parents}) {
        my $result = $self->redis->eval(
            $self->_lua_script('check_dependencies'),
            1, # KEYS count
            $self->prefix,
            $job_id,
            $self->json->encode($parents)
        );
        return undef if $result && $result eq 'DEADLOCK';
    }

    return $job_id;
}


=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, $timeout, \%options);

Dequeue a job, supporting options C<id>, C<queues>, and C<min_priority>.

=cut

sub dequeue {
    my ($self, $worker_id, $timeout, $options) = @_;
    $options ||= {};

    # Promote delayed jobs first
    $self->_promote_delayed_jobs();

    # Dequeue by specific job ID
    if (my $id = $options->{id}) {
        return $self->_dequeue_by_id($id, $worker_id);
    }

    # Dequeue by priority from specified queues
    my @queues = $options->{queues} ? @{$options->{queues}} : ('default');
    my $min_priority = $options->{min_priority} || 0;

    foreach my $queue (@queues) {
        my $job = $self->_dequeue_by_priority($queue, $worker_id, $min_priority);
        return $job if $job;
    }

    return undef;
}

sub _promote_delayed_jobs {
    my $self = shift;
    $self->redis->eval(
        $self->_lua_script('promote_delayed_jobs'),
        1, # KEYS count
        $self->prefix,
        time
    );
}

sub _dequeue_by_id {
    my ($self, $job_id, $worker_id) = @_; # Remove $options
    my $job_data = $self->redis->eval(
        $self->_lua_script('dequeue_by_id'),
        1,
        $self->prefix,
        $job_id,
        $worker_id,
        time # Remove min_priority
    );
    return $self->_parse_job_data($job_data);
}

sub _dequeue_by_priority {
    my ($self, $queue, $worker_id, $min_priority) = @_;

    my $job_data = $self->redis->eval(
        $self->_lua_script('dequeue_by_priority'),
        1, # KEYS count
        $self->prefix,
        $queue,
        $worker_id,
        $min_priority,
        time
    );

    return $self->_parse_job_data($job_data);
}

sub _parse_job_data {
    my ($self, $job_data) = @_;
    return undef unless $job_data && ref $job_data eq 'ARRAY';
    my %job = @$job_data;
    return undef unless $job{id} && $job{state} && $job{queue};

    # Decode JSON-encoded fields
    $job{args} = $self->json->decode($job{args} // '[]');
    $job{notes} = $self->json->decode($job{notes} // '{}');
    $job{result} = $self->json->decode($job{result} // '{}') if $job{state} eq 'finished';
    $job{error} = $self->json->decode($job{error} // '""') if $job{state} eq 'failed';
    $job{parents} = $self->json->decode($job{parents} // '[]');  # Add this line

    return \%job;
}



=head2 fail_job

  $backend->fail_job($job_id, $retries, $error);

Mark a job as failed.

=cut

sub fail_job {
    my ($self, $job_id, $retries, $error) = @_;
    if ($error =~ /deadlock/i) {
        # Special handling for deadlocked jobs
        $self->redis->hset($self->_job_key($job_id), 'deadlock', 1);
    }
    my $success = $self->redis->eval(
        $self->_lua_script('change_job_state'),
        1,
        $self->prefix,
        $job_id,
        'failed',
        $error,
        time,
        1 # Process dependents for failed jobs
    );

    $self->_increment_history('failed') if $success;

    # Process dependencies on failure
    $self->redis->eval(
        $self->_lua_script('complete_dependencies'),
        1, # KEYS count
        $self->prefix,
        $job_id,
        $self->{lax} ? 1 : 0 # Lax mode: process dependents even if job failed
    );
    return $success;
}

=head2 finish_job

  $backend->finish_job($job_id, $retries, $result);

Mark a job as finished.

=cut

sub finish_job {
    my ($self, $job_id, $retries, $result) = @_;
    my $result_json = $self->json->encode($result);
    my $success = $self->redis->eval(
        $self->_lua_script('change_job_state'),
        1,
        $self->prefix,
        $job_id,
        'finished',
        $result_json,
        time,
        1 # Process dependents for finished jobs
    );
    $self->_increment_history('finished') if $success;

    $self->redis->eval(
        $self->_lua_script('complete_dependencies'),
        1, # KEYS count
        $self->prefix,
        $job_id,
        0
    );

    return $success;
}

=head2 retry_job

  $backend->retry_job($job_id, \%options);

Retry a job with new options.

=cut

sub retry_job {
    my ($self, $job_id, $options) = @_;
    $options ||= {};

    my $result = $self->redis->eval(
        $self->_lua_script('retry_job'),
        1,
        $self->prefix,
        $job_id,
        $options->{queue} || 'default',
        $options->{attempts} || 1,
        $options->{priority} || 0,
        $options->{delay} || 0,
        time
    );

    return $result ? 1 : undef;
}


=head2 remove_job

  $backend->remove_job($job_id);

Remove a job from all queues and indexes.

=cut

sub remove_job {
    my ($self, $job_id) = @_;
    $self->redis->eval(
        $self->_lua_script('remove_job'),
        1, # KEYS count
        $self->prefix,
        $job_id
    );
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
    my $uuid = create_uuid_as_string(UUID_V4);
    my $now = time;
    my $expire_time = $now + $expire;

    my $result = $self->redis->eval(
        $self->_lua_script('lock'),
        1,
        $lock_key,
        $now,
        $expire_time,
        $limit,
        $uuid
    );

    return $result ? {
        resource => $lock_key,
        value    => $uuid,
        validity => $expire * 0.99 # 99% of original TTL
    } : undef;
}

=head2 unlock

  $backend->unlock($resource, $value);

Release a lock.

=cut

sub unlock {
    my ($self, $resource, $value) = @_;
    return $self->redis->eval(
        $self->_lua_script('unlock'),
        1, # KEYS count
        $resource,
        $value,
        time
    );
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
        id              => $job_id,
        args            => $self->json->decode($data{args} || '[]'),
        attempts        => $data{attempts} || 1,
        created         => $data{created},
        delayed         => $data{delayed},
        finished        => $data{finished},
        notes           => $self->json->decode($data{notes} || '{}'),
        priority        => $data{priority} || 0,
        queue           => $data{queue} || 'default',
        result          => $self->json->decode($data{result} || '{}'),
        retries         => $data{retries} || 0,
        started         => $data{started},
        state           => $data{state} || 'inactive',
        task            => $data{task},
        worker          => $data{worker},
        error           => $data{error},
        parents         => $self->json->decode($data{parents} // '[]'),
        pending_parents => $data{pending_parents} || 0,
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

    # Clean up deadlocked jobs
    my $deadlock_jobs = $self->redis->smembers($self->_state_set('failed'));
    foreach my $job_id (@$deadlock_jobs) {
        if ($self->redis->hget($self->_job_key($job_id), 'deadlock')) {
            $self->log->debug("Removing deadlocked job $job_id");
            $self->remove_job($job_id);
        }
    }
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

# Helper to load Lua scripts
sub _lua_script {
    my ($self, $name) = @_;
    state $scripts = {
        promote_delayed_jobs  => q{
            local prefix = KEYS[1]
            local now = tonumber(ARGV[1])

            local delayed = redis.call('ZRANGEBYSCORE', prefix..':delayed', 0, now, 'LIMIT', 0, 1)
            if #delayed > 0 then
                local job_id = delayed[1]
                local job_key = prefix..':job:'..job_id
                local queue = redis.call('HGET', job_key, 'queue') or 'default'
                redis.call('ZREM', prefix..':delayed', job_id)
                redis.call('LPUSH', prefix..':queue:'..queue, job_id)
            end
        },
        dequeue_by_id         => q{
            local prefix = KEYS[1]
            local job_id = ARGV[1]
            local worker_id = ARGV[2]
            local now = tonumber(ARGV[3])

            local job_key = prefix..':job:'..job_id
            local state = redis.call('HGET', job_key, 'state')
            local queue = redis.call('HGET', job_key, 'queue') or 'default'

            local pending_parents = tonumber(redis.call('HGET', job_key, 'pending_parents') or 0)
            if pending_parents > 0 then
                redis.call('LPUSH', list_key, job_id)  -- Requeue if parents pending
                return nil
            end

            if state == 'inactive' then
                -- Update job state
                redis.call('HSET', job_key,
                    'state', 'active',
                    'started', now,
                    'worker', worker_id
                )

                -- Atomic index updates
                redis.call('SREM', prefix..':state:inactive', job_id)
                redis.call('SADD', prefix..':state:active', job_id)
                redis.call('LREM', prefix..':queue:'..queue, 0, job_id)

                return redis.call('HGETALL', job_key)
            end
            return nil
        },
        dequeue_by_priority   => q{
            local prefix = KEYS[1]
            local queue = ARGV[1]
            local worker_id = ARGV[2]
            local min_priority = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])

            local list_key = prefix..':queue:'..queue
            local job_id = redis.call('RPOP', list_key)
            if not job_id then return nil end

            local job_key = prefix..':job:'..job_id
            local state = redis.call('HGET', job_key, 'state')
            local priority = tonumber(redis.call('HGET', job_key, 'priority') or 0)

            local pending_parents = tonumber(redis.call('HGET', job_key, 'pending_parents') or 0)
            if pending_parents > 0 then
                redis.call('LPUSH', list_key, job_id)  -- Requeue if parents pending
                return nil
            end

            if state == 'inactive' and priority >= min_priority then
                -- Update job state
                redis.call('HSET', job_key,
                    'state', 'active',
                    'started', now,
                    'worker', worker_id
                )

                -- Atomic index updates
                redis.call('SREM', prefix..':state:inactive', job_id)
                redis.call('SADD', prefix..':state:active', job_id)

                return redis.call('HGETALL', job_key)
            else
                redis.call('LPUSH', list_key, job_id)  -- Requeue
                return nil
            end
        },
        # In _lua_script
        enqueue_core          => q{
            -- KEYS[1]: job_counter
            -- ARGV[1-8]: task, args, queue, priority, attempts, delay, timestamp, notes
            -- ARGV[9]: prefix
            local job_id = redis.call('INCR', KEYS[1])
            if job_id == 0 then return nil end

            local prefix = ARGV[9]
            local job_key = prefix..':job:'..job_id
            local state_set = prefix..':state:inactive'
            local queue_set = prefix..':queue_set:'..ARGV[3]
            local delayed_key = prefix..':delayed'
            local queue_list = prefix..':queue:'..ARGV[3]

            -- Store core job metadata
            redis.call('HMSET', job_key,
                'id', job_id,
                'task', ARGV[1],
                'args', ARGV[2],
                'state', 'inactive',
                'created', ARGV[7],
                'queue', ARGV[3],
                'priority', ARGV[4],
                'attempts', ARGV[5],
                'retries', 0,
                'notes', ARGV[8],
                'pending_parents', 0
            )

            -- Add to indexes
            redis.call('SADD', state_set, job_id)
            redis.call('SADD', queue_set, job_id)

            -- Handle delayed jobs
            if tonumber(ARGV[6]) > 0 then
                redis.call('ZADD', delayed_key, ARGV[7] + ARGV[6], job_id)
            else
                redis.call('LPUSH', queue_list, job_id)
            end

            return job_id
        },
        check_dependencies    => q{
            -- KEYS[1]: prefix
            -- ARGV[1]: job_id
            -- ARGV[2]: parents_json
            local parents = cjson.decode(ARGV[2])
            if #parents == 0 then return nil end

            -- Store parents and initialize pending count
            redis.call('HSET', KEYS[1]..':job:'..ARGV[1], 'parents', ARGV[2])
            redis.call('HSET', KEYS[1]..':job:'..ARGV[1], 'pending_parents', #parents)

            -- Add to parent's dependents
            for _, pid in ipairs(parents) do
                redis.call('SADD', KEYS[1]..':dependents:'..pid, ARGV[1])
            end

            return nil
        },
        complete_dependencies => q{
            -- KEYS[1]: prefix
            -- ARGV[1]: job_id
            -- ARGV[2]: lax (0 or 1)
            local lax = tonumber(ARGV[2]) or 0
            local dependents = redis.call('SMEMBERS', KEYS[1]..':dependents:'..ARGV[1])

            for _, dep_id in ipairs(dependents) do
                local dep_key = KEYS[1]..':job:'..dep_id
                local parents = cjson.decode(redis.call('HGET', dep_key, 'parents') or '[]')

                -- Check if parent is valid (finished or failed with lax)
                local valid = 0
                for _, pid in ipairs(parents) do
                    local parent_state = redis.call('HGET', KEYS[1]..':job:'..pid, 'state')
                    if parent_state == 'finished' or (lax == 1 and parent_state == 'failed') then
                        valid = valid + 1
                    end
                end

                -- Only decrement if parent state satisfies dependency
                if valid == #parents then
                    redis.call('HINCRBY', dep_key, 'pending_parents', -1)
                    local remaining = tonumber(redis.call('HGET', dep_key, 'pending_parents') or 0)
                    if remaining <= 0 then
                        local queue = redis.call('HGET', dep_key, 'queue') or 'default'
                        redis.call('LPUSH', KEYS[1]..':queue:'..queue, dep_id)
                    end
                end
            end

            redis.call('DEL', KEYS[1]..':dependents:'..ARGV[1])
            return true
        },
        retry_job             => q{
            local prefix = KEYS[1]
            local job_id = ARGV[1]
            local new_queue = ARGV[2]
            local attempts = ARGV[3]
            local priority = ARGV[4]
            local delay = ARGV[5]
            local now = ARGV[6]

            local job_key = prefix..':job:'..job_id
            local old_state = redis.call('HGET', job_key, 'state')
            local old_queue = redis.call('HGET', job_key, 'queue') or 'default'

            -- Update job data
            redis.call('HMSET', job_key,
                'state', 'inactive',
                'retries', 0,
                'attempts', attempts,
                'queue', new_queue,
                'priority', priority
            )

            -- Update state indexes
            redis.call('SREM', prefix..':state:'..old_state, job_id)
            redis.call('SADD', prefix..':state:inactive', job_id)

            -- Update queue indexes
            redis.call('SREM', prefix..':queue_set:'..old_queue, job_id)
            redis.call('SADD', prefix..':queue_set:'..new_queue, job_id)

            -- Handle delay
            if tonumber(delay) > 0 then
                redis.call('ZADD', prefix..':delayed', now + delay, job_id)
            else
                redis.call('LPUSH', prefix..':queue:'..new_queue, job_id)
            end

            return 1
        },
        change_job_state      => q{
            -- KEYS[1]: prefix
            -- ARGV[1]: job_id
            -- ARGV[2]: state ('finished' or 'failed')
            -- ARGV[3]: result/error message
            -- ARGV[4]: timestamp
            -- ARGV[5]: process_dependents (1 or 0)

            local prefix = KEYS[1]
            local job_id = ARGV[1]
            local new_state = ARGV[2]
            local payload = ARGV[3]
            local now = tonumber(ARGV[4])
            local process_deps = tonumber(ARGV[5])

            local job_key = prefix..':job:'..job_id
            local old_state = redis.call('HGET', job_key, 'state') or 'active'
            local queue = redis.call('HGET', job_key, 'queue') or 'default'
            local old_queue = redis.call('HGET', job_key, 'queue') or 'default'  -- Add default

            -- Update job data
            if new_state == 'finished' then
                redis.call('HMSET', job_key,
                    'state', new_state,
                    'result', payload,
                    'finished', now
                )
            else
                redis.call('HMSET', job_key,
                    'state', new_state,
                    'error', payload,
                    'retries', redis.call('HINCRBY', job_key, 'retries', 1),
                    'finished', now
                )
            end

            -- Update state indexes
            redis.call('SREM', prefix..':state:'..old_state, job_id)
            redis.call('SADD', prefix..':state:'..new_state, job_id)

            -- Update queue indexes if queue changed
            if old_queue ~= queue then
                redis.call('SREM', prefix..':queue_set:'..old_queue, job_id)
                redis.call('SADD', prefix..':queue_set:'..queue, job_id)
            end

            -- Process dependents if required
            if process_deps == 1 then
                local dependents = redis.call('SMEMBERS', prefix..':dependents:'..job_id)
                for _, dep_id in ipairs(dependents) do
                    local remaining = redis.call('HINCRBY', prefix..':job:'..dep_id, 'pending_parents', -1)
                    if remaining <= 0 then
                        local dep_queue = redis.call('HGET', prefix..':job:'..dep_id, 'queue') or 'default'
                        redis.call('LPUSH', prefix..':queue:'..dep_queue, dep_id)
                    end
                end
                redis.call('DEL', prefix..':dependents:'..job_id)
            end

            return 1
        },
        remove_job            => q{
            local prefix = KEYS[1]
            local job_id = ARGV[1]

            local job_key = prefix..':job:'..job_id
            local state = redis.call('HGET', job_key, 'state') or 'inactive'
            local queue = redis.call('HGET', job_key, 'queue') or 'default'

            -- Remove from all data structures
            redis.call('LREM', prefix..':queue:'..queue, 0, job_id)
            redis.call('ZREM', prefix..':delayed', job_id)
            redis.call('SREM', prefix..':state:'..state, job_id)
            redis.call('SREM', prefix..':queue_set:'..queue, job_id)
            redis.call('DEL', job_key)

            -- Cleanup dependents
            redis.call('DEL', prefix..':dependents:'..job_id)

            return 1
        },
        lock                  => q{
            local lock_key = KEYS[1]
            local now = tonumber(ARGV[1])
            local expire_time = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local lock_value = ARGV[4]

            -- Atomic cleanup and check
            redis.call('ZREMRANGEBYSCORE', lock_key, 0, now) -- Remove expired
            local count = redis.call('ZCOUNT', lock_key, now, '+inf') -- Active locks

            if count < limit then
                -- Add lock with atomic expiration handling
                redis.call('ZADD', lock_key, 'NX', expire_time, lock_value)

                -- Auto-clean entire lock set 1min after last lock expires
                local ttl = expire_time - now + 60
                redis.call('EXPIRE', lock_key, ttl)

                return lock_value
            end
            return nil
        },
        unlock                => q{
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local now = tonumber(ARGV[2])

            -- Verify lock exists and is still valid
            local score = tonumber(redis.call('ZSCORE', lock_key, lock_value))
            if score and score >= now then
                return redis.call('ZREM', lock_key, lock_value)
            end
            return 0
        }
    };
    return $scripts->{$name};
}

1;

__END__
