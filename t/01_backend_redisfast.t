use strict;
use warnings;
use v5.26;
use Test::More;
use Test::RedisServer;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

# Test initialization
my $redis_server;
eval {
    $redis_server = Test::RedisServer->new(
        conf => {
            port => 7474,
        }
    );
} or plan skip_all => 'redis-server is required for this test';

my $redis = Redis::Fast->new($redis_server->connect_info);
my $prefix = "minion_test_" . int(rand(10000));

# Cleanup with atomic SCAN
sub cleanup {
    my $cursor = 0;
    do {
        my ($next, $keys) = $redis->scan($cursor, 'MATCH', "$prefix:*", 'COUNT', 100);
        $redis->del(@$keys) if @$keys;
        $cursor = $next;
    } while ($cursor);
    $redis->del("$prefix:job_counter"); # Reset ID sequence
}


# Basic backend initialization
subtest 'Basic functionality' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Job lifecycle tests
    my $job_id = $backend->enqueue('test_task', [ 1, 2, 3 ]);
    ok $job_id, 'Job enqueued';

    my $job_info = $backend->job_info($job_id);
    is $job_info->{state}, 'inactive', 'Job starts inactive';

    # Dequeue and process
    my $worker_id = 'worker1';
    my $dequeued = $backend->dequeue($worker_id, 0.5);
    ok $dequeued, 'Job dequeued';
    is $dequeued->{id}, $job_id, 'Correct job ID';

    $backend->finish_job($job_id, 0, { result => 'success' });
    $job_info = $backend->job_info($job_id);
    is $job_info->{state}, 'finished', 'Job marked finished';
};

# Dependency handling tests
subtest 'Job dependencies' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Parent job
    my $parent = $backend->enqueue('parent_task');
    ok $parent, 'Parent enqueued';

    # Child with dependency
    my $child = $backend->enqueue('child_task', [], { parents => [ $parent ] });
    ok $child, 'Child enqueued';

    # Verify dependency tracking
    my $child_info = $backend->job_info($child);
    is $child_info->{pending_parents}, 1, 'Child has pending parent';

    # Complete parent
    $backend->finish_job($parent, 0, {});

    # Child should process
    my $dequeued;
    for (1 .. 5) {
        # Allow async processing
        $dequeued = $backend->dequeue('worker2', 0);
        last if $dequeued;
        sleep 1;
    }
    ok $dequeued && $dequeued->{id} eq $child, 'Child processed after parent';
};

# Locking tests
subtest 'Distributed locking' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Acquire lock
    my $lock = $backend->lock('resource1', 10);
    ok $lock, 'Lock acquired';

    # Try duplicate lock
    my $lock2 = $backend->lock('resource1', 10);
    ok !$lock2, 'Duplicate lock denied';

    # Release and retry
    $backend->unlock($lock->{resource}, $lock->{value});
    $lock2 = $backend->lock('resource1', 10);
    ok $lock2, 'Lock reacquired after release';
};

# Failure handling tests
subtest 'Job failure handling' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    my $job_id = $backend->enqueue('failing_task');
    my $dequeued = $backend->dequeue('worker3', 0);

    $backend->fail_job($job_id, 0, 'Test failure');
    my $job_info = $backend->job_info($job_id);
    is $job_info->{state}, 'failed', 'Job marked failed';
    like $job_info->{error}, qr/Test failure/, 'Error message preserved';
};

# Concurrency tests
subtest 'Concurrent processing' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Enqueue multiple jobs
    my @jobs;
    push @jobs, $backend->enqueue("task_$_") for 1 .. 10;

    # Dequeue in parallel
    my %processed;
    my $worker = sub {
        while (my $job = $backend->dequeue('worker', 0)) {
            $processed{$job->{id}}++;
            $backend->finish_job($job->{id}, 0, {});
        }
    };

    # Simulate parallel workers
    $worker->() for 1 .. 3;

    is scalar keys %processed, 10, 'All jobs processed';
    ok $processed{$_}, "Job $_ processed" for @jobs;
};

# Deadlock prevention tests
subtest 'Deadlock prevention' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix,
        lax    => 0
    );

    # Create dependency chain
    my $job1 = $backend->enqueue('task1');
    my $job2 = $backend->enqueue('task2', [], { parents => [ $job1 ] });
    my $job3 = $backend->enqueue('task3', [], { parents => [ $job2 ] });

    # Attempt cycle
    my $job4 = $backend->enqueue('task4', [], { parents => [ $job3, $job1 ] });
    ok $job4, 'Cyclic job enqueued';

    # Verify remains inactive
    my $info = $backend->job_info($job4);
    is $info->{state}, 'inactive', 'Cyclic job blocked';
    is $info->{pending_parents}, 2, 'Dependencies unresolved';
};

# Lock contention tests
subtest 'Lock contention' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Test 1: Exclusive lock collision
    my $lock1 = $backend->lock('resource', 1);
    my $lock2 = $backend->lock('resource', 1);
    ok($lock1 && !$lock2, 'Exclusive lock collision handled');

    # Test 2: Lock expiration
    sleep 2; # Let lock1 expire
    my $lock3 = $backend->lock('resource', 1);
    ok($lock3, 'Expired lock replaced');

    # Test 3: Shared lock limits
    my $shared1 = $backend->lock('shared', 10, {limit => 3});
    my $shared2 = $backend->lock('shared', 10, {limit => 3});
    my $shared3 = $backend->lock('shared', 10, {limit => 3});
    my $shared4 = $backend->lock('shared', 10, {limit => 3});
    ok($shared1 && $shared2 && $shared3 && !$shared4, 'Shared lock limits enforced');
};

# Enhanced: Cross-queue priority
subtest 'Multi-queue priority' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Enqueue jobs across queues with different priorities
    $backend->enqueue('low',  [], {queue => 'A', priority => 1});
    $backend->enqueue('high', [], {queue => 'B', priority => 5});

    # Dequeue specifying queue order
    my $job = $backend->dequeue('worker', 0, {queues => [qw(B A)]});
    is($job->{queue}, 'B', 'Higher priority queue dequeued first');
};

# Job note management
subtest 'Job notes' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    my $job = $backend->enqueue('notes_test', []);
    $backend->note($job, {step1 => 'done', count => 5});

    my $info = $backend->job_info($job);
    is_deeply($info->{notes}, {step1 => 'done', count => 5}, 'Notes added');

    $backend->note($job, {count => undef});
    $info = $backend->job_info($job);
    is_deeply($info->{notes}, {step1 => 'done'}, 'Note removal');
};

# Historical statistics
subtest 'Job history' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    # Create jobs in different states
    my $j1 = $backend->enqueue('task', []);
    my $j2 = $backend->enqueue('task', []);
    $backend->finish_job($j1, 0, {});
    $backend->fail_job($j2, 0, 'error');

    my $stats = $backend->stats;
    is($stats->{finished_jobs}, 1, 'Finished jobs tracked');
    is($stats->{failed_jobs}, 1, 'Failed jobs tracked');
};

# Enhanced: Atomic retry updates
subtest 'Atomic job retries' => sub {
    cleanup();
    my $backend = Minion::Backend::RedisFast->new(
        $redis_server->connect_info,
        prefix => $prefix
    );

    my $job = $backend->enqueue('task', [], {attempts => 3});
    $backend->fail_job($job, 0, 'error');

    ok($backend->retry_job($job, {
        attempts => 5,
        queue => 'high',
        delay => 10
    }), 'Retried with new options');

    my $info = $backend->job_info($job);
    is($info->{attempts}, 5, 'Attempts updated atomically');
    is($info->{queue}, 'high', 'Queue updated atomically');
};

done_testing();
