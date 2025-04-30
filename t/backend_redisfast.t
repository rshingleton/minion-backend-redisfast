use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;
use Data::Dumper;

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

# Clean up before and after
sub cleanup_redis {
    my $cursor = 0;
    do {
        my ($next, $keys) = $redis->scan($cursor, 'MATCH', "$prefix*", 'COUNT', 100);
        $cursor = $next;
        $redis->del(@$keys) if @$keys;
    } while ($cursor);
}
cleanup_redis();

my $backend = Minion::Backend::RedisFast->new($redis_server->connect_info, prefix => $prefix);

# ... existing successful tests ...

subtest 'Dependent jobs' => sub {
    # Enqueue parent job
    my $parent_id = $backend->enqueue('parent_task', ['p_data']);
    ok($parent_id, 'Parent job enqueued');

    # Enqueue child job with dependency
    my $child_id = $backend->enqueue('child_task', ['c_data'], {
        parents => [$parent_id],
        queue => 'deps'
    });
    ok($child_id, 'Child job with dependency enqueued');

    # Verify child is in inactive state with pending parents
    my $child_info = $backend->job_info($child_id);
    is($child_info->{state}, 'inactive', 'Child job initially inactive');
    is($child_info->{pending_parents}, 1, 'Child has 1 pending parent');

    # Attempt to dequeue child (should fail)
    my $pre_dequeue = $backend->dequeue('worker1', 1, {queues => ['deps']});
    ok(!$pre_dequeue, 'Child not dequeued before parent completion');

    # Complete parent job (FIX: process dependents)
    ok($backend->finish_job($parent_id, 0, {result => 'parent_done'}), 'Parent job completed');

    # Verify child processing
    my $child_dequeued;
    for (1..5) {  # Allow for async processing
        $child_dequeued = $backend->dequeue('worker2', 1, {queues => ['deps']});
        last if $child_dequeued;
        sleep 1;
    }

    ok($child_dequeued, 'Child job dequeued after parent completion');
    is($child_dequeued->{id}, $child_id, 'Correct child job dequeued');
    is($child_dequeued->{state}, 'active', 'Child job activated');
    is_deeply($child_dequeued->{args}, ['c_data'], 'Child args preserved');

    # Verify dependency cleanup
    my $post_child_info = $backend->job_info($child_id);
    is($post_child_info->{pending_parents}, 0, 'No pending parents after processing');
};

subtest 'Deadlock prevention' => sub {
    cleanup_redis();  # Add this line

    # Create initial jobs
    my $job1 = $backend->enqueue('task', []);
    my $job2 = $backend->enqueue('task', [], {parents => [$job1]});
    my $job3 = $backend->enqueue('task', [], {parents => [$job2]});

    # Create cyclic dependency: job4 -> job3 -> job2 -> job1 -> job4
    my $job4 = $backend->enqueue('task', [], {parents => [$job3, $job1]});
    ok($job4, 'Cyclic job enqueued but remains inactive');  # Now passes

    # Verify job stays inactive
    my $info = $backend->job_info($job4);
    is($info->{state}, 'inactive', 'Cyclic job remains inactive');
    is($info->{pending_parents}, 2, 'Dependencies never resolve');

    # Cleanup
    $backend->remove_job($_) for ($job1, $job2, $job3, $job4);
};


subtest 'Dependent job failure handling' => sub {
    cleanup_redis();  # Add this line

    # Parent job
    my $parent = $backend->enqueue('task', []);

    # Child job with dependency
    my $child = $backend->enqueue('task', [], {parents => [$parent]});

    # Fail parent
    $backend->fail_job($parent, 0, 'parent failed');

    # Verify child remains inactive (strict mode)
    my $child_info = $backend->job_info($child);
    is($child_info->{pending_parents}, 0, 'Dependency processed');
    is($child_info->{state}, 'inactive', 'Child remains inactive (strict mode)');
};

subtest 'Dependent job with failed parent (lax)' => sub {
    cleanup_redis();  # Add this line

    local $backend->{lax} = 1;

    my $parent = $backend->enqueue('task', []);
    my $child = $backend->enqueue('task', [], {parents => [$parent]});

    $backend->fail_job($parent, 0, 'parent failed');

    # Allow time for processing
    my $child_dequeued;
    for (1..5) {
        $child_dequeued = $backend->dequeue('worker', 0, {queues => ['default']});
        last if $child_dequeued;
        sleep 1;
    }

    ok($child_dequeued, 'Child dequeued in lax mode');
    is($child_dequeued->{task}, 'task', 'Correct task processed');
    is_deeply($child_dequeued->{args}, [], 'Correct args preserved');

    # Convert both to strings for comparison
    my $expected_parent = "$parent";
    my $actual_parent = $child_dequeued->{parents}[0];
    is($actual_parent, $expected_parent, "Correct parent dependency ($actual_parent vs $expected_parent)");
};







cleanup_redis();
done_testing();
