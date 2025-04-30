use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new(conf => {port => 7474});
} or plan skip_all => 'redis-server is required for this test';

my $redis = Redis::Fast->new($redis_server->connect_info);
my $prefix = "minion_test_" . int(rand(10000));

sub cleanup_redis {
    my $cursor = 0;
    do {
        my ($next, $keys) = $redis->scan($cursor, 'MATCH', "$prefix:*", 'COUNT', 100);
        $cursor = $next;
        $redis->del(@$keys) if @$keys;
    } while ($cursor);
}

cleanup_redis();
my $backend = Minion::Backend::RedisFast->new($redis_server->connect_info, prefix => $prefix);

subtest 'Basic job enqueue' => sub {
    my $job_id = $backend->enqueue('test_task', ['arg1', 'arg2']);
    ok($job_id, 'Job enqueued successfully');

    my $job = $backend->job_info($job_id);
    is($job->{task}, 'test_task', 'Task name matches');
    is_deeply($job->{args}, ['arg1', 'arg2'], 'Arguments stored correctly');
    is($job->{state}, 'inactive', 'Initial state is inactive');

    my $redis = $backend->redis;
    ok($redis->sismember($backend->_state_set('inactive'), $job_id), 'In state index');
    ok($redis->sismember($backend->_queue_set('default'), $job_id), 'In queue index');

    # Fixed: Check if lpos returns defined value (0 is valid index)
    ok(defined $redis->lpos($backend->_queue_key('default'), $job_id), 'In default queue');

    cleanup_redis();
};

subtest 'Delayed job enqueue' => sub {
    my $job_id = $backend->enqueue('delayed_task', [], {delay => 60});
    ok($job_id, 'Delayed job enqueued');

    my $redis = $backend->redis;
    my $score = $redis->zscore($backend->_key('delayed'), $job_id);
    ok($score > time, 'Job in delayed set with future timestamp');
    ok(!$redis->lpos($backend->_queue_key('default'), $job_id), 'Not in immediate queue');

    cleanup_redis();
};

subtest 'Job with dependencies' => sub {
    my $parent_id = $backend->enqueue('parent_task', []);
    my $child_id = $backend->enqueue('child_task', [], {parents => [$parent_id]});

    my $redis = $backend->redis;
    is($redis->scard($backend->_key("dependents:$parent_id")), 1, 'Parent has dependent');
    is($redis->hget($backend->_job_key($child_id), 'pending_parents'), 1, 'Child has pending parents count');

    cleanup_redis();
};

subtest 'Custom queue enqueue' => sub {
    my $job_id = $backend->enqueue('custom_task', [], {queue => 'urgent'});
    ok($job_id, 'Job in custom queue');

    my $redis = $backend->redis;
    ok($redis->sismember($backend->_queue_set('urgent'), $job_id), 'In urgent queue index');

    # Fixed: Check if lpos returns defined value (0 is valid index)
    ok(defined $redis->lpos($backend->_queue_key('urgent'), $job_id), 'In urgent queue list');

    cleanup_redis();
};

done_testing();
