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
    $redis_server = Test::RedisServer->new(
        conf => {
            port => 7474,
        }
    );
} or plan skip_all => 'redis-server is required for this test';

my $redis = Redis::Fast->new($redis_server->connect_info);
my $prefix = "minion_test_" . int(rand(10000));

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

# Enqueue jobs with varying priorities and queues
my $job1 = $backend->enqueue('task', ['a'], {queue => 'alpha', priority => 1});
my $job2 = $backend->enqueue('task', ['b'], {queue => 'alpha', priority => 5});
my $job3 = $backend->enqueue('task', ['c'], {queue => 'beta',  priority => 10});
my $job4 = $backend->enqueue('task', ['d'], {queue => 'beta',  priority => -2});

ok($job1 && $job2 && $job3 && $job4, 'Jobs enqueued with priorities');

# Test dequeue by id (should only dequeue if inactive)
my $worker_id = 'workerA';
my $dq2 = $backend->dequeue($worker_id, 1, {id => $job2});
ok($dq2, 'Dequeued by id');
is($dq2->{id}, $job2, 'Dequeued correct job by id');
is($dq2->{state}, 'active', 'Job is now active');
is($dq2->{priority}, 5, 'Job has correct priority');

# Try to dequeue the same job by id again (should return undef)
my $dq2_again = $backend->dequeue($worker_id, 1, {id => $job2});
ok(!$dq2_again, 'Cannot dequeue the same job by id twice');

# Test dequeue by min_priority (should skip jobs below threshold)
my $worker_id2 = 'workerB';
my $dq_alpha_minprio = $backend->dequeue($worker_id2, 1, {queues => ['alpha'], min_priority => 3});
ok(!$dq_alpha_minprio, 'No job dequeued from alpha with min_priority=3 (since only job1 remains, priority=1)');

# Dequeue from beta with min_priority=5 (should get job3, skip job4)
my $dq_beta_minprio = $backend->dequeue($worker_id2, 1, {queues => ['beta'], min_priority => 5});
ok($dq_beta_minprio, 'Dequeued from beta with min_priority=5');
is($dq_beta_minprio->{id}, $job3, 'Dequeued job3 from beta');
is($dq_beta_minprio->{priority}, 10, 'Job3 has priority 10');

# Dequeue from beta with min_priority=-1 (should skip job4, since priority=-2)
my $dq_beta_minprio2 = $backend->dequeue($worker_id2, 1, {queues => ['beta'], min_priority => -1});
ok(!$dq_beta_minprio2, 'No job dequeued from beta with min_priority=-1 (job4 priority=-2)');

# Dequeue by id from beta (job4, which is still inactive)
my $dq4 = $backend->dequeue($worker_id2, 1, {id => $job4});
ok($dq4, 'Dequeued job4 by id');
is($dq4->{id}, $job4, 'Dequeued correct job4 by id');
is($dq4->{priority}, -2, 'Job4 has priority -2');

cleanup_redis();
done_testing();
