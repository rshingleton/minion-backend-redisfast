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

# Enqueue jobs to three queues with distinct data
my $job_a1 = $backend->enqueue('taskA', ['a1'], {queue => 'alpha'});
my $job_a2 = $backend->enqueue('taskA', ['a2'], {queue => 'alpha'});
my $job_b1 = $backend->enqueue('taskB', ['b1'], {queue => 'beta'});
my $job_b2 = $backend->enqueue('taskB', ['b2'], {queue => 'beta'});
my $job_c1 = $backend->enqueue('taskC', ['c1'], {queue => 'gamma'});

ok($job_a1 && $job_a2 && $job_b1 && $job_b2 && $job_c1, 'Jobs enqueued to multiple queues');

# Dequeue from 'alpha' should return jobs in FIFO order
my $w = 'workerQ';
my $dq_a1 = $backend->dequeue($w, 1, {queues => ['alpha']});
is($dq_a1->{id}, $job_a1, 'Dequeued oldest job from alpha');
is($dq_a1->{queue}, 'alpha', 'Job is from alpha queue');
is_deeply($dq_a1->{args}, ['a1'], 'Correct args for alpha job');

my $dq_a2 = $backend->dequeue($w, 1, {queues => ['alpha']});
is($dq_a2->{id}, $job_a2, 'Dequeued next job from alpha');
is($dq_a2->{queue}, 'alpha', 'Job is from alpha queue');
is_deeply($dq_a2->{args}, ['a2'], 'Correct args for alpha job');

# Dequeue from 'beta' queue (FIFO)
my $dq_b1 = $backend->dequeue($w, 1, {queues => ['beta']});
is($dq_b1->{id}, $job_b1, 'Dequeued oldest job from beta');
is($dq_b1->{queue}, 'beta', 'Job is from beta queue');
is_deeply($dq_b1->{args}, ['b1'], 'Correct args for beta job');

my $dq_b2 = $backend->dequeue($w, 1, {queues => ['beta']});
is($dq_b2->{id}, $job_b2, 'Dequeued next job from beta');
is($dq_b2->{queue}, 'beta', 'Job is from beta queue');
is_deeply($dq_b2->{args}, ['b2'], 'Correct args for beta job');

# Dequeue from 'gamma' queue
my $dq_c1 = $backend->dequeue($w, 1, {queues => ['gamma']});
is($dq_c1->{id}, $job_c1, 'Dequeued only job from gamma');
is($dq_c1->{queue}, 'gamma', 'Job is from gamma queue');
is_deeply($dq_c1->{args}, ['c1'], 'Correct args for gamma job');

# Enqueue more jobs to multiple queues
my $job_a3 = $backend->enqueue('taskA', ['a3'], {queue => 'alpha'});
my $job_b3 = $backend->enqueue('taskB', ['b3'], {queue => 'beta'});
my $job_c2 = $backend->enqueue('taskC', ['c2'], {queue => 'gamma'});

# Dequeue from multiple queues at once
my %expect = (
    $job_a3 => 'alpha',
    $job_b3 => 'beta',
    $job_c2 => 'gamma',
);
my %seen;
for (1..3) {
    my $dq = $backend->dequeue($w, 1, {queues => ['alpha', 'beta', 'gamma']});
    ok($dq, 'Dequeued from one of multiple queues');
    $seen{$dq->{id}}++;
    ok($expect{$dq->{id}}, 'Job id is expected');
    is($dq->{queue}, $expect{$dq->{id}}, 'Queue matches expected');
}
is(scalar(keys %seen), 3, 'All jobs from all queues dequeued');

# Ensure queues are now empty
for my $q ('alpha', 'beta', 'gamma') {
    my $dq = $backend->dequeue($w, 1, {queues => [$q]});
    ok(!$dq, "No jobs left in $q queue");
}

cleanup_redis();
done_testing();
