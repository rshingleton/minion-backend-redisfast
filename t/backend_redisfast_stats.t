use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_stats_test_" . int(rand(10000));
my $redis = Redis::Fast->new(server => $redis_url);

sub cleanup_redis {
    my $cursor = 0;
    do {
        my ($next, $keys) = $redis->scan($cursor, 'MATCH', "$prefix*", 'COUNT', 100);
        $cursor = $next;
        $redis->del(@$keys) if @$keys;
    } while ($cursor);
}
cleanup_redis();

my $backend = Minion::Backend::RedisFast->new(server => $redis_url, prefix => $prefix);

# Enqueue jobs in different queues and states
my $job1 = $backend->enqueue('task1', [], {queue => 'alpha'});
my $job2 = $backend->enqueue('task2', [], {queue => 'beta'});
my $job3 = $backend->enqueue('task3', [], {queue => 'gamma'});

# Dequeue job1 to make it active
my $worker_id = 'worker1';
my $active_job = $backend->dequeue($worker_id, 1, {queues => ['alpha']});

# Fail job2
$backend->fail_job($job2, 0, 'fail_reason');

# Finish job3
$backend->finish_job($job3, 0, {result => 'done'});

# Register workers
my $wid1 = $backend->register_worker('worker1', {status => {foo => 1}});
my $wid2 = $backend->register_worker('worker2', {status => {bar => 2}});

# Acquire a lock to test active_locks
my $lock = $backend->lock('testlock', 10);

# Get stats
my $stats = $backend->stats;

# Validate stats fields and values
is($stats->{enqueued_jobs}, 3, 'Total enqueued jobs');
is($stats->{active_jobs}, 1, 'Active jobs count');
is($stats->{failed_jobs}, 1, 'Failed jobs count');
is($stats->{finished_jobs}, 1, 'Finished jobs count');
is($stats->{inactive_jobs}, 0, 'Inactive jobs count');
ok($stats->{delayed_jobs} >= 0, 'Delayed jobs count');
is($stats->{workers}, 2, 'Total workers count');
is($stats->{active_workers}, 1, 'Active workers count');
is($stats->{inactive_workers}, 1, 'Inactive workers count');
ok($stats->{uptime} > 0, 'Uptime is positive');
is($stats->{active_locks}, 1, 'Active locks count');

# Cleanup
cleanup_redis();

done_testing();
