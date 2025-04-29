# In t/backend_redisfast_job_removal.t
use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_remove_test_" . int(rand(10000));
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

# Enqueue job to default queue
my $job1 = $backend->enqueue('task1', []);
ok($job1, 'Job enqueued to default queue');

# Enqueue job to custom queue
my $job2 = $backend->enqueue('task2', [], {queue => 'critical'});
ok($job2, 'Job enqueued to critical queue');

# Enqueue delayed job
my $job3 = $backend->enqueue('task3', [], {delay => 60});
ok($job3, 'Delayed job enqueued');

# Remove job1 (default queue)
ok($backend->remove_job($job1), 'Job removed from default queue');
is($backend->job_info($job1), undef, 'Job1 metadata deleted');
is($redis->lindex($backend->_queue_key('default'), 0), undef, 'Job1 not in default queue');

# Remove job2 (critical queue)
ok($backend->remove_job($job2), 'Job removed from critical queue');
is($backend->job_info($job2), undef, 'Job2 metadata deleted');
is($redis->lindex($backend->_queue_key('critical'), 0), undef, 'Job2 not in critical queue');

# Remove job3 (delayed)
ok($backend->remove_job($job3), 'Delayed job removed');
is($backend->job_info($job3), undef, 'Job3 metadata deleted');
is($redis->zscore($backend->_key('delayed'), $job3), undef, 'Job3 not in delayed set');

cleanup_redis();
done_testing();
