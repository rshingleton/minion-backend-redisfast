use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_retry_test_" . int(rand(10000));
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

# Enqueue job
my $job_id = $backend->enqueue('task', [], {attempts => 3, priority => 5, queue => 'alpha'});
ok($job_id, 'Job enqueued');

# Fail job to trigger retry
$backend->fail_job($job_id, 0, 'error');
my $job = $backend->job_info($job_id);
is($job->{retries}, 1, 'Job retries incremented');

# Retry with new options
ok($backend->retry_job($job_id, {
    attempts => 5,
    priority => 10,
    queue    => 'beta',
    delay    => 60,
}), 'Job retried with new options');

# Verify updates
$job = $backend->job_info($job_id);
is($job->{attempts}, 5, 'Attempts updated');
is($job->{priority}, 10, 'Priority updated');
is($job->{queue}, 'beta', 'Queue updated');
ok($redis->zscore($backend->_key('delayed'), $job_id), 'Job in delayed set');

cleanup_redis();
done_testing();
