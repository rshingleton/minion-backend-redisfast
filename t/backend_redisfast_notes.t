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

# Enqueue a job with no notes
my $job_id = $backend->enqueue('task', []);
ok($job_id, 'Job enqueued');

# Add notes
ok($backend->note($job_id, {foo => 'bar', count => 1}), 'Added notes');
my $job = $backend->job_info($job_id);
is_deeply($job->{notes}, {foo => 'bar', count => 1}, 'Notes added correctly');

# Update a note
ok($backend->note($job_id, {foo => 'baz'}), 'Updated note');
$job = $backend->job_info($job_id);
is_deeply($job->{notes}, {foo => 'baz', count => 1}, 'Note updated correctly');

# Remove a note field
ok($backend->note($job_id, {count => undef}), 'Removed note field');
$job = $backend->job_info($job_id);
is_deeply($job->{notes}, {foo => 'baz'}, 'Note field removed correctly');

# Remove the last field (notes should become empty hash)
ok($backend->note($job_id, {foo => undef}), 'Removed last note field');
$job = $backend->job_info($job_id);
is_deeply($job->{notes}, {}, 'All notes removed, notes is empty hash');

cleanup_redis();
done_testing();
