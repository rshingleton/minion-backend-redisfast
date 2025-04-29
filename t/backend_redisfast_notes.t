use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_notes_test_" . int(rand(10000));
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
