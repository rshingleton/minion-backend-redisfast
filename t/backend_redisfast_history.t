use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::Fast;
use FindBin;
use Time::HiRes 'time';
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

# Enqueue jobs
my $job1 = $backend->enqueue('task', ['a']);
my $job2 = $backend->enqueue('task', ['b']);
my $job3 = $backend->enqueue('task', ['c']);

ok($job1 && $job2 && $job3, 'Jobs enqueued');

# Mark job1 and job2 as finished, job3 as failed
$backend->finish_job($job1, 0, {result => 'ok'});
$backend->finish_job($job2, 0, {result => 'ok'});
$backend->fail_job($job3, 0, 'fail');

# Give Redis a moment to process (not strictly necessary, but avoids race on slow systems)
sleep 1;

# Retrieve history
my $history = $backend->history;
ok($history && ref $history->{daily} eq 'ARRAY', 'History returned daily array');

# Find the current hour bucket
my $now = time;
my $current_hour = int($now / 3600) * 3600;
my ($bucket) = grep { $_->{epoch} == $current_hour } @{ $history->{daily} };

ok($bucket, 'Found current hour bucket in history');
is($bucket->{finished_jobs}, 2, 'Correct number of finished jobs in current hour');
is($bucket->{failed_jobs}, 1, 'Correct number of failed jobs in current hour');

cleanup_redis();
done_testing();
