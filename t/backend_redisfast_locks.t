use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_lock_test_" . int(rand(10000));
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

# Test exclusive lock (limit => 1)
my $lock1 = $backend->lock('resource1', 10, {limit => 1});
ok($lock1, 'Acquired exclusive lock');
is($lock1->{validity}, 9.9, 'Lock validity is 99% of TTL');

# Try to acquire same lock again (should fail)
my $lock1_dup = $backend->lock('resource1', 10, {limit => 1});
ok(!$lock1_dup, 'Cannot acquire exclusive lock twice');

# Release lock and re-acquire
ok($backend->unlock($lock1->{resource}, $lock1->{value}), 'Released exclusive lock');
my $lock1_new = $backend->lock('resource1', 10, {limit => 1});
ok($lock1_new, 'Re-acquired exclusive lock after release');

# Test shared lock (limit => 2)
my $shared_lock1 = $backend->lock('shared_resource', 10, {limit => 2});
ok($shared_lock1, 'Acquired first shared lock');

my $shared_lock2 = $backend->lock('shared_resource', 10, {limit => 2});
ok($shared_lock2, 'Acquired second shared lock');

# Third attempt should fail
my $shared_lock3 = $backend->lock('shared_resource', 10, {limit => 2});
ok(!$shared_lock3, 'Cannot exceed shared lock limit');

# Release one shared lock
ok($backend->unlock($shared_lock1->{resource}, $shared_lock1->{value}), 'Released first shared lock');

# Third attempt should now succeed
$shared_lock3 = $backend->lock('shared_resource', 10, {limit => 2});
ok($shared_lock3, 'Acquired third shared lock after release');

# Test lock expiration (simulate by setting low TTL)
my $expiring_lock = $backend->lock('ephemeral', 1, {limit => 1});
ok($expiring_lock, 'Acquired expiring lock');
sleep 2; # Wait for lock to expire
my $expired_lock_attempt = $backend->lock('ephemeral', 1, {limit => 1});
ok($expired_lock_attempt, 'Lock expired and can be re-acquired');

cleanup_redis();
done_testing();
