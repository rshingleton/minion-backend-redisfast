use strict;
use warnings;
use Test2::V0;
use Test::RedisServer;
use Redis::Fast;
use FindBin;
use Time::HiRes 'time';
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;
use Test2::AsyncSubtest;
use Test2::IPC;

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

# Test 1: Basic exclusive lock acquisition
subtest 'Exclusive lock' => sub {
    my $lock = $backend->lock('resource1', 10);
    ok($lock, 'Acquired exclusive lock');
    is($lock->{validity}, 9.9, 'Lock validity is 99% of TTL');

    # Verify lock exists in Redis
    my $score = $redis->zscore($lock->{resource}, $lock->{value});
    ok(defined $score && $score > time, 'Lock exists with future expiration');

    # Test ownership verification
    ok(!$backend->unlock($lock->{resource}, 'wrong_value'), 'Wrong value fails to unlock');
    ok($backend->unlock($lock->{resource}, $lock->{value}), 'Correct value unlocks');

    # Verify lock removal
    is($redis->zscore($lock->{resource}, $lock->{value}), undef, 'Lock removed from Redis');
};

# Test 2: Shared lock concurrency
subtest 'Shared locks' => sub {
    my $lock1 = $backend->lock('shared', 10, {limit => 2});
    ok($lock1, 'Acquired first shared lock');

    my $lock2 = $backend->lock('shared', 10, {limit => 2});
    ok($lock2, 'Acquired second shared lock');

    my $lock3 = $backend->lock('shared', 10, {limit => 2});
    ok(!$lock3, 'Cannot exceed shared lock limit');

    # Release one lock
    ok($backend->unlock($lock1->{resource}, $lock1->{value}), 'Released first lock');

    # Should be able to acquire again
    my $lock4 = $backend->lock('shared', 10, {limit => 2});
    ok($lock4, 'Acquired new lock after release');

    # Cleanup
    $backend->unlock($lock2->{resource}, $lock2->{value});
    $backend->unlock($lock4->{resource}, $lock4->{value});
};

# Test 3: Lock expiration and cleanup
subtest 'Lock expiration' => sub {
    my $lock = $backend->lock('ephemeral', 1);
    ok($lock, 'Acquired temporary lock');

    # Immediate re-lock should fail
    ok(!$backend->lock('ephemeral', 1), 'Lock still active');

    # Wait for expiration
    sleep 2;

    # Verify automatic cleanup
    my $expired_lock = $backend->lock('ephemeral', 1);
    ok($expired_lock, 'Acquired lock after expiration');
    is($redis->zcard($expired_lock->{resource}), 1, 'Only one active lock');

    # Cleanup
    $backend->unlock($expired_lock->{resource}, $expired_lock->{value});
};

# Test 4: Concurrent locking using simple forking
subtest 'Concurrent locking' => sub {
    my $concurrency = 3;
    my $attempts = 5;
    my @pids;

    # Create pipe for synchronization
    pipe(my ($reader, $writer)) or die "Pipe failed: $!";

    for (1..$attempts) {
        my $pid = fork();
        die "Fork failed: $!" unless defined $pid;

        if ($pid == 0) { # Child
            close $writer;  # Close writer in child
            my $buf;
            sysread($reader, $buf, 1); # Wait for parent signal

            my $worker_backend = Minion::Backend::RedisFast->new(
                $redis_server->connect_info,
                prefix => $prefix
            );

            # Acquire lock or exit with failure
            my $lock = $worker_backend->lock(
                'concurrent',
                10,
                {limit => $concurrency}
            ) or exit 1;

            exit 0; # Success
        }
        else { # Parent
            push @pids, $pid;
        }
    }

    # Signal all children AFTER forking
    close $reader;
    print $writer 'G' for 1..$attempts;
    close $writer;

    # Wait for results
    my $success = 0;
    foreach my $pid (@pids) {
        waitpid($pid, 0);
        $success++ if $? == 0;
    }

    is($success, $concurrency, "Only $concurrency concurrent locks granted");
};

# Test 5: Stale lock cleanup
subtest 'Stale lock cleanup' => sub {
    # Manually add expired lock
    $redis->zadd($backend->_key('lock:stale'), time - 10, 'stale_value');

    # Attempt to acquire new lock
    my $lock = $backend->lock('stale', 10);
    ok($lock, 'Acquired new lock after stale cleanup');

    # Verify stale lock was removed
    is($redis->zscore($backend->_key('lock:stale'), 'stale_value'), undef, 'Stale lock cleaned up');

    # Cleanup
    $backend->unlock($lock->{resource}, $lock->{value});
};

cleanup_redis();
done_testing();
