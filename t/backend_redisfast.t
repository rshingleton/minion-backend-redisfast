use strict;
use warnings;
use Test::More;
use Redis::Fast;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Minion::Backend::RedisFast;

my $redis_url = $ENV{REDIS_URL} || '127.0.0.1:6379';
my $prefix = "minion_test_" . int(rand(10000));

# Clean up before and after
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

# 1. Test enqueue and job_info
my $job_id = $backend->enqueue('test_task', [1,2,3], {queue => 'important'});
ok($job_id, 'Job enqueued');

my $job = $backend->job_info($job_id);
is($job->{task}, 'test_task', 'Correct task');
is_deeply($job->{args}, [1,2,3], 'Correct args');
is($job->{queue}, 'important', 'Correct queue');
is($job->{state}, 'inactive', 'Job is inactive');

# 2. Test dequeue
my $worker_id = 'worker1';
my $deq_job = $backend->dequeue($worker_id, 1, {queues => ['important']});
ok($deq_job, 'Job dequeued');
is($deq_job->{id}, $job_id, 'Dequeued job id matches');
is($deq_job->{state}, 'active', 'Job is active');
is($deq_job->{worker}, $worker_id, 'Job assigned to worker');

# 3. Test finish_job
ok($backend->finish_job($job_id, 0, {result => 'done'}), 'Job finished');
my $finished = $backend->job_info($job_id);
is($finished->{state}, 'finished', 'Job state is finished');
is_deeply($finished->{result}, {result => 'done'}, 'Job result stored');

# 4. Test fail_job and retry_job
my $jid2 = $backend->enqueue('fail_task', []);
ok($backend->fail_job($jid2, 0, 'fail_reason'), 'Job failed');
my $failed = $backend->job_info($jid2);
is($failed->{state}, 'failed', 'Job state is failed');
is($failed->{error}, 'fail_reason', 'Job error message set');

ok($backend->retry_job($jid2), 'Job retried');
my $retried = $backend->job_info($jid2);
is($retried->{state}, 'inactive', 'Job state reset to inactive');

# 5. Test lock and unlock
my $lock = $backend->lock('resource1', 2);
ok($lock && $lock->{resource}, 'Lock acquired');
ok($backend->unlock($lock->{resource}, $lock->{value}), 'Lock released');

# 6. Test register_worker and worker_info
my $wid = $backend->register_worker('workerX', {status => {foo => 1}});
ok($wid, 'Worker registered');
my $winfo = $backend->worker_info($wid);
is($winfo->{id}, $wid, 'Worker id matches');
is_deeply($winfo->{status}, {foo => 1}, 'Worker status matches');

# 7. Test stats
my $stats = $backend->stats;
ok($stats->{enqueued_jobs} >= 2, 'Stats returns enqueued jobs');

# 8. Test purge and reset
ok($backend->purge({older => 0}), 'Purge old jobs');
ok($backend->reset, 'Backend reset');
my $job_after_reset = $backend->job_info($job_id);
ok(!$job_after_reset, 'Job removed after reset');

cleanup_redis();

done_testing();
