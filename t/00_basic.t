use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::Fast;
use Data::Dumper;


my $redis_server;
eval {
    $redis_server = Test::RedisServer->new(
        conf => {
            port => 7474,
        }
    );
} or plan skip_all => 'redis-server is required for this test';

my $redis = Redis::Fast->new($redis_server->connect_info);
ok($redis->ping, 'Connected to Redis');
done_testing();
