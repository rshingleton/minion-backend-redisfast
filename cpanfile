requires 'perl', '5.26.0';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::RedisServer', '0.04';
};

requires 'Mojo::Log';
requires 'Mojo::Base';
requires 'Redis::Fast';
requires 'Try::Tiny';
requires 'UUID::Tiny';