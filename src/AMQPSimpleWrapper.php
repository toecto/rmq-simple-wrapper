<?php

namespace toecto\AMQPSimpleWrapper;

use \PhpAmqpLib\Connection\AMQPLazyConnection;
use \PhpAmqpLib\Message\AMQPMessage;

class AMQPSimpleWrapper {

    protected $channel = null;
    protected $connection = null;
    protected $credentials;

    public function __construct($user, $password, $vhost, $host = 'localhost', $port = 5672) {
        $this->credentials = array(
            'user' => $user,
            'password' => $password,
            'vhost' => $vhost,
            'host' => $host,
            'port' => $port
        );
    }

    public function __destruct() {
        if ($this->channel) {
            $this->channel->close();
        }
        if ($this->connection) {
            $this->connection->close();
        }
    }

    private function getConnection() {
        if (!$this->connection) {
            $this->connection = new AMQPLazyConnection(
                $this->credentials['host'],
                $this->credentials['port'],
                $this->credentials['user'],
                $this->credentials['password'],
                $this->credentials['vhost']
            );    
        }
        return $this->connection;
    }

    public function getChannel() {
        if (!$this->channel) {
            $connection = $this->getConnection();
            $this->channel = $connection->channel();    
        }
        return $this->channel;
    }

    public function publish($exchange, $key, $message, $args = null) {
        $msg_body = json_encode($message, \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE);
        $msg_args = array('delivery_mode' => 2);
        if ($args) {
            $msg_args = array_merge($msg_args, $args);
        }
        $msg = new AMQPMessage($msg_body, $msg_args);
        $this->getChannel()->basic_publish($msg, $exchange, $key);
    }

    public function declareExchange($name, $durable = true, $auto_delete = false) {
        $this->getChannel()->exchange_declare(
            $name,
            'topic',
            false, // passive
            $durable,
            $auto_delete
        );
    }

    public function bindExchange($destination, $source, $routing_key = '') {
        $this->getChannel()->exchange_bind($destination, $source, $routing_key);
    }

    public function unbindExchange($destination, $source, $routing_key = '') {
        $this->getChannel()->exchange_unbind($destination, $source, $routing_key);
    }

    public function deleteExchange($name) {
        $this->getChannel()->exchange_delete($name);
    }

    public function declareQueue($name, $durable = true, $auto_delete = false) {
        $this->getChannel()->queue_declare(
            $name,
            false, //passive
            $durable,
            false, //exclusive
            $auto_delete
        );
    }

    public function bindQueue($name, $exchange, $routing_key = '') {
        $this->getChannel()->queue_bind($name, $exchange, $routing_key);
    }

    public function unbindQueue($name, $exchange, $routing_key = '') {
        $this->getChannel()->queue_unbind($name, $exchange, $routing_key);

    }

    public function deleteQueue($name) {
        $this->getChannel()->queue_delete($name);
    }

    public function QOS($prefetch_count, $prefetch_size = 0) {
        $this->getChannel()->basic_qos(
            intval($prefetch_size * $prefetch_count * 1.2),
            $prefetch_count,
            false //global
        );
    }

    public function consume($queue, $callback, $limit = 1, $prefetch = 10, $consumer_tag = null, $timeout = 0) {
        $limit = intval($limit);
        $this->QOS($prefetch);
        $channel = $this->getChannel();
        $consumer_tag = $this->getChannel()->basic_consume(
            $queue,
            $consumer_tag,
            false, //no_local
            false, //no_ack
            false, //exclusive
            false, //nowait
            function ($msg) use ($callback) {
                $ack = call_user_func($callback, json_decode($msg->body, true), $msg->delivery_info['routing_key'], $msg);
                if ($ack === false) {
                    $msg->delivery_info['channel']->basic_nack($msg->delivery_info['delivery_tag']);
                } else {
                    $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                }
            }
        );

        do {
            $channel->wait(null, false, $timeout);
            $limit--;
        } while ($limit != 0);
        $channel->basic_cancel($consumer_tag);
    }

}

