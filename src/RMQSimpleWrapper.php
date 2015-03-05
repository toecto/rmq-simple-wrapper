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

    private function getChannel() {
        if (!$this->channel) {
            $connection = $this->getConnection();
            $this->channel = $connection->channel();    
        }
        return $this->channel;
    }

    public function publish($exchange, $key, $message, $args = null) {
        $msg_body = json_encode($message);
        $msg_args = array('delivery_mode' => 2);
        if ($args) {
            $msg_args = array_merge($msg_args, $args);
        }
        $msg = new AMQPMessage($msg_body, $msg_args);
        $this->getChannel()->basic_publish($msg, $exchange, $key);
    }

    public function declareExchange($name, $durable = true, $auto_delete = false) {
        $this->getChannel()->exchange_declare($name, 'topic', false, $durable, $auto_delete);
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
        $this->getChannel()->queue_declare($name, false, $durable, false, $auto_delete);
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

    public function consume($queue, $callback) {
        $channel = $this->getChannel();
    }
}

