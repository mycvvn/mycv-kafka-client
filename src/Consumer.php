<?php

namespace MyCVKafka;

class Consumer {
    private static $instance;
    protected $topics = ['test'];

    public static function getInstance()
    {
        if (self::$instance) {
            return self::$instance;
        }
        self::$instance = new Consumer();
        return self::$instance;
    }

    public function __construct()
    {
        $this->conf = new \RdKafka\Conf();
        $this->conf->set('metadata.broker.list', env('KAFKA_BROKER_LIST'));
    }

    public function setGroup($group)
    {
        $this->conf->set('group.id', $group);
        return $this;
    }

    public function setTopics($topics)
    {
        $this->topics = $topics;
        return $this;
    }

    public function setRebalanceCb()
    {
        $this->conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;
        
                 case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                     echo "Revoke: ";
                     var_dump($partitions);
                     $kafka->assign(NULL);
                     break;
        
                 default:
                    throw new \Exception($err);
            }
        });
        return $this;
    }

    public function listeningMessage(callable $callback)
    {
        $consumer = new \RdKafka\KafkaConsumer($this->conf);
        $consumer->subscribe($this->topics);
        
        while (true) {
            $message = $consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->handleListeningCallback($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    private function handleListeningCallback(callable $callback, $message)
    {
        $payload = new \stdClass();
        $payloadJson = \json_decode($message->payload);
        $payload->payload = $payloadJson ? $payloadJson : $message->payload;
        
        $callback($message->topic_name, $message->partition, $payload);
    }
}
