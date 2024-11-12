<?php

namespace Meet\ElasticsearchOrm\Test;

use PHPUnit\Framework\TestCase;
use Meet\ElasticsearchOrm\Builder;
use Meet\ElasticsearchOrm\Factory;

class Test extends TestCase
{
    /** @var Builder */
    public static $builder;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass(); // TODO: Change the autogenerated stub
        $config = require __DIR__ . '/../config/elasticsearch.php';
        $config['hosts'] = [
            ['host' => '127.0.0.1:9200']
        ];
        static::$builder = Factory::builder($config);
    }

    /**
     * @doesNotPerformAssertions
     * @return object
     * @throws \Exception
     */
    public function testCreate()
    {
        $result = static::$builder->index('test')->create(['key' => 'value']);
        var_dump($result);
        return $result;
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testBatchCreate()
    {
        $result = static::$builder->index('test')->batchCreate([['key' => 'value2'], ['key' => 'value3']]);
        var_dump($result);
        return $result;
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testScriptOrder()
    {
        $result = static::$builder->index('qy_list_gather_data')->orderBy('_script', [
            'script' => 'Math.random()',
            'type' => 'number',
            'order' => 'asc'
        ])->paginate(1, 2);
        var_dump($result);
        return $result;
    }
}
