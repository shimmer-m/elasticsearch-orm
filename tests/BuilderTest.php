<?php

declare(strict_types=1);

namespace Meet\ElasticsearchOrm\Test;

use Elasticsearch\Client;
use Meet\ElasticsearchOrm\Factory;
use Meet\ElasticsearchOrm\Builder;
use PHPUnit\Framework\TestCase;
use Elasticsearch\Common\EmptyLogger;

class BuilderTest extends TestCase
{
    /**
     * @var Builder
     */
    private Builder $builder;

    private string $index = 'test';

    private Client $client;

    protected function setUp(): void
    {

        $config = require __DIR__ . '/../config/elasticsearch.php';
        $config['hosts'] = [
            'elastic:9200'
        ];
        // 初始化 Builder
        $this->builder = Factory::builder($config, new EmptyLogger());
        $this->client = $this->builder->getElasticSearch();
    }


    public function testCreateIndex()
    {

        $exists = $this->client->indices()->exists(['index' => $this->index]);
        $params = [
            'index' => $this->index,
            'body' => [
                'mappings' => [
                    'properties' => [
                        'name' => [
                            'type' => 'text'
                        ],
                        'age' => [
                            'type' => 'integer'
                        ]
                    ]
                ]
            ]
        ];
        if ($exists) {
            $res = $this->client->indices()->putMapping($params);
        } else {
            $res = $this->client->indices()->create($params);
        }
        var_dump($res);
        exit();
    }

    /**
     * 测试创建数据
     */
    public function testCreateData(): void
    {
        $result = $this->builder->index($this->index)->create([
            'name' => 'Test User',
            'age' => 30
        ]);
        dd($result);
    }

    /**
     * 测试修改数据
     */
    public function testUpdateData(): void
    {
        $documentId = '54adac35-5ccb-4df5-a658-f8267b87f50c';

        $data = [
            'name' => 'Updated User',
            'age' => 35
        ];

        $response = $this->builder->index($this->index)->update($documentId, $data);
        var_dump($response);
        exit();
    }

    /**
     * 测试批量创建数据
     */
    public function testBulkCreateData(): void
    {
        $data = [

            [
                'name' => 'User One',
                'age' => 25
            ],
            [
                'name' => 'User Two',
                'age' => 28
            ]
        ];
        $result = $this->builder->index($this->index)->batchCreate($data);
        dd($result);
    }

    /**
     * 测试删除数据
     */
    public function testDeleteData(): void
    {
        $documentId = '9ef8231e-e262-49fe-8d32-6e19b5e10e71'; // 假设已存在文档ID

        $result = $this->builder->index($this->index)->where('_id', $documentId)->delete();
        dd($result);

    }

    /**
     * 测试查询数据
     */
    public function testQueryData(): void
    {
        $query = $this->builder->index($this->index)->whereMatch('name', 'User One');
        dd($query->get());
    }

    /**
     * 测试查询条件：whereTerm
     */
    public function testWhereTerm(): void
    {
        $query = $this->builder->index($this->index)->whereTerm('age', 25);
        dd($query->get());
    }

    /**
     * 测试分页查询
     */
    public function testPagination(): void
    {
        $query = $this->builder->index($this->index);
        dd($query->paginate(1, 10));
    }
}
