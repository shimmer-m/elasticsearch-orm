<?php

namespace Meet\ElasticsearchOrm;

use BadMethodCallException;
use Closure;
use Elasticsearch\Client;
use Meet\ElasticsearchOrm\Tool\Arr;
use Meet\ElasticsearchOrm\Tool\Collection;
use Meet\ElasticsearchOrm\Tool\Json;
use Ramsey\Uuid\Uuid;
use RuntimeException;

/**
 * @method Builder index(string|array $index)
 * @method Builder type(string $type)
 * @method Builder limit(int $value)
 * @method Builder take(int $value)
 * @method Builder offset(int $value)
 * @method Builder skip(int $value)
 * @method Builder orderBy(string $field, $sort)
 * @method Builder aggBy(string | array $field, $type = null)
 * @method Builder scroll(string $scroll)
 * @method Builder select(string |array $columns)
 * @method Builder whereMatch($field, $value, $boolean = 'and')
 * @method Builder orWhereMatch($field, $value, $boolean = 'or')
 * @method Builder whereTerm($field, $value, $boolean = 'and')
 * @method Builder whereIn($field, array $value)
 * @method Builder orWhereIn($field, array $value)
 * @method Builder orWhereTerm($field, $value, $boolean = 'or')
 * @method Builder whereRange($field, $operator = null, $value = null, $boolean = 'and')
 * @method Builder orWhereRange($field, $operator = null, $value = null)
 * @method Builder whereBetween($field, array $values, $boolean = 'and')
 * @method Builder whereNotBetween($field, array $values)
 * @method Builder orWhereBetween($field, array $values)
 * @method Builder orWhereNotBetween(string $field, array $values)
 * @method Builder whereExists($field, $boolean = 'and')
 * @method Builder whereNotExists($field, $boolean = 'and')
 * @method Builder where($column, $operator = null, $value = null, string $leaf = 'term', string $boolean = 'and')
 * @method Builder whereLike($field, $value = null)
 * @method Builder orWhere($field, $operator = null, $value = null, $leaf = 'term')
 * @method Builder whereNested(string $nestedColumn, Closure $callback, $operator = null, string $boolean = 'and')
 * @method Builder newQuery()
 * @method Client getElasticSearch() 获取ElasticSearch实例
 */
class Builder
{
    /**
     * @var Query
     */
    protected Query $query;

    /**
     * @var array
     */
    protected array $queryLogs = [];

    /**
     * @var bool
     */
    protected bool $enableQueryLog = false;

    /**
     * @param Query $query
     */
    public function __construct(Query $query)
    {
        $this->query = $query;
    }

    /**
     * 重置查询Query
     *
     * @return void
     */
    public function resetQuery()
    {
        $this->query = $this->query->newQuery();
    }

    /**
     * 获取一条数据
     *
     * @return object|null
     */
    public function first()
    {
        $this->query->limit(1);

        return $this->get()->first();
    }

    /**
     * 执行查询，并处理返回数据
     *
     * @return Collection
     */
    public function get()
    {
        return $this->metaData($this->getOriginal());
    }

    /**
     * 执行原始查询，不处理返回数据
     *
     * @return array
     */
    public function getOriginal()
    {
        return $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'search');
    }

    /**
     * 分页查询
     *
     * @param int $page
     * @param int $perPage
     *
     * @return Collection
     */
    public function paginate(int $page, int $perPage = 15)
    {
        $from = (($page * $perPage) - $perPage);

        if (empty($this->query->offset)) {
            $this->query->offset($from);
        }

        if (empty($this->query->limit)) {
            $this->query->limit($perPage);
        }

        $results = $this->runQuery($this->query->getGrammar()->compileSelect($this->query));

        $data = $this->metaData($results);

        $maxPage = intval(ceil($results['hits']['total']['value'] / $perPage));

        return Collection::make([
            'total' => $results['hits']['total']['value'],
            'per_page' => $perPage,
            'current_page' => $page,
            'next_page' => $page < $maxPage ? $page + 1 : $maxPage,
            //'last_page' => $maxPage,
            'total_pages' => $maxPage,
            'from' => $from,
            'to' => $from + $perPage,
            'data' => $data,
        ]);
    }

    /**
     * 通过主键获取数据
     *
     * @param string|int $id
     *
     * @return null|object
     */
    public function byId($id)
    {
        $result = $this->runQuery(
            $this->query->whereTerm('_id', $id)->getGrammar()->compileSelect($this->query)
        );

        return isset($result['hits']['hits'][0]) ?
            $this->sourceToObject($result['hits']['hits'][0]) :
            null;
    }

    /**
     * 获取数据或抛出异常
     *
     * @param string|int $id
     *
     * @return object
     */
    public function byIdOrFail($id)
    {
        $result = $this->byId($id);

        if (empty($result)) {
            throw new RuntimeException('Resource not found by id:' . $id);
        }

        return $result;
    }

    /**
     * 组块读取数据
     *
     * @param callable $callback
     * @param int $limit
     * @param string $scroll
     *
     * @return bool
     */
    public function chunk(callable $callback, int $limit = 2000, string $scroll = '10m')
    {
        if (empty($this->query->scroll)) {
            $this->query->scroll($scroll);
        } else {
            $scroll = $this->query->scroll;
        }

        if (empty($this->query->limit)) {
            $this->query->limit($limit);
        } else {
            $limit = $this->query->limit;
        }

        $condition = $this->query->getGrammar()->compileSelect($this->query);
        $results = $this->runQuery($condition, 'search');

        if ($results['hits']['total']['value'] === 0) {
            return false;
        }

        // First total eq limit
        $total = $limit;

        $whileNum = intval(floor($results['hits']['total']['value'] / $total));

        do {
            if (call_user_func($callback, $this->metaData($results)) === false) {
                return false;
            }

            $results = $this->runQuery(['scroll_id' => $results['_scroll_id'], 'scroll' => $scroll], 'scroll');

            $total += count($results['hits']['hits']);
        } while ($whileNum--);
        return true;
    }

    /**
     * 创建数据
     *
     * @param array $data
     * @param string|int|null $id
     * @param string $key
     *
     * @return object
     * @throws \Exception
     */
    public function create(array $data, $id = null, $key = 'id')
    {
        $id = $id ?? ($data[$key] ?? Uuid::uuid4()->toString());

        $result = $this->runQuery(
            $this->query->getGrammar()->compileCreate($this->query, $id, $data),
            'create'
        );

        if (!isset($result['result']) || $result['result'] !== 'created') {
            throw new RunTimeException('Create error, params: ' . Json::encode($this->getLastQueryLog()));
        }

        $data['_id'] = $id;
        $data['_result'] = $result;

        return (object)$data;
    }

    /**
     * 批量创建数据
     *
     * @param array $data
     * @param string $key primary_key
     * @return mixed
     */
    public function batchCreate(array $data, string $key = 'id')
    {
        foreach ($data as &$item) {
            $item['id'] = $item[$key] ?? Uuid::uuid4()->toString();
        }
        return $this->runQuery(
            $this->query->getGrammar()->compileBulkCreate($this->query, $data),
            'bulk'
        );
    }

    /**
     * 批量创建或更新数据
     *
     * @param array $data
     * @param string $key
     * @return mixed
     */
    public function batchUpdateOrCreate(array $data, string $key = 'id')
    {
        foreach ($data as &$item) {
            $item['id'] = $item[$key] ?? Uuid::uuid4()->toString();
        }
        return $this->runQuery(
            $this->query->getGrammar()->compileBulkUpdateOrCreate($this->query, $data),
            'bulk'
        );
    }

    /**
     * 创建数据集合
     *
     * @param array $data
     * @param string|int|null $id
     * @param string $key
     *
     * @return Collection
     * @throws \Exception
     */
    public function createCollection(array $data, $id = null, $key = 'id')
    {
        return Collection::make($this->create($data, $id, $key));
    }

    /**
     * 更新数据
     *
     * @param string|int $id
     * @param array $data
     *
     * @return bool
     */
    public function update($id, array $data)
    {
        $result = $this->runQuery($this->query->getGrammar()->compileUpdate($this->query, $id, $data), 'update');
        if (!isset($result['result']) || ($result['result'] !== 'updated' && $result['result'] !== 'noop')) {
            throw new RunTimeException('Update error params: ' . Json::encode($this->getLastQueryLog()));
        }
        if ($result['result'] === 'updated') {
            return true;
        }
        return false;
    }

    /**
     *
     * 删除数据
     *
     * @param string|int $id
     *
     * @return bool
     */
    public function deleteById($id)
    {
        $result = $this->runQuery($this->query->getGrammar()->compileDelete($this->query, $id), 'delete');

        if (!isset($result['result']) || ($result['result'] !== 'deleted' && $result['result'] !== 'not_found')) {
            throw new RunTimeException('Delete error params:' . Json::encode($this->getLastQueryLog()));
        }
        if ($result['result'] === 'deleted') {
            return true;
        }
        return false;
    }

    /**
     * 批量删除数据
     *
     * @return int|mixed
     */
    public function delete()
    {
        $result = $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'DeleteByQuery');
        return $result['deleted'] ?? 0;
    }

    /**
     * 数量
     *
     * @return int
     */
    public function count(): int
    {
        $result = $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'count');

        return $result['count'];
    }

    /**
     * 运行查询
     *
     * @param array $params
     * @param string $method
     *
     * @return mixed
     */
    public function runQuery(array $params, string $method = 'search')
    {
        if ($this->enableQueryLog) {
            $this->queryLogs[] = $params;
        }

        return tap(call_user_func([$this->query->getElasticSearch(), $method], $params), function () {
            $this->resetQuery();
        });
    }

    /**
     * 启用查询日志
     *
     * @return Builder
     */
    public function enableQueryLog()
    {
        $this->enableQueryLog = true;

        return $this;
    }

    /**
     * 关闭查询日志
     *
     * @return Builder
     */
    public function disableQueryLog()
    {
        $this->enableQueryLog = false;

        return $this;
    }

    /**
     * 获取查询日志
     *
     * @return array
     */
    public function getQueryLog()
    {
        return $this->queryLogs;
    }

    /**
     * 获取最近一次的查询日志
     *
     * @return mixed
     */
    public function getLastQueryLog()
    {
        return Arr::last($this->queryLogs);
    }

    /**
     * 获取元数据
     *
     * @param array $results
     *
     * @return Collection
     */
    protected function metaData(array $results)
    {
        return Collection::make($results['hits']['hits'])->map(function ($hit) {
            return $this->sourceToObject($hit);
        });
    }

    /**
     * 将结果转换为对象
     *
     * @param array $result
     *
     * @return object
     */
    protected function sourceToObject(array $result)
    {
        return (object)array_merge($result['_source'], ['_id' => $result['_id'], '_score' => $result['_score']]);
    }

    /**
     * 转换为body
     *
     * @return array
     */
    public function toBody()
    {
        return $this->query->getGrammar()->compileSelect($this->query);
    }

    /**
     * 动态调用
     *
     * @param string $name
     * @param array $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (method_exists($this->query, $name)) {
            $query = call_user_func_array([$this->query, $name], $arguments);
            // If the query instance is returned, it is managed
            if ($query instanceof $this->query) {
                return $this;
            }

            return $query;
        }

        throw new BadMethodCallException(sprintf('The method[%s] not found', $name));
    }

    /**
     * scroll查询
     * @param string $scroll
     * @param string $scrollId
     * @return mixed
     */
    public function scrollPaginate(string $scroll, string $scrollId = '')
    {
        $params = ['scroll' => $scroll];
        if (!empty($scrollId)) {
            $params['scroll_id'] = $scrollId;
            return $this->runQuery($params, 'scroll');
        }
        $params = array_merge($params, $this->query->getGrammar()->compileSelect($this->query));
        return $this->runQuery($params);
    }

    /**
     * function_score查询
     * 使用场景：搜索结果打分，排序、过滤，用于给用户推荐相关数据等等
     * @param null $functionParams
     * @param int|null $size
     * @return Collection
     */
    public function functionScoreGet($functionParams = null, int $size = 20)
    {
        $params = $this->compileSelect();
        $functionSource = [
            // 得分模式: multiply、replace、sum、avg、max、min
            'boost_mode' => $functionParams['boost_mode'] ?? 'replace',
            // 定义如何合并多个函数的得分: multiply、sum、avg、first、max、min
            'score_mode' => $functionParams['score_mode'] ?? 'multiply',
        ];
        // 组装条件
        if (!empty($params['body']['query'])) {
            $functionSource['query'] = $params['body']['query'];
            unset($params['body']['query']);
        }

        // 评分规则函数
        if (!empty($functionParams['functions'])) {
            $functionSource['functions'] = $functionParams['functions'];
        } elseif (!empty($functionParams['random_score'])) {
            // 随机分数模式: seed 整数| field指定字段随机
            $functionSource['random_score'] = (object)$functionParams['random_score'];
        }
        $params['body']['query']['function_score'] = $functionSource;
        $params['size'] = $size;
        return $this->metaData($this->runQuery($params));
    }

    /**
     * 生成查询语句
     * @return array
     */
    public function compileSelect(): array
    {
        return $this->query->getGrammar()->compileSelect($this->query);
    }

}
