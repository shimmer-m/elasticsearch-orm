<?php

namespace Meet\ElasticsearchOrm\Tool;

use ArrayAccess;
use Countable;
use IteratorAggregate;
use ArrayIterator;
use Meet\ElasticsearchOrm\Tool\contracts\Arrayable;
use Meet\ElasticsearchOrm\Tool\contracts\Jsonable;

class Collection implements ArrayAccess, Countable, IteratorAggregate, Arrayable, Jsonable
{
    /**
     * @var array
     */
    protected $items = [];

    public function all()
    {
        return $this->items;
    }

    /**
     * Collection constructor.
     *
     * @param array $items
     */
    public function __construct(array $items = [])
    {
        $this->items = $items;
    }

    /**
     * 创建一个新的集合实例
     *
     * @param array $items
     * @return static
     */
    public static function make(array $items = [])
    {
        return new static($items);
    }

    /**
     * 使用回调映射集合中的每个元素
     *
     * @param callable $callback
     * @return static
     */
    public function map(callable $callback)
    {
        return new static(array_map($callback, $this->items));
    }

    // 实现 ArrayAccess 接口的方法
    public function offsetExists($offset): bool
    {
        return isset($this->items[$offset]);
    }

    public function offsetGet($offset)
    {
        return $this->items[$offset];
    }

    public function offsetSet($offset, $value): void
    {
        if ($offset === null) {
            $this->items[] = $value;
        } else {
            $this->items[$offset] = $value;
        }
    }

    public function offsetUnset($offset): void
    {
        unset($this->items[$offset]);
    }

    // 实现 Countable 接口的方法
    public function count(): int
    {
        return count($this->items);
    }

    // 实现 IteratorAggregate 接口的方法
    public function getIterator(): ArrayIterator
    {
        return new ArrayIterator($this->items);
    }

    public function sortBy($callback, $options = SORT_REGULAR, $descending = false)
    {
        $results = [];

        $callback = $this->valueRetriever($callback);

        // First we will loop through the items and get the comparator from a callback
        // function which we were given. Then, we will sort the returned values and
        // and grab the corresponding values for the sorted keys from this array.
        foreach ($this->items as $key => $value) {
            $results[$key] = $callback($value, $key);
        }

        $descending ? arsort($results, $options)
            : asort($results, $options);

        // Once we have sorted all of the keys in the array, we will loop through them
        // and grab the corresponding model so we can set the underlying items list
        // to the sorted version. Then we'll just return the collection instance.
        foreach (array_keys($results) as $key) {
            $results[$key] = $this->items[$key];
        }

        return new static($results);
    }

    protected function valueRetriever($value)
    {
        if ($this->useAsCallable($value)) {
            return $value;
        }

        return function ($item) use ($value) {
            return data_get($item, $value);
        };
    }

    /**
     * Determine if the given value is callable, but not a string.
     *
     * @param mixed $value
     * @return bool
     */
    protected function useAsCallable($value)
    {
        return !is_string($value) && is_callable($value);
    }

    /**
     * 是否为空
     * @access public
     * @return bool
     */
    public function isEmpty(): bool
    {
        return empty($this->items);
    }

    /**
     * 把一个数组分割为新的数组块.
     *
     * @access public
     * @param int $size 块大小
     * @param bool $preserveKeys
     * @return static
     */
    public function chunk(int $size, bool $preserveKeys = false)
    {
        $chunks = [];

        foreach (array_chunk($this->items, $size, $preserveKeys) as $chunk) {
            $chunks[] = new static($chunk);
        }

        return new static($chunks);
    }

    /**
     * 在数组开头插入一个元素
     * @access public
     * @param mixed $value 元素
     * @param string $key KEY
     * @return $this
     */
    public function unshift($value, string $key = null)
    {
        if (is_null($key)) {
            array_unshift($this->items, $value);
        } else {
            $this->items = [$key => $value] + $this->items;
        }

        return $this;
    }

    /**
     * 比较数组，返回差集
     *
     * @access public
     * @param mixed $items 数据
     * @param string $indexKey 指定比较的键名
     * @return static
     */
    public function diff($items, string $indexKey = null)
    {
        if ($this->isEmpty() || is_scalar($this->items[0])) {
            return new static(array_diff($this->items, $this->convertToArray($items)));
        }

        $diff = [];
        $dictionary = $this->dictionary($items, $indexKey);

        if (is_string($indexKey)) {
            foreach ($this->items as $item) {
                if (!isset($dictionary[$item[$indexKey]])) {
                    $diff[] = $item;
                }
            }
        }

        return new static($diff);
    }

    /**
     * 按指定键整理数据
     *
     * @access public
     * @param mixed $items 数据
     * @param string $indexKey 键名
     * @return array
     */
    public function dictionary($items = null, string &$indexKey = null)
    {
        if ($items instanceof self) {
            $items = $items->all();
        }

        $items = is_null($items) ? $this->items : $items;

        if ($items && empty($indexKey)) {
            $indexKey = is_array($items[0]) ? 'id' : $items[0]->getPk();
        }

        if (isset($indexKey) && is_string($indexKey)) {
            return array_column($items, null, $indexKey);
        }

        return $items;
    }


    /**
     * 比较数组，返回交集
     *
     * @access public
     * @param mixed $items 数据
     * @param string $indexKey 指定比较的键名
     * @return static
     */
    public function intersect($items, string $indexKey = null)
    {
        if ($this->isEmpty() || is_scalar($this->items[0])) {
            return new static(array_intersect($this->items, $this->convertToArray($items)));
        }

        $intersect = [];
        $dictionary = $this->dictionary($items, $indexKey);

        if (is_string($indexKey)) {
            foreach ($this->items as $item) {
                if (isset($dictionary[$item[$indexKey]])) {
                    $intersect[] = $item;
                }
            }
        }

        return new static($intersect);
    }

    /**
     * 交换数组中的键和值
     *
     * @access public
     * @return static
     */
    public function flip()
    {
        return new static(array_flip($this->items));
    }

    /**
     * 返回数组中所有的键名
     *
     * @access public
     * @return static
     */
    public function keys()
    {
        return new static(array_keys($this->items));
    }


    /**
     * 返回数组中所有的值组成的新 Collection 实例
     * @access public
     * @return static
     */
    public function values()
    {
        return new static(array_values($this->items));
    }

    /**
     * 删除数组的最后一个元素（出栈）
     *
     * @access public
     * @return mixed
     */
    public function pop()
    {
        return array_pop($this->items);
    }

    /**
     * 通过使用用户自定义函数，以字符串返回数组
     *
     * @access public
     * @param callable $callback 调用方法
     * @param mixed $initial
     * @return mixed
     */
    public function reduce(callable $callback, $initial = null)
    {
        return array_reduce($this->items, $callback, $initial);
    }

    /**
     * 以相反的顺序返回数组。
     *
     * @access public
     * @return static
     */
    public function reverse()
    {
        return new static(array_reverse($this->items));
    }

    /**
     * 删除数组中首个元素，并返回被删除元素的值
     *
     * @access public
     * @return mixed
     */
    public function shift()
    {
        return array_shift($this->items);
    }

    /**
     * 在数组结尾插入一个元素
     * @access public
     * @param mixed $value 元素
     * @param string $key KEY
     * @return $this
     */
    public function push($value, string $key = null)
    {
        if (is_null($key)) {
            $this->items[] = $value;
        } else {
            $this->items[$key] = $value;
        }

        return $this;
    }

    /**
     * 给每个元素执行个回调
     *
     * @access public
     * @param callable $callback 回调
     * @return $this
     */
    public function each(callable $callback)
    {
        foreach ($this->items as $key => $item) {
            $result = $callback($item, $key);

            if (false === $result) {
                break;
            } elseif (!is_object($item)) {
                $this->items[$key] = $result;
            }
        }

        return $this;
    }


    /**
     * 用回调函数过滤数组中的元素
     * @access public
     * @param callable|null $callback 回调
     * @return static
     */
    public function filter(callable $callback = null)
    {
        if ($callback) {
            return new static(array_filter($this->items, $callback));
        }

        return new static(array_filter($this->items));
    }

    /**
     * 对数组排序
     *
     * @access public
     * @param callable|null $callback 回调
     * @return static
     */
    public function sort(callable $callback = null)
    {
        $items = $this->toArray();

        $callback = $callback ?: function ($a, $b) {
            return $a == $b ? 0 : (($a < $b) ? -1 : 1);
        };

        uasort($items, $callback);

        return new static($items);
    }


    /**
     * 指定字段排序
     * @access public
     * @param string $field 排序字段
     * @param string $order 排序
     * @return $this
     */
    public function order(string $field, string $order = 'asc')
    {
        return $this->sort(function ($a, $b) use ($field, $order) {
            $fieldA = $a[$field] ?? null;
            $fieldB = $b[$field] ?? null;

            return 'desc' == strtolower($order) ? intval($fieldB > $fieldA) : intval($fieldA > $fieldB);
        });
    }

    /**
     * 将数组打乱
     *
     * @access public
     * @return static
     */
    public function shuffle()
    {
        $items = $this->items;

        shuffle($items);

        return new static($items);
    }

    /**
     * 获取第一个单元数据
     *
     * @access public
     * @param callable|null $callback
     * @param null $default
     * @return mixed
     */
    public function first(callable $callback = null, $default = null)
    {
        return Arr::first($this->items, $callback, $default);
    }

    /**
     * 获取最后一个单元数据
     *
     * @access public
     * @param callable|null $callback
     * @param null $default
     * @return mixed
     */
    public function last(callable $callback = null, $default = null)
    {
        return Arr::last($this->items, $callback, $default);
    }

    /**
     * 截取数组
     *
     * @access public
     * @param int $offset 起始位置
     * @param int $length 截取长度
     * @param bool $preserveKeys preserveKeys
     * @return static
     */
    public function slice(int $offset, int $length = null, bool $preserveKeys = false)
    {
        return new static(array_slice($this->items, $offset, $length, $preserveKeys));
    }

    /**
     * 转换当前数据集为JSON字符串
     * @access public
     * @param integer $options json参数
     * @return string
     */
    public function toJson(int $options = JSON_UNESCAPED_UNICODE): string
    {
        return json_encode($this->toArray(), $options);
    }

    public function toArray(): array
    {
        return array_map(function ($value) {
            // 优化
            if ($value instanceof Arrayable) {
                // 如果是 Arrayable 接口实例，调用其 toArray 方法
                return $value->toArray();
            } elseif ($value instanceof \stdClass) {
                // 如果是对象，且不是标准类对象，将对象转为数组 转换标准对象为数组
                return (array)$value;
            }
            return $value; // 其他对象不处理
        }, $this->items);
    }

    /**
     * 合并数组
     *
     * @access public
     * @param mixed $items 数据
     * @return static
     */
    public function merge($items)
    {
        return new static(array_merge($this->items, $this->convertToArray($items)));
    }

    /**
     * 返回数据中指定的一列
     * @access public
     * @param string|null $columnKey 键名
     * @param string|null $indexKey 作为索引值的列
     * @return array
     */
    public function column(?string $columnKey, string $indexKey = null)
    {
        return array_column($this->items, $columnKey, $indexKey);
    }

    /**
     * LIKE过滤
     * @access public
     * @param string $field 字段名
     * @param string $value 数据
     * @return static
     */
    public function whereLike(string $field, string $value)
    {
        return $this->where($field, 'like', $value);
    }

    /**
     * NOT LIKE过滤
     * @access public
     * @param string $field 字段名
     * @param string $value 数据
     * @return static
     */
    public function whereNotLike(string $field, string $value)
    {
        return $this->where($field, 'not like', $value);
    }

    /**
     * IN过滤
     * @access public
     * @param string $field 字段名
     * @param array $value 数据
     * @return static
     */
    public function whereIn(string $field, array $value)
    {
        return $this->where($field, 'in', $value);
    }

    /**
     * NOT IN过滤
     * @access public
     * @param string $field 字段名
     * @param array $value 数据
     * @return static
     */
    public function whereNotIn(string $field, array $value)
    {
        return $this->where($field, 'not in', $value);
    }

    /**
     * BETWEEN 过滤
     * @access public
     * @param string $field 字段名
     * @param mixed $value 数据
     * @return static
     */
    public function whereBetween(string $field, $value)
    {
        return $this->where($field, 'between', $value);
    }

    /**
     * NOT BETWEEN 过滤
     * @access public
     * @param string $field 字段名
     * @param mixed $value 数据
     * @return static
     */
    public function whereNotBetween(string $field, $value)
    {
        return $this->where($field, 'not between', $value);
    }

    /**
     * 根据字段条件过滤数组中的元素
     * @access public
     * @param string $field 字段名
     * @param mixed $operator 操作符
     * @param mixed $value 数据
     * @return static
     */
    public function where(string $field, $operator, $value = null)
    {
        if (is_null($value)) {
            $value = $operator;
            $operator = '=';
        }

        return $this->filter(function ($data) use ($field, $operator, $value) {
            if ($data instanceof \stdClass) {
                $data = (array)$data;
            }
            if (strpos($field, '.')) {
                [$field, $relation] = explode('.', $field);

                $result = $data[$field][$relation] ?? null;
            } else {
                $result = $data[$field] ?? null;
            }

            switch (strtolower($operator)) {
                case '===':
                    return $result === $value;
                case '!==':
                    return $result !== $value;
                case '!=':
                case '<>':
                    return $result != $value;
                case '>':
                    return $result > $value;
                case '>=':
                    return $result >= $value;
                case '<':
                    return $result < $value;
                case '<=':
                    return $result <= $value;
                case 'like':
                    return is_string($result) && false !== strpos($result, $value);
                case 'not like':
                    return is_string($result) && false === strpos($result, $value);
                case 'in':
                    return is_scalar($result) && in_array($result, $value, true);
                case 'not in':
                    return is_scalar($result) && !in_array($result, $value, true);
                case 'between':
                    [$min, $max] = is_string($value) ? explode(',', $value) : $value;
                    return is_scalar($result) && $result >= $min && $result <= $max;
                case 'not between':
                    [$min, $max] = is_string($value) ? explode(',', $value) : $value;
                    return is_scalar($result) && $result > $max || $result < $min;
                case '==':
                case '=':
                default:
                    return $result == $value;
            }
        });
    }

    /**
     * 转换成数组
     *
     * @access public
     * @param mixed $items 数据
     * @return array
     */
    protected function convertToArray($items): array
    {
        if ($items instanceof self) {
            return $items->all();
        }

        return (array)$items;
    }

    public function __toString()
    {
        return $this->toJson();
    }

}
