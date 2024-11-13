<?php

namespace Meet\ElasticsearchOrm\Tool;

class Json
{

    public const FORCE_ARRAY = JSON_OBJECT_AS_ARRAY;
    public const PRETTY = JSON_PRETTY_PRINT;
    public const ESCAPE_UNICODE = 1 << 19;

    public static function encode($value, $flags = 0)
    {
        $flags = ($flags & self::ESCAPE_UNICODE ? 0 : JSON_UNESCAPED_UNICODE)
            | JSON_UNESCAPED_SLASHES
            | ($flags & ~self::ESCAPE_UNICODE)
            | (defined('JSON_PRESERVE_ZERO_FRACTION') ? JSON_PRESERVE_ZERO_FRACTION : 0); // since PHP 5.6.6 & PECL JSON-C 1.3.7

        return json_encode($value, $flags);
    }

    /**
     * Parses JSON to PHP value. The flag can be Json::FORCE_ARRAY, which forces an array instead of an object as the return value.
     * @return mixed
     * @throws JsonException
     */
    public static function decode(string $json, int $flags = 0)
    {
        $value = json_decode($json, null, 512, $flags | JSON_BIGINT_AS_STRING);

        return $value;
    }
}
