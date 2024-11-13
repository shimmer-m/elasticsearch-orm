<?php

namespace Meet\ElasticsearchOrm\Tool\contracts;

interface Jsonable
{
    public function toJson(int $options = JSON_UNESCAPED_UNICODE): string;
}
