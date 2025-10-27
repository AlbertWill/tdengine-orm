<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm;

use Yurun\TDEngine\Orm\Annotation\Tag;
use Yurun\TDEngine\Orm\Contract\IQueryResult;
use Yurun\TDEngine\Orm\Enum\DataType;
use Yurun\TDEngine\Orm\Meta\Meta;
use Yurun\TDEngine\Orm\Meta\MetaManager;

/**
 * Model 基类.
 */
abstract class BaseModel implements \JsonSerializable
{
    /**
     * Meta.
     *
     * @var \Yurun\TDEngine\Orm\Meta\Meta
     */
    private $__meta;

    /**
     * 表名.
     *
     * @var string|null
     */
    protected $__table;

    /**
     * 构造方法
     * @param array $data
     * @param string|null $table
     */
    public function __construct(array $data = [], ?string $table = null)
    {
        $this->__meta = $meta = static::__getMeta();
        $this->__table = $table;
        //初始化字段
        if($data){
            foreach ($meta->getProperties() as $propertyName => $property)
            {
                if (isset($data[$propertyName]))
                {
                    $this->$propertyName = $data[$propertyName];
                }
                elseif (isset($data[$property->name]))
                {
                    $this->$propertyName = $data[$property->name];
                }
            }
        }

    }

    /**
     * 创建超级表
     * @param bool $ifNotExists
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function createSuperTable(bool $ifNotExists = true): IQueryResult
    {
        $meta = self::__getMeta();
        $tableAnnotation = $meta->getTable();
        $sql = 'CREATE TABLE ';
        if ($ifNotExists)
        {
            $sql .= 'IF NOT EXISTS ';
        }
        $fields = [];
        foreach ($meta->getFields() as $propertyName => $annotation)
        {
            $fields[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '') . ($annotation->primary_key ? ' PRIMARY KEY' : '');
        }
        $sql .= self::getFullTableName() . ' (' . implode(',', $fields) . ')';

        $fields = [];
        foreach ($meta->getTags() as $propertyName => $annotation)
        {
            $fields[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '');
        }
        $sql .= ' TAGS (' . implode(',', $fields) . ')';

        return TDEngineOrm::getClientHandler()->query($sql, $tableAnnotation->client ?? null);
    }

    /**
     * 创建超级表的子表
     * @param string $tableName 表名
     * @param array $tags 标签数组
     * @param bool $ifNotExists
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function createSubTable(string $tableName, array $tags = [], bool $ifNotExists = true): IQueryResult
    {
        $meta = self::__getMeta();
        $tableAnnotation = $meta->getTable();
        if (!$tableAnnotation->super)
        {
            return self::createSuperTable($ifNotExists);
        }
        $sql = 'CREATE TABLE ';
        if ($ifNotExists)
        {
            $sql .= 'IF NOT EXISTS ';
        }
        $sql .=  '`' . $tableAnnotation->database . '`.`' . $tableName . '` USING ' . self::getFullTableName() . ' ';
        if ($tags)
        {
            if (array_is_list($tags))
            {
                $i = 0;
                $values = [];
                foreach ($meta->getTags() as $annotation)
                {
                    $values[] = self::parseValue($annotation->type, $tags[$i] ?? null);
                    ++$i;
                }
                if ($values)
                {
                    $sql .= 'TAGS (' . implode(',', $values) . ') ';
                }
            }
            else
            {
                $tagAnnotations = $meta->getTags();
                $propertiesByFieldName = $meta->getPropertiesByFieldName();
                $propertyNames = [];
                $values = [];
                foreach ($tags as $key => $value)
                {
                    if (isset($tagAnnotations[$key]))
                    {
                        $tagAnnotation = $tagAnnotations[$key];
                    }
                    elseif (isset($propertiesByFieldName[$key]) && $propertiesByFieldName[$key] instanceof Tag)
                    {
                        $tagAnnotation = $propertiesByFieldName[$key];
                    }
                    else
                    {
                        continue;
                    }
                    $propertyNames[] = '`' . ($tagAnnotation->name ?? $key) . '`';
                    $values[] = self::parseValue($tagAnnotation->type, $value);
                }
                if ($values)
                {
                    $sql .= '(' . implode(',', $propertyNames) . ') TAGS (' . implode(',', $values) . ') ';
                }
            }
        }

        return TDEngineOrm::getClientHandler()->query($sql, $tableAnnotation->client ?? null);
    }

    /**
     * 创建普通表
     * @param bool $ifNotExists
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function createTables(bool $ifNotExists = true): IQueryResult
    {
        $meta = self::__getMeta();
        $tableAnnotation = $meta->getTable();
        $sql = 'CREATE TABLE ';
        if ($ifNotExists)
        {
            $sql .= 'IF NOT EXISTS ';
        }
        $fields = [];
        foreach ($meta->getFields() as $propertyName => $annotation)
        {
            $fields[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '') . ($annotation->primary_key ? ' PRIMARY KEY' : '');
        }
        $sql .= self::getFullTableName() . ' (' . implode(',', $fields) . ')';

        $tags = [];
        foreach ($meta->getTags() as $propertyName => $annotation)
        {
            $tags[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '');
        }
        if ($tags)
        {
            $sql .= ' TAGS ('.implode(',', $tags).')';
        }

        return TDEngineOrm::getClientHandler()->query($sql, $tableAnnotation->client ?? null);
    }

    /**
     * 新增一个Tag列
     * @param string $tag
     * @return array
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function addTag(string $tag): array
    {
        //获取所有的TAGS
        $tags = self::__getMeta()->getTags();
        //判断是否存在这个tag
        if(!array_key_exists($tag, $tags)){
            throw new \RuntimeException('tag not exists');
        }

        //执行对象
        $sql = 'ALTER STABLE ' . self::getFullTableName();

        //新加的tags
        $sql .=  ' ADD TAG `' . $tag . '` '. $tags[$tag]->type;

        if($tags[$tag]->length > 0){
            $sql .=  ' (' . $tags[$tag]->length . ')';
        }

        $result = TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);

        $rows = $result->getData();
        return $rows;
    }

    /**
     * 刷新Tag数据（新增一个Tag列时，刷新历史每个子表的这个Tag值用）
     * @param string $subTableName
     * @param string $tag
     * @param $value
     * @return array
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function flushTags(string $subTableName,string $tag, $value): array
    {
        //获取所有的TAGS
        $tags = self::__getMeta()->getTags();
        //判断是否存在这个tag
        if(!array_key_exists($tag, $tags)){
            throw new \RuntimeException("'tag:{$tag} not exists'");
        }

        //执行对象
        $sql = 'ALTER TABLE ' . self::getSubTableName($subTableName);

        //重置的tags
        $sql .=  ' SET TAG `' . $tag . '` = '. self::parseValue($tags[$tag]->type, $value);

        $result = TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);

        $rows = $result->getData();
        return $rows;
    }

    /**
     * 批量插入数据
     * @param array $models
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function batchInsert(array $models): IQueryResult
    {
        $sql = 'INSERT INTO ';
        $data_sql_arr = [];
        foreach ($models as $model)
        {
            $data_sql = '';
            $meta = $model::__getMeta();
            $tableAnnotation = $meta->getTable();
            $database = $tableAnnotation->database;
            $stable = $tableAnnotation->name;
            if (null === ($table = $model->__getTable())){
                throw new \RuntimeException('Table name cannot be null');
            }
            $data_sql .= '`' . $database . '`.`' . $table . '` ';
            if ($tableAnnotation->super)
            {
                $data_sql .= 'USING `' . $database . '`.`' . $stable . '` ';
                $tags = $tagValues = [];
                foreach ($meta->getTags() as $propertyName => $tagAnnotation)
                {
                    if(isset($model->$propertyName)){
                        $tags[] = '`' . ($tagAnnotation->name ?? $propertyName) . '`';
                        $tagValues[] = self::parseValue($tagAnnotation->type, $model->$propertyName);
                    }
                }
                if ($tags)
                {
                    $data_sql .= '(' . implode(',', $tags) . ') TAGS (' . implode(',', $tagValues) . ') ';
                }
            }
            $fields = $values = [];
            foreach ($meta->getFields() as $propertyName => $fieldAnnotation)
            {
                if(isset($model->$propertyName)){
                    $fields[] = '`' . ($fieldAnnotation->name ?? $propertyName) . '`';
                    $values[] = self::parseValue($fieldAnnotation->type, $model->$propertyName);
                }
            }
            if ($fields)
            {
                $data_sql .= '(' . implode(',', $fields) . ') VALUES (' . implode(',', $values) . ') ';
            }
            $data_sql_arr[] = $data_sql;
        }
        $sql .= implode(',', $data_sql_arr);

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 根据条件删除数据
     * @param array $condition 条件数组
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function deleteAll(array $condition): IQueryResult
    {
        if(empty($condition)){
            throw new \RuntimeException('condition cannot be null');
        }
        $where = self::buildWhere(self::__getMeta(), $condition);
        $sql = 'DELETE FROM ' . self::getFullTableName() . ' ' .  $where;

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 根据条件查询数据
     * @param array $condition 条件数组
     * @param string|array $colums
     * @param int $pageSize
     * @param int $page
     * @param string|array $orderBy
     * @param string|array $groupBy
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function queryList(array $condition, $colums, int $pageSize=0, int $page=1,  $orderBy='',  $groupBy=''): IQueryResult
    {
        $sql = self::buildQuery($condition, $colums, $pageSize, $page, $orderBy, $groupBy);

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 生成查询sql语句
     * @param array $condition
     * @param $colums
     * @param int $pageSize
     * @param int $page
     * @param string $orderBy
     * @param string $groupBy
     * @return string
     */
    public static function buildQuery(array $condition, $colums, int $pageSize=0, int $page=1,  $orderBy='',  $groupBy=''): string
    {
        if(empty($colums))
        {
            throw new \RuntimeException('colums cannot be null');
        }
        //查询字段
        $sql = 'SELECT ' . self::buildCommaSeparatedString($colums);
        //查询对象
        $sql .=  ' FROM ' . self::getFullTableName();
        //查询条件
        if($condition){
            $sql .= ' ' . self::buildWhere(self::__getMeta(), $condition);
        }
        //GROUP BY
        if($groupBy){
            $sql .= ' GROUP BY ' . self::buildCommaSeparatedString($groupBy);
        }
        //ORDER BY
        if($orderBy){
            $sql .= ' ORDER BY ' . self::buildCommaSeparatedString($orderBy);
        }
        //LIMIT
        if ($pageSize > 0 && $page > 0) {
            $offset = ($page - 1) * $pageSize;
            if($offset > 0){
                $sql .= ' LIMIT ' . $offset . ',' . $pageSize;
            }else{
                $sql .= ' LIMIT ' . $pageSize;
            }
        }
        return $sql;
    }

    /**
     * 获取完整的表名
     * @return string
     */
    public static function getFullTableName():string
    {
        $tableAnnotation = self::__getMeta()->getTable();
        return '`' . $tableAnnotation->database . '`.`' . $tableAnnotation->name . '`';
    }

    /**
     * 获取超级表子表的表名
     * @param $subTableName
     * @return string
     */
    public static function getSubTableName($subTableName):string
    {
        $tableAnnotation = self::__getMeta()->getTable();
        return '`' . $tableAnnotation->database . '`.`' . $subTableName . '`';
    }

    /**
     * 将输入（字符串或数组）转换为逗号分隔的字符串
     * @param string|array $input
     * @return string
     */
    public static function buildCommaSeparatedString($input): string
    {
        $str = '';
        if(is_string($input)){
            $str = $input;
        }else if (is_array($input)){
            $str = implode(',', $input);
        }
        return $str;
    }

    /**
     * 生成where条件
     * @param Meta $meta
     * @param array $condition
     * @return string
     */
    public static function buildWhere(Meta $meta, array $condition): string
    {
        $fieldTypeMap = self::getFieldTypeMap($meta);
        $where = [];

        foreach ($condition as $key => $item) {
            //  形式1： 'field' => 'value'
            if (is_string($key)) {
                if (!(is_string($item) || is_numeric($item))) {
                    throw new \RuntimeException("Invalid condition: field '$key' value type is invalid");
                }
                if (!isset($fieldTypeMap[$key])) {
                    throw new \RuntimeException("Invalid condition: unknown field '$key'");
                }
                $valueStr = self::parseWhereValue($fieldTypeMap[$key], $item);
                $where[] = "(`$key` = $valueStr)";
                continue;
            }

            // 形式2： ['OR', [...], [...]]
            if (is_array($item) && isset($item[0]) && strtoupper($item[0]) === 'OR') {
                $sql = self::buildOrCondition($item, $fieldTypeMap);
                if ($sql) {
                    $where[] = $sql;
                }
                continue;
            }

            // 形式3： ['op', 'field', 'value']
            if (is_array($item)) {
                if (count($item) !== 3) {
                    throw new \RuntimeException('Invalid condition: must be [operator, field, value] OR field=>value');
                }

                [$operator, $field, $value] = $item;

                if (!isset($fieldTypeMap[$field])) {
                    throw new \RuntimeException("Invalid condition: unknown field '$field'");
                }

                $valueStr = is_array($value)
                    ? '(' . implode(',', array_map(fn($v) => self::parseWhereValue($fieldTypeMap[$field], $v), $value)) . ')'
                    : self::parseWhereValue($fieldTypeMap[$field], $value);

                $where[] = "(`$field` $operator $valueStr)";
                continue;
            }

            // 不支持的结构，报错提示
            throw new \RuntimeException('Invalid condition format: ' . json_encode($item, JSON_UNESCAPED_UNICODE));
        }

        return $where ? 'WHERE ' . implode(' AND ', $where) : '';
    }

    /**
     * 构建 OR 条件组
     * 示例：
     * ['OR', 'item_id'=>'30713461', ['in', 'user_id', [1,2]], ['>', 'id', 552]]
     */
    public static function buildOrCondition(array $orCondition, array $fieldTypeMap): ?string
    {
        $orWhere = [];

        foreach ($orCondition as $key => $subItem) {
            //跳过 "OR" 字符串本身
            if ((is_string($subItem) && strtoupper($subItem) === 'OR') || (is_string($key) && strtoupper($key) === 'OR')) {
                continue;
            }

            // 形式： 'field' => value
            if (is_string($key)) {
                if (!isset($fieldTypeMap[$key])) {
                    throw new \RuntimeException("Invalid OR condition: unknown field '$key'");
                }
                $vStr = self::parseWhereValue($fieldTypeMap[$key], $subItem);
                $orWhere[] = "(`$key` = $vStr)";
                continue;
            }

            // 形式： ['op', 'field', 'value']
            if (is_array($subItem)) {
                if (count($subItem) !== 3) {
                    throw new \RuntimeException('Invalid OR condition: must be [operator, field, value] OR field=>value');
                }

                [$operator, $field, $value] = $subItem;
                if (!isset($fieldTypeMap[$field])) {
                    throw new \RuntimeException("Invalid OR condition: unknown field '$field'");
                }

                $valueStr = is_array($value)
                    ? '(' . implode(',', array_map(fn($v) => self::parseWhereValue($fieldTypeMap[$field], $v), $value)) . ')'
                    : self::parseWhereValue($fieldTypeMap[$field], $value);

                $orWhere[] = "(`$field` $operator $valueStr)";
                continue;
            }

            throw new \RuntimeException('Invalid OR condition format: ' . json_encode($subItem, JSON_UNESCAPED_UNICODE));
        }

        return $orWhere ? '(' . implode(' OR ', $orWhere) . ')' : null;
    }

    /**
     * 获取字段类型映射
     * @param Meta $meta
     * @return array
     */
    protected static function getFieldTypeMap(Meta $meta): array
    {
        //字段类型映射
        $fieldTypeMap = [];
        foreach ($meta->getFields() as $propertyName => $fieldAnnotation)
        {
            $field = $fieldAnnotation->name ?? $propertyName;
            $fieldTypeMap[$field] = $fieldAnnotation->type;
        }
        foreach ($meta->getTags() as $propertyName => $tagAnnotation)
        {
            $field = $tagAnnotation->name ?? $propertyName;
            $fieldTypeMap[$field] = $tagAnnotation->type;
        }
        return $fieldTypeMap;
    }

    /**
     * 值转义
     * @param string $type
     * @param $value
     * @return string
     */
    public static function parseValue(string $type, $value)
    {
        if (null === $value)
        {
            return 'NULL';
        }
        switch ($type)
        {
            case DataType::TIMESTAMP:
                if (!\is_string($value))
                {
                    break;
                }
            // no break
            case DataType::BINARY:
            case DataType::VARCHAR:
            case DataType::NCHAR:
                return '\'' . strtr((string)$value, [
                        "\0"     => '\0',
                        "\n"     => '\n',
                        "\r"     => '\r',
                        "\t"     => '\t',
                        \chr(26) => '\Z',
                        \chr(8)  => '\b',
                        '"'      => '\"',
                        '\''     => '\\\'',
                        '_'      => '\_',
                        '%'      => '\%',
                        '\\'     => '\\\\',
                    ]) . '\'';
            case DataType::BOOL:
                return $value ? 'true' : 'false';
        }

        return $value;
    }

    /**
     * 查询条件中的值转义
     * @param string $type
     * @param $value
     * @return string
     */
    public static function parseWhereValue(string $type, $value)
    {
        if (null === $value)
        {
            return 'NULL';
        }
        switch ($type)
        {
            case DataType::TIMESTAMP:
                if (!\is_string($value))
                {
                    break;
                }
            // no break
            case DataType::BINARY:
            case DataType::VARCHAR:
            case DataType::NCHAR:
                return '\'' . strtr((string)$value, [
                        "\0"     => '\0',
                        "\n"     => '\n',
                        "\r"     => '\r',
                        "\t"     => '\t',
                        \chr(26) => '\Z',
                        \chr(8)  => '\b',
                        '"'      => '\"',
                        '\''     => '\\\'',
                        '_'      => '\_',
                        '\\'     => '\\\\',
                    ]) . '\'';
            case DataType::BOOL:
                return $value ? 'true' : 'false';
        }

        return $value;
    }

    /**
     * 判断是否关联数组
     * @param array $arr
     * @return bool
     */
    public static function isAssocArray(array $arr): bool
    {
        return array_keys($arr) !== range(0, count($arr) - 1);
    }

    /**
     * 获取模型元数据
     * @return Meta
     */
    public static function __getMeta(): Meta
    {
        return MetaManager::get(static::class);
    }

    /**
     * @param string $name
     * @return null
     */
    public function &__get(string $name)
    {
        $methodName = 'get' . ucfirst($name);
        if (method_exists($this, $methodName))
        {
            $result = $this->$methodName();
        }
        else
        {
            $result = null;
        }

        return $result;
    }

    /**
     * @param string $name
     * @param $value
     * @return void
     */
    public function __set(string $name, $value):void
    {
        $methodName = 'set' . ucfirst($name);

        $this->$methodName($value);
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function __isset($name)
    {
        return null !== $this->__get($name);
    }

    /**
     * @param string $name
     *
     * @return void
     */
    public function __unset($name)
    {
    }

    /**
     * 将当前对象作为数组返回.
     */
    public function toArray(): array
    {
        $result = [];
        foreach ($this->__meta->getProperties() as $propertyName => $_)
        {
            $result[$propertyName] = $this->__get($propertyName);
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    #[\ReturnTypeWillChange]
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    public function __getTable(): ?string
    {
        return $this->__table;
    }

    public function __settable(?string $table): self
    {
        $this->__table = $table;

        return $this;
    }

}
